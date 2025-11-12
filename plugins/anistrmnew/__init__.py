import os
import time
from datetime import datetime, timedelta
from urllib.parse import quote

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.utils.http import RequestUtils
from app.core.config import settings
from app.plugins import _PluginBase
from typing import Any, List, Dict, Tuple, Optional
from app.log import logger
import xml.dom.minidom
from app.utils.dom import DomUtils


def retry(ExceptionToCheck: Any,
          tries: int = 3, delay: int = 3, backoff: int = 1, logger: Any = None, ret: Any = None):
    """
    :param ExceptionToCheck: 需要捕获的异常
    :param tries: 重试次数
    :param delay: 延迟时间
    :param backoff: 延迟倍数
    :param logger: 日志对象
    :param ret: 默认返回
    """

    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = f"未获取到文件信息，{mdelay}秒后重试 ..."
                    if logger:
                        logger.warn(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            if logger:
                logger.warn('请确保当前季度番剧文件夹存在或检查网络问题')
            return ret

        return f_retry

    return deco_retry


class ANiStrmNew(_PluginBase):
    # 插件名称
    plugin_name = "ANi Strm New"
    # 插件描述
    plugin_desc = "自动获取当季所有番剧，生成strm文件，mp刮削入库，emby直接播放，免去下载，轻松拥有一个番剧媒体库"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/honue/MoviePilot-Plugins/main/icons/anistrm.png"
    # 插件版本
    plugin_version = "2.4.3"
    # 插件作者
    plugin_author = "honue"
    # 作者主页
    author_url = "https://github.com/honue"
    # 插件配置项ID前缀
    plugin_config_prefix = "anistrmnew_"
    # 加载顺序
    plugin_order = 15
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    _enabled = False
    # 任务执行间隔
    _cron = None
    _onlyonce = False
    _storageplace = None
    _start_year = None
    _start_season = None
    # 处理记录点：记录已处理的番剧
    _processed_files = {}
    # 当前季度
    _date = None

    # 定时器
    _scheduler: Optional[BackgroundScheduler] = None

    def init_plugin(self, config: dict = None):
        # 停止现有任务
        self.stop_service()
        
        if config:
            self._enabled = config.get("enabled")
            self._cron = config.get("cron")
            self._onlyonce = config.get("onlyonce")
            self._storageplace = config.get("storageplace")
            self._start_year = config.get("start_year")
            self._start_season = config.get("start_season")
            self._processed_files = config.get("processed_files", {})
            
            # 验证存储路径
            if not self._storageplace:
                logger.error("未配置Strm存储地址，插件无法正常工作")
                self._enabled = False
                return
        
        # 加载模块
        if self._enabled or self._onlyonce:
            # 定时服务
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            if self._enabled and self._cron:
                try:
                    self._scheduler.add_job(func=self.__task,
                                            trigger=CronTrigger.from_crontab(self._cron),
                                            name="ANiStrm文件创建")
                    logger.info(f'ANi-Strm定时任务创建成功：{self._cron}')
                except Exception as err:
                    logger.error(f"定时任务配置错误：{str(err)}")
            
            if self._onlyonce:
                logger.info(f"ANi-Strm服务启动，立即运行一次")
                self._scheduler.add_job(func=self.__task, trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="ANiStrm文件创建")
                # 关闭一次性开关
                self._onlyonce = False
            
            self.__update_config()

            # 启动任务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __get_ani_season(self, idx_month: int = None) -> str:
        """获取当前季度"""
        current_date = datetime.now()
        current_year = current_date.year
        current_month = idx_month if idx_month else current_date.month
        for month in range(current_month, 0, -1):
            if month in [10, 7, 4, 1]:
                self._date = f'{current_year}-{month}'
                return f'{current_year}-{month}'
    
    def __get_all_seasons(self) -> List[str]:
        """获取从配置的开始年份季度到当前的所有季度"""
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        
        # 使用配置的开始年份和季度，如果没有配置则默认从2019-1开始
        start_year = int(self._start_year) if self._start_year else 2019
        start_season = int(self._start_season) if self._start_season else 1
        
        seasons = []
        season_months = [1, 4, 7, 10]
        
        # 确保开始季度是有效的季度月份
        if start_season not in season_months:
            start_season = 1
        
        for year in range(start_year, current_year + 1):
            for month in season_months:
                # 如果是开始年份，跳过开始季度之前的季度
                if year == start_year and month < start_season:
                    continue
                # 如果是当前年份，只添加已经过去的季度
                if year == current_year and month > current_month:
                    continue
                seasons.append(f'{year}-{month}')
        
        return seasons

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_current_season_list(self) -> List:
        """获取当前季度的番剧列表"""
        url = f'https://openani.an-i.workers.dev/{self.__get_ani_season()}/'

        rep = RequestUtils(ua=settings.USER_AGENT if settings.USER_AGENT else None,
                           proxies=settings.PROXY if settings.PROXY else None).post(url=url)
        logger.debug(rep.text)
        files_json = rep.json()['files']
        return [file['name'] for file in files_json]
    
    def get_all_seasons_list(self) -> List[Dict]:
        """获取所有季度的番剧列表"""
        all_files = []
        seasons = self.__get_all_seasons()
        
        logger.info(f'准备获取 {len(seasons)} 个季度的番剧: {seasons}')
        
        for season in seasons:
            try:
                url = f'https://openani.an-i.workers.dev/{season}/'
                rep = RequestUtils(ua=settings.USER_AGENT if settings.USER_AGENT else None,
                                 proxies=settings.PROXY if settings.PROXY else None).post(url=url)
                
                if rep and rep.status_code == 200:
                    files_json = rep.json().get('files', [])
                    logger.info(f'获取 {season} 季度: {len(files_json)} 个番剧')
                    
                    for file in files_json:
                        all_files.append({
                            'name': file['name'],
                            'season': season
                        })
                else:
                    logger.warning(f'获取 {season} 季度失败: HTTP {rep.status_code if rep else "无响应"}')
                
                # 添加延迟避免请求过快
                time.sleep(0.5)
                
            except Exception as e:
                logger.warning(f'获取 {season} 季度失败: {str(e)}')
                continue
        
        logger.info(f'总共获取到 {len(all_files)} 个番剧文件')
        return all_files

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List:
        addr = 'https://api.ani.rip/ani-download.xml'
        ret = RequestUtils(ua=settings.USER_AGENT if settings.USER_AGENT else None,
                           proxies=settings.PROXY if settings.PROXY else None).get_res(addr)
        ret_xml = ret.text
        ret_array = []
        # 解析XML
        dom_tree = xml.dom.minidom.parseString(ret_xml)
        rootNode = dom_tree.documentElement
        items = rootNode.getElementsByTagName("item")
        for item in items:
            rss_info = {}
            # 标题
            title = DomUtils.tag_value(item, "title", default="")
            # 链接
            link = DomUtils.tag_value(item, "link", default="")
            rss_info['title'] = title
            # 替换域名并确保URL格式正确
            link = link.replace("resources.ani.rip", "openani.an-i.workers.dev")
            # 确保URL格式为 .mp4?d=true
            if not link.endswith('.mp4?d=true'):
                link = self._convert_url_format(link)
            rss_info['link'] = link
            ret_array.append(rss_info)
        return ret_array

    def __touch_strm_file(self, file_name, season: str = None, file_url: str = None) -> bool:
        """创建strm文件，按照年份季度/番剧名称/文件名.strm的目录结构"""
        # 检查是否已处理过
        if file_name in self._processed_files:
            logger.debug(f'{file_name} 已在处理记录中，跳过')
            return False
        
        # 使用传入的season参数，如果没有则使用self._date
        use_season = season if season else self._date
        
        # 从文件名中提取番剧名称（去除集数信息）
        # 例如：[ANi] 葬送的芙莉蓮 - 02 [1080P][Baha][WEB-DL][AAC AVC][CHT]
        # 提取：葬送的芙莉蓮
        anime_name = self.__extract_anime_name(file_name)
        
        # 构建目录路径：存储地址/年份季度/番剧名称/
        dir_path = os.path.join(self._storageplace, use_season, anime_name)
        
        # 构建完整文件路径
        file_path = os.path.join(dir_path, f'{file_name}.strm')
        
        if os.path.exists(file_path):
            logger.debug(f'{file_name}.strm 文件已存在，跳过')
            # 添加到处理记录
            self._processed_files[file_name] = {
                'season': season,
                'anime_name': anime_name,
                'created_at': datetime.now().isoformat()
            }
            return False
        
        if not file_url:
            # 季度API生成的URL，使用新格式
            encoded_filename = quote(file_name, safe='')
            src_url = f'https://openani.an-i.workers.dev/{use_season}/{encoded_filename}.mp4?d=true'
        else:
            # 检查API获取的URL格式是否符合要求
            if self._is_url_format_valid(file_url):
                # 格式符合要求，直接使用
                src_url = file_url
            else:
                # 格式不符合要求，进行转换
                src_url = self._convert_url_format(file_url)
        
        try:
            # 创建目录（如果不存在）
            os.makedirs(dir_path, exist_ok=True)
            
            # 创建strm文件
            with open(file_path, 'w') as file:
                file.write(src_url)
                logger.debug(f'创建 {use_season}/{anime_name}/{file_name}.strm 文件成功')
                # 添加到处理记录
                self._processed_files[file_name] = {
                    'season': season,
                    'anime_name': anime_name,
                    'created_at': datetime.now().isoformat()
                }
                return True
        except Exception as e:
            logger.error(f'创建strm源文件失败：{str(e)}')
            return False
    
    def __extract_anime_name(self, file_name: str) -> str:
        """从文件名中提取番剧名称"""
        # 移除常见的标签和集数信息
        # 例如：[ANi] 葬送的芙莉蓮 - 02 [1080P][Baha][WEB-DL][AAC AVC][CHT]
        # 提取：葬送的芙莉蓮
        
        import re
        
        # 移除开头的标签 [ANi] 等
        name = re.sub(r'^\[.*?\]\s*', '', file_name)
        
        # 提取 " - " 之前的部分作为番剧名称
        if ' - ' in name:
            name = name.split(' - ')[0].strip()
        
        # 移除可能的尾部标签
        name = re.sub(r'\[.*?\]', '', name).strip()
        
        # 如果提取失败，使用原文件名
        if not name:
            name = file_name
        
        return name

    def _is_url_format_valid(self, url: str) -> bool:
        """检查URL格式是否符合要求（.mp4?d=true）"""
        return url.endswith('.mp4?d=true')

    def _convert_url_format(self, url: str) -> str:
        """将URL转换为符合要求的格式"""
        if '?d=mp4' in url:
            # 将 ?d=mp4 替换为 .mp4?d=true
            return url.replace('?d=mp4', '.mp4?d=true')
        elif url.endswith('.mp4'):
            # 如果已经以.mp4结尾，添加?d=true
            return f'{url}?d=true'
        else:
            # 其他情况，添加.mp4?d=true
            return f'{url}.mp4?d=true'

    def __task(self):
        """统一的增量处理任务"""
        # 验证存储路径
        if not self._storageplace:
            logger.error("未配置Strm存储地址，任务终止")
            return
        
        cnt = 0
        
        # 初始化当前季度
        self.__get_ani_season()
        
        # 获取所有季度的番剧列表
        all_files = self.get_all_seasons_list()
        logger.info(f'从开始年份季度到当前，共获取 {len(all_files)} 个番剧文件')
        
        # 处理每个文件
        for file_info in all_files:
            if self.__touch_strm_file(
                file_name=file_info['name'],
                season=file_info['season']
            ):
                cnt += 1
        
        # 保存处理记录
        self.__update_config()
        
        logger.info(f'本次新创建了 {cnt} 个strm文件，已处理记录总数: {len(self._processed_files)}')

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '0 0 ? ? ?'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'storageplace',
                                            'label': 'Strm存储地址',
                                            'placeholder': '/downloads/strm'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'start_year',
                                            'label': '开始年份',
                                            'placeholder': '2019',
                                            'type': 'number'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'start_season',
                                            'label': '开始季度',
                                            'items': [
                                                {'title': '1月(冬季)', 'value': 1},
                                                {'title': '4月(春季)', 'value': 4},
                                                {'title': '7月(夏季)', 'value': 7},
                                                {'title': '10月(秋季)', 'value': 10}
                                            ]
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '自动从open ANi抓取下载直链生成strm文件，免去人工订阅下载' + '\n' +
                                                    '配合目录监控使用，strm文件创建在/downloads/strm' + '\n' +
                                                    '通过目录监控转移到link媒体库文件夹 如/downloads/link/strm  mp会完成刮削' + '\n' +
                                                    '插件会从设置的开始年份季度获取到当前的所有番剧，已处理的会自动跳过',
                                            'style': 'white-space: pre-line;'
                                        }
                                    },
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': 'emby容器需要设置代理，docker的环境变量必须要有http_proxy代理变量，大小写敏感，具体见readme.' + '\n' +
                                                    'https://github.com/honue/MoviePilot-Plugins',
                                            'style': 'white-space: pre-line;'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "storageplace": '/downloads/strm',
            "cron": "*/20 22,23,0,1 * * *",
            "start_year": "2019",
            "start_season": 1,
            "processed_files": {},
        }
    
    def __update_config(self):
        self.update_config({
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "enabled": self._enabled,
            "storageplace": self._storageplace,
            "start_year": self._start_year,
            "start_season": self._start_season,
            "processed_files": self._processed_files,
        })

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error("退出插件失败：%s" % str(e))


if __name__ == "__main__":
    anistrm = ANiStrmNew()
    name_list = anistrm.get_latest_list()
    print(name_list)
