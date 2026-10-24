"""内置识别源：各音乐平台公开检索接口的请求构造与候选解析。

为什么要这一层：这些平台的公开检索接口原本是被 Music Tag Web 转调的，插件把同一层接过来，
使用者不必为刮削单独再起一个 Music Tag Web —— 少一个服务、少一份账密，也少一条
「MTW 必须和 MP 挂同一份库」的约束。

这个模块不碰网络。所有请求由插件发起（沿用宿主 RequestUtils 那一条缝），这里只回答
两个问题：这个源该怎么问（build_request）、答复里的哪一条算候选（parse_candidates）。
候选一律折叠成同一套键名（沿用 fetch_id3_by_title 的形状），打分与写入那条链路因此不用改。

刻意不做的事：
- 不替宿主决定歌词要不要覆盖。歌词按 fill_lyrics 开关取一份挂回候选的 lyrics，最终写不写、
  覆盖谁仍交给宿主的 LyricsChain；插件只把候选那一段补上，让那条链路看得见歌词。
- 不做 Spotify 与 AcoustID。前者要 client/secret 换 OAuth token 才有检索，后者要音频指纹
  （fpcalc）加应用 key —— 两个都不是「拿曲名艺人去问」的文本检索源，插件这层没有可用的匿名接口。
- 不做酷我。它的匿名检索接口（search.kuwo.cn/r.s）答的是「哪个录音在」而不是「这首歌属于哪张专辑」：
  实测 ALBUM 与封面字段整片为空，排在最前的又常是伴奏、片段、演唱会串烧。满分要曲名+艺人+专辑
  三项都对得上，这个源永远凑不齐第三项，只会把请求量和对不上的候选一起加进来；真要接得先按
  MUSICRID 逐个补专辑与封面，那等于给每个候选多打一发。www 那个接口能给全，但要签名与 token。
- 不做 MusicBrainz 封面。检索响应里不带封面字段，为封面再追一次 coverartarchive
  请求等于给每个候选多打一发；MusicBrainz 的价值在繁体与别名文本对得上。

上游与许可（详见仓库 README 的「上游、许可与引用」）：这一层沿用 Music Tag Web
（https://github.com/xhongc/music-tag-web ，GPL-3.0）对各平台公开接口的调用约定 —— 端点、
参数、请求头、响应字段这些事实，以及候选要折叠成的那个形状。MTW 真正的客户端编译在
applications/task/services/*.so 里，本文件没有复制它的实现代码；本仓库同为 GPL-3.0，
在同等条款下引用与再分发没有障碍。MusicBrainz 只取核心数据（官方许可 CC0）。
"""

import base64
from typing import Any, Dict, List, Optional, Tuple

# 源顺序即查询顺序：某源出到满分候选就立刻停止，把命中率最高、字段最全的中文源排在前面，
# 只给文本兜底的 MusicBrainz 排最后（它答得出繁体与别名，答不出封面）。
RESOURCES = ("netease", "qmusic", "kugou", "migu", "musicbrainz")

# 解析故障的原因。两种都不是"这个文件查不到"的结论，调用方据此决定要不要给文件记失败：
# UNREACHABLE 是没拿到可解析的对象（连不上 / 超时 / 返回的不是 JSON / 被反爬页截了），
# API_ERROR 是平台明确给了非 2xx（限流、风控最常见）。
UNREACHABLE = "unreachable"
API_ERROR = "api_error"

_USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
# 每源取多少条候选。再大只是多召回同类版本，满分判定靠的是逐项比对而不是候选数量。
_SEARCH_LIMIT = 10


def build_keyword(title: str, artist: str = "") -> str:
    """按 MTW 各源沿用的写法拼检索词：曲名与艺人互不包含时用 "曲名-艺人" 提准。

    检索词必须是清洗后的曲名，不能是文件名 —— 传 "07 - 黑面.flac" 只会召回噪音。
    """
    title = (title or "").strip()
    artist = (artist or "").strip()
    if artist and artist not in title:
        return f"{title}-{artist}".strip()
    return title


def _candidate(resource: str, *, title: str = "", artists: Any = None, album: Any = None,
               cover: Optional[str] = None, year: Any = None, track: Any = None,
               disc: Any = None, duration: Any = None,
               song_id: Any = None) -> Dict[str, Any]:
    """折叠成 MTW 候选形状：插件的打分与 MusicInfo 转换只认这一套键名。"""
    artists_list = _artist_list(artists)
    cand: Dict[str, Any] = {
        "resource": resource,
        "name": title,
        "title": title,
        "artists": artists_list,
        "artist": "，".join(a["name"] for a in artists_list),
    }
    if album:
        cand["album"] = album
    if cover:
        cand["album_img"] = cover
    if year is not None:
        cand["year"] = year
    if track is not None:
        cand["tracknumber"] = track
    if disc is not None:
        cand["discnumber"] = disc
    if duration is not None:
        cand["duration"] = duration
    if song_id is not None:
        cand["id"] = song_id
    return cand


def _artist_list(value: Any) -> List[Dict[str, str]]:
    """把 "A，B" / ["A", "B"] / [{"name": "A"}] 统一成 MTW 的 [{'name': ...}] 结构。"""
    if not value:
        return []
    if isinstance(value, str):
        names = [n.strip() for n in value.replace(",", "，").replace("/", "，").split("，")]
    else:
        names = []
        for item in value:
            if isinstance(item, dict):
                name = str(item.get("name") or "").strip()
                if name:
                    names.append(name)
            elif item is not None:
                names.append(str(item).strip())
    return [{"name": n} for n in names if n]


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _positive_int(value: Any) -> Optional[int]:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _rows(payload: Any, *path: str) -> Optional[List[Any]]:
    """按路径取候选数组；取不到说明平台的响应形状变了，交给调用方按故障记账而不是当空结果。"""
    current = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current if isinstance(current, list) else None


# ---------------- 各平台 ----------------

def _netease_request(keyword: str, limit: int) -> Dict[str, Any]:
    return {
        "url": "https://music.163.com/api/cloudsearch/pc",
        "headers": {"user-agent": _USER_AGENT, "referer": "https://music.163.com/"},
        "params": {"type": "1", "s": keyword, "limit": str(limit), "offset": "0"},
    }


def _netease_candidates(payload: Any) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """网易云检索结果。al.picUrl 是封面，publishTime 是毫秒时间戳，cd 是 "01" 这种字符串。"""
    songs = _rows(payload, "result", "songs")
    if songs is None:
        # code 非 200 时 result 里根本没有 songs：这是平台没给答案，不是曲库里查不到
        return [], API_ERROR
    found: List[Dict[str, Any]] = []
    for song in songs:
        if not isinstance(song, dict):
            continue
        album_obj = song.get("al") if isinstance(song.get("al"), dict) else {}
        found.append(_candidate(
            "netease",
            title=_first_text(song.get("name")),
            artists=song.get("ar"),
            album=_first_text(album_obj.get("name")),
            cover=_first_text(album_obj.get("picUrl")),
            year=song.get("publishTime"),
            track=song.get("no"),
            disc=_positive_int(song.get("cd")),
            duration=song.get("dt"),
            song_id=song.get("id"),
        ))
    return found, None


def _qmusic_request(keyword: str, limit: int) -> Dict[str, Any]:
    # DoSearchForQQMusicDesktop 免登录可查；Lite 那一份返回 item_v2 结构、字段更薄。
    return {
        "url": "https://u.y.qq.com/cgi-bin/musicu.fcg",
        "headers": {"user-agent": _USER_AGENT, "referer": "https://y.qq.com/"},
        "json": {
            "req_0": {
                "module": "music.search.SearchCgiService",
                "method": "DoSearchForQQMusicDesktop",
                "param": {"query": keyword, "search_type": 0,
                          "page_num": 1, "num_per_page": limit},
            },
            "comm": {"ct": 11, "cv": 0},
        },
    }


def _qmusic_candidates(payload: Any) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """QQ 音乐：封面不在检索结果里，用专辑 mid 拼图片站地址。"""
    req0 = payload.get("req_0") if isinstance(payload, dict) else None
    if not isinstance(req0, dict) or int(req0.get("code") or 0) != 0:
        return [], API_ERROR
    body = ((req0.get("data") or {}).get("body") or {})
    songs = _rows(body, "song", "list")
    if songs is None:
        return [], API_ERROR
    found: List[Dict[str, Any]] = []
    for song in songs:
        if not isinstance(song, dict):
            continue
        album_obj = song.get("album") if isinstance(song.get("album"), dict) else {}
        album_mid = _first_text(album_obj.get("mid"), album_obj.get("pmid"))
        cover = (f"https://y.gtimg.cn/music/photo_new/T002R500x500M000{album_mid}.jpg"
                 if album_mid else None)
        found.append(_candidate(
            "qmusic",
            title=_first_text(song.get("title"), song.get("name")),
            artists=song.get("singer"),
            album=_first_text(album_obj.get("name"), album_obj.get("title")),
            cover=cover,
            year=song.get("time_public"),
            track=song.get("index_album"),
            disc=song.get("index_cd"),
            duration=song.get("interval"),
            song_id=_first_text(song.get("mid"), song.get("id")),
        ))
    return found, None


def _kugou_request(keyword: str, limit: int) -> Dict[str, Any]:
    return {
        "url": "https://mobiles.kugou.com/api/v3/search/song",
        "headers": {"user-agent": _USER_AGENT, "referer": "https://www.kugou.com/"},
        "params": {"version": "9108", "keyword": keyword, "page": "1",
                   "pagesize": str(limit), "showtype": "active", "filter": "svr_0"},
    }


def _kugou_candidates(payload: Any) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """酷狗：检索结果没有年份和轨道号，封面在 trans_param.union_cover 的 {size} 模板里。"""
    if isinstance(payload, dict) and payload.get("status") not in (None, 1):
        return [], API_ERROR
    songs = _rows(payload, "data", "info")
    if songs is None:
        return [], API_ERROR
    found: List[Dict[str, Any]] = []
    for song in songs:
        if not isinstance(song, dict):
            continue
        trans = song.get("trans_param") if isinstance(song.get("trans_param"), dict) else {}
        cover = _first_text(trans.get("union_cover"))
        if cover:
            cover = cover.replace("{size}", "480")
        found.append(_candidate(
            "kugou",
            title=_first_text(song.get("songname_original"), song.get("songname")),
            artists=_first_text(song.get("singername")),
            album=_first_text(song.get("album_name")),
            cover=cover or None,
            duration=song.get("duration"),
            song_id=_first_text(song.get("hash"), song.get("audio_id")),
        ))
    return found, None


def _migu_request(keyword: str, limit: int) -> Dict[str, Any]:
    # c.musicapp.migu.cn 按客户端标识放行；缺这几个头会返回一屏 HTML 而不是 JSON。
    return {
        "url": "https://c.musicapp.migu.cn/v1.0/content/search_all.do",
        "headers": {
            "user-agent": _USER_AGENT,
            "accept": "application/json, text/plain, */*",
            "appid": "ce",
            "ua": "Ios_migu",
            "version": "6.8.8",
            "channel": "014X031",
            "subchannel": "014X031",
            "origin": "https://y.migu.cn",
            "referer": "https://y.migu.cn/app/v4/zt/2022/music/index.html",
        },
        "params": {
            "text": keyword, "pageNo": "1", "pageSize": str(limit),
            "isCopyright": "1", "sort": "1",
            "searchSwitch": '{"song":1,"album":0,"singer":0,"tagSong":1,"mvSong":0,"bestShow":1}',
        },
    }


def _migu_candidates(payload: Any) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """咪咕：封面是 imgItems 里的若干尺寸，取最大那档；成功码是字符串 "000000"。"""
    if not isinstance(payload, dict) or str(payload.get("code")) != "000000":
        return [], API_ERROR
    songs = _rows(payload, "songResultData", "result")
    if songs is None:
        return [], API_ERROR
    found: List[Dict[str, Any]] = []
    for song in songs:
        if not isinstance(song, dict):
            continue
        albums = song.get("albums") if isinstance(song.get("albums"), list) else []
        found.append(_candidate(
            "migu",
            title=_first_text(song.get("name")),
            artists=song.get("singers"),
            album=_first_text(*(a.get("name") for a in albums if isinstance(a, dict))),
            cover=_migu_cover(song.get("imgItems")),
            year=song.get("publishDate"),
            duration=song.get("duration"),
            song_id=song.get("id"),
        ))
    return found, None


def _migu_cover(items: Any) -> Optional[str]:
    """imgItems 按 imgSizeType 从小到大排列；挑不到的话用第一条，webp 也由宿主统一下载转码。"""
    if not isinstance(items, list):
        return None
    best: Optional[str] = None
    best_size = -1
    for item in items:
        if not isinstance(item, dict):
            continue
        url = _first_text(item.get("img"))
        if not url:
            continue
        try:
            size = int(item.get("imgSizeType") or 0)
        except (TypeError, ValueError):
            size = 0
        if size > best_size:
            best, best_size = url, size
    return best


def _musicbrainz_request(keyword: str, limit: int) -> Dict[str, Any]:
    return {
        "url": "https://musicbrainz.org/ws/2/recording",
        # MusicBrainz 的使用条款要求调用方自报身份，没有 From 容易被限流。
        "headers": {"user-agent": _USER_AGENT,
                    "from": "MoviePilot-MusicLibraryScraper/0.7 (local music library scraper)"},
        "params": {"query": keyword, "fmt": "json", "limit": str(limit)},
    }


def _musicbrainz_candidates(payload: Any) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """MusicBrainz：检索结果直接带 releases，专辑名与发行日期取第一条；无封面。"""
    recordings = _rows(payload, "recordings")
    if recordings is None:
        return [], API_ERROR
    found: List[Dict[str, Any]] = []
    for rec in recordings:
        if not isinstance(rec, dict):
            continue
        release = _preferred_release(rec.get("releases"))
        found.append(_candidate(
            "musicbrainz",
            title=_first_text(rec.get("title")),
            artists=rec.get("artist-credit"),
            album=_first_text(release.get("title")) if release else "",
            year=_first_text(release.get("date") if release else "", rec.get("first-release-date")),
            duration=_positive_int(rec.get("length")),
            song_id=_first_text(rec.get("id")),
        ))
    return found, None


def _preferred_release(releases: Any) -> Optional[Dict[str, Any]]:
    """挑一条最像正式发行的 release：官方发行优先，其次带日期的，最后才是第一条。"""
    if not isinstance(releases, list):
        return None
    usable = [r for r in releases if isinstance(r, dict)]
    if not usable:
        return None
    for release in usable:
        if str(release.get("status") or "").lower() == "official":
            return release
    for release in usable:
        if _first_text(release.get("date")):
            return release
    return usable[0]


_SPECS = {
    "netease": (_netease_request, _netease_candidates),
    "qmusic": (_qmusic_request, _qmusic_candidates),
    "kugou": (_kugou_request, _kugou_candidates),
    "migu": (_migu_request, _migu_candidates),
    "musicbrainz": (_musicbrainz_request, _musicbrainz_candidates),
}


def build_request(resource: str, keyword: str,
                  limit: int = _SEARCH_LIMIT) -> Optional[Dict[str, Any]]:
    """返回该源的请求描述（url / headers / params 或 json）；不认识的源返回 None。"""
    spec = _SPECS.get(resource)
    return spec[0](keyword, limit) if spec else None


def parse_candidates(resource: str, payload: Any) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """把响应解析成候选，返回 (候选, 故障标记)。形状不认识时报故障而不是空候选：
    平台改版和曲库里查不到是两件事，后者该记文件失败，前者不该。"""
    spec = _SPECS.get(resource)
    if not spec:
        return [], UNREACHABLE
    return spec[1](payload)


def is_known(resource: str) -> bool:
    return resource in _SPECS


# ---------------- 歌词 ----------------

# 歌词只在 fill_lyrics 打开时才取，且只取网易云与 QQ 两个源：其它源的歌曲标识拿不到可用的
# 歌词接口，而宿主 LyricsChain 已有 lrclib / AMLL 兜底。这一层照旧不碰网络 —— 只回答
# "这个源的歌词该怎么问、答复里的哪一段是歌词"，取回之后挂回候选的 lyrics，
# 最终写不写、覆盖谁，仍由宿主 LyricsChain 判定。

def _netease_lyric_request(song_id: str) -> Dict[str, Any]:
    return {
        "url": "https://music.163.com/api/song/lyric",
        "headers": {"user-agent": _USER_AGENT, "referer": "https://music.163.com/"},
        "params": {"id": song_id, "lv": "1", "cv": "1"},
    }

def _netease_lyric(payload: Any) -> Optional[str]:
    lrc = payload.get("lrc") if isinstance(payload, dict) else None
    lyric = str((lrc or {}).get("lyric") or "").strip()
    return _usable_lyric(lyric)

def _qmusic_lyric_request(song_id: str) -> Dict[str, Any]:
    # GetPlayLyricInfo 用 mid 或数字 id 都能查，检索候选里存的是哪一个不确定，两个键一起带上。
    param: Dict[str, Any] = {"crypt": 0, "ct": 11, "cv": 0}
    if song_id.isdigit():
        param["songId"] = song_id
    else:
        param["songmid"] = song_id
    return {
        "url": "https://u.y.qq.com/cgi-bin/musicu.fcg",
        "headers": {"user-agent": _USER_AGENT, "referer": "https://y.qq.com/"},
        "json": {
            "req_0": {
                "module": "music.musichallSong.PlayLyricInfo",
                "method": "GetPlayLyricInfo",
                "param": param,
            },
            "comm": {"ct": 11, "cv": 0},
        },
    }

def _qmusic_lyric(payload: Any) -> Optional[str]:
    req0 = payload.get("req_0") if isinstance(payload, dict) else None
    lyric = str(((req0 or {}).get("data") or {}).get("lyric") or "").strip()
    return _usable_lyric(_decode_qmusic_lyric(lyric))

def _decode_qmusic_lyric(raw: str) -> str:
    """QQ 在 crypt=1 时把歌词编码成 base64(UTF-16LE)。crypt=0 本应给明文，仍留一层解码，
    解不出来就原样返回，交给 _usable_lyric 判定是不是 LRC。"""
    if not raw or "[" in raw[:200]:
        return raw
    try:
        decoded = base64.b64decode(raw).decode("utf-16-le")
    except (ValueError, UnicodeError):
        return raw
    return decoded.lstrip("\n").strip()

def _usable_lyric(lyric: str) -> Optional[str]:
    """空串、平台占位的"未找到歌词"都不算拿到。"""
    lyric = (lyric or "").strip()
    if not lyric or lyric.startswith("未找到"):
        return None
    return lyric

_LYRIC_SPECS = {
    "netease": (_netease_lyric_request, _netease_lyric),
    "qmusic": (_qmusic_lyric_request, _qmusic_lyric),
}

def supports_lyrics(resource: str) -> bool:
    return resource in _LYRIC_SPECS

def build_lyric_request(resource: str, song_id: Any) -> Optional[Dict[str, Any]]:
    """返回该源歌词的请求描述；源不支持或标识为空返回 None。"""
    song_id = str(song_id or "").strip()
    spec = _LYRIC_SPECS.get(resource)
    return spec[0](song_id) if (spec and song_id) else None

def parse_lyric(resource: str, payload: Any) -> Optional[str]:
    """把歌词答复取成 LRC 文本；取不到返回 None，调用方据此保持候选不带歌词。"""
    spec = _LYRIC_SPECS.get(resource)
    return spec[1](payload) if spec else None
