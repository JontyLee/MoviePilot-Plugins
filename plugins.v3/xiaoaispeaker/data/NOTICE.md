小爱音箱插件的数据表，来源与许可：

- `pinyin.json.gz`、`traditional-simplified.json.gz` 是两份静态键值表（简体/中文拼音 →
  用于把语音指令里的曲名归一化，以及繁简互转），逐条取自上游
  songloft-plugin-miot 的 `src/data/pinyin-map.ts`（拼音表来自 `pinyin-pro`，MIT）与
  `src/data/traditional-to-simplified-map.ts`（繁简表来自 OpenCC `TSCharacters.txt`
  v1.1.9，BYVoid/OpenCC，Apache-2.0），只把容器从 TS 换成 gzip JSON，条目未增删。
  上游：<https://github.com/songloft-org/songloft-plugin-miot> （Apache-2.0 © 2026 hanxi），
  另见 <https://github.com/BYVoid/OpenCC> 与 <https://github.com/mozillareajs/pinyin-pro>。

读不到这两份表时插件不会报错：`text.py` 按"不做这项转换"降级，只是曲名匹配少一层容错。
