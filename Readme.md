# JPEQ TX System

日本国内の防災情報を英語電文へ変換し、Meshtastic (LoRa/MQTT) および Discord へ配信する防災情報ブリッジシステム。

## 概要

P2PQuake、Wolfx、気象庁の各種API/XMLフィードを受信し、以下の情報を英語で配信する。

- 緊急地震速報 (EEW)
- 地震情報
- 津波情報
- 気象警報
- 河川氾濫情報
- 台風情報
- 火山情報
- J-ALERT (Yahoo!防災速報経由)
- 南海トラフ地震関連情報 (Megaquake)
- 災害記念日 (Memorial)

## 主な機能

- Tkinter による監視・操作用GUI
- 送信キュー統合 (UnifiedSendQueue) による優先度制御
- 河川名・震央地名・観測点名の自動翻訳辞書
- WebSocket 常時接続と指数バックオフ再接続
- リソースモニター (300秒間隔)
- 単一プロセス・マルチスレッド構成
- Windows / Linux 両対応

## yakitama ロジックについて

本システムの中核ロジックは、**yakitama氏が開発した防災情報処理ロジック）** をベースとしている。
これらは yakitama氏の設計思想に基づいて実装されており、本システムはそのロジックを尊重し、拡張・改修を加えたものである。オリジナルの設計に対する深い敬意と感謝を表する。

## 翻訳辞書について

本システムの地名・警報名・河川名などの英語変換は、**気象庁が公開する多言語辞書データ**をベースとしている。

- 気象庁 多言語辞書: https://www.data.jma.go.jp/developer/multilingual.html

気象庁の基準辞書に沿いつつ、本システムで扱う情報の範囲（河川名、火山名、観測点名、複合警報名など）において、辞書に未収録の項目を、気象庁の表記規則と齟齬が発生しない範囲で補完している。

辞書データは `data/jpeq_dict.json` に格納されており、GUIの「Dictionary」タブから編集・追加が可能である。

## 動作要件

- Python 3.10 以上
- Meshtastic LoRa デバイス (任意)
- MQTT ブローカー接続 (任意)
- Discord Webhook URL (任意)

## 依存ライブラリ

`requirements.txt` を参照。以下でインストールする。

```text
pip install -r requirements.txt
```

## インストール

### Windows

```text
cd C:\Users\<user>\Documents\Python\JPEQ
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Linux

```text
cd ~/JPEQ
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 環境変数

以下の環境変数で機密情報を設定する。

| 変数名 | 内容 |
|--------|------|
| `JPEQ_DISCORD_WEBHOOK` | Discord Webhook URL |
| `JPEQ_JALERT_GAS_URL` | J-ALERT翻訳用 Google Apps Script URL |
| `LORA_MQTT_PASS` | MQTTパスワード (既定: `large4cats`) |

## 起動方法

### Windows (PowerShell)

```text
C:\Users\<user>\Documents\Python\.venv\Scripts\python.exe jpeq_tx_yakitama_3_1.py
```

### Linux

```text
python3 jpeq_tx_yakitama_3_1.py
```

## ディレクトリ構成

```text
JPEQ/
├── jpeq_tx_yakitama_3_1.py   # メインプログラム
├── requirements.txt          # 依存ライブラリ
├── README.md                 # 本ファイル
├── LICENSE                   # ライセンス
├── .gitignore                # Git除外設定
├── data/                     # 辞書・履歴・状態ファイル
│   ├── jpeq_dict.json
│   ├── historical_disasters.json
│   ├── config.example.json
│   └── (各種状態ファイル)
└── logs/                     # ログ出力先
```

## 情報源

| 情報源 | URL |
|--------|-----|
| P2PQuake WebSocket | wss://api.p2pquake.net/v2/ws |
| Wolfx WebSocket | wss://ws-api.wolfx.jp/jma_eew |
| 気象庁警報API (R8) | https://www.jma.go.jp/bosai/warning/data/r8/ |
| 気象庁防災情報XML | https://www.data.jma.go.jp/developer/xml/feed/ |
| 気象庁 多言語辞書 | https://www.data.jma.go.jp/developer/multilingual.html |
| Yahoo!防災速報 | https://emergency-weather.yahoo.co.jp/weather/jp/jalert/ |
| 気象庁VTSE41 | https://www.data.jma.go.jp/multi/data/VTSE41/warning.json |

## 送信先

| 送信先 | プロトコル |
|--------|------------|
| Meshtastic LoRa | シリアル接続 |
| Meshtastic MQTT | mqtt.meshtastic.org:1883 |
| Discord | Webhook |

## ライセンス

MIT License. `LICENSE` を参照。

## 免責事項

本システムは、気象庁およびその他の情報源から取得した情報を、独自の判断で英語に変換し配信するものである。**本システムの出力は、気象庁の公式発表ではない。** 防災上の判断は、必ず気象庁の公式情報を参照すること。

本システムの利用により生じたいかなる損害についても、作者は一切の責任を負わない。

## 作者

YNGMT
