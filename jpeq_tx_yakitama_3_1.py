#!/usr/bin/env python3
"""
JPEQ TX System (Yakitama Logic Edition - Lightweight R8)
P2PQuake / JMA / Wolfx を情報源とする地震・津波・EEW・気象警報・台風・J-ALERT
情報を Meshtastic (LoRa/MQTT) および Discord へ配信するブリッジシステム。
Tkinter による監視・操作用GUIを内蔵する。

気象警報の状態管理は一次細分区域コード単位で統一しており、
R8 JSON API を用いて常に最新の警報状態を取得する。
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import functools
import hashlib
import heapq
import json
import logging
import logging.handlers
import os
import queue
import random
import re
import sys
import threading
import time
import unicodedata
import xml.etree.ElementTree as ET
from collections import OrderedDict, defaultdict, deque
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock, RLock
from typing import Any

import requests
from bs4 import BeautifulSoup

try:
    import websockets
    _HAS_WEBSOCKETS = True
except ImportError:
    _HAS_WEBSOCKETS = False

# Tkinter
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk

log = logging.getLogger("jpeq_tx")


def request_with_retry(
    method: str,
    url: str,
    retries: int = 3,
    backoff: float = 1.0,
    **kwargs,
) -> requests.Response | None:
    last_exc = None
    for attempt in range(retries):
        try:
            response = requests.request(method, url, **kwargs)
            return response
        except requests.RequestException as e:
            last_exc = e
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
    log.warning(
        "HTTP %s %s failed after %d retries: %s",
        method,
        url,
        retries,
        last_exc,
    )
    return None


# ================================================================
# 二重起動防止 (Single-instance lock)
# ================================================================
_INSTANCE_LOCK_HANDLE = None
_INSTANCE_LOCK_PATH = Path(__file__).resolve().parent / "jpeq_tx.lock"

def _release_single_instance_lock():
    global _INSTANCE_LOCK_HANDLE
    if _INSTANCE_LOCK_HANDLE is not None:
        try:
            _INSTANCE_LOCK_HANDLE.close()
            _INSTANCE_LOCK_PATH.unlink(missing_ok=True)
        except Exception:
            pass
        _INSTANCE_LOCK_HANDLE = None

def acquire_single_instance_lock() -> bool:
    global _INSTANCE_LOCK_HANDLE
    try:
        f = open(_INSTANCE_LOCK_PATH, "a+")
        if os.name == "nt":
            import msvcrt
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                f.close()
                return False
        else:
            import fcntl
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                f.close()
                return False
        try:
            f.seek(0)
            f.truncate()
            f.write(str(os.getpid()))
            f.flush()
        except Exception:
            pass
        _INSTANCE_LOCK_HANDLE = f
        import atexit
        atexit.register(_release_single_instance_lock)
        return True
    except Exception as e:
        log.warning("Instance lock unavailable, continuing without it: %s", e)
        return True

# ================================================================
# Config
# ================================================================
class Config:
    USE_MQTT             = True
    USE_LORA             = True
    USE_DUMMY            = False
    USE_SCHEDULED_TEST   = True
    SCHEDULED_TEST_HOURS = [9, 12, 15, 18, 21]
    TEST_INTERVAL_SEC    = None

    SERIAL_PORT       = "/dev/ttyACM0"
    CHANNEL_INDEX     = 3
    LORA_MQTT_BROKER  = "mqtt.meshtastic.org"
    LORA_MQTT_PORT    = 1883
    LORA_MQTT_TLS     = False
    LORA_MQTT_USER    = "meshdev"
    LORA_MQTT_PASS    = os.environ.get("LORA_MQTT_PASS", "large4cats")
    LORA_CHANNEL      = "JPEQ"
    # 旧ハードコード: LORA_NODE_ID = "!b90f0fde"
    LORA_NODE_ID      = ""
    LORA_CHANNEL_KEY  = "EQ=="
    MQTT_TOPIC_TX     = "msh/JP/"
    MQTT_TOPIC_RX     = "msh/JP/"

    MAX_CHUNK_CHARS   = 200
    SPLIT_WAIT_SEC    = 5
    MSG_INTERVAL_SEC  = 12
    LOOPBACK_TIMEOUT  = 12

    ENABLE_EEW          = True
    ENABLE_EQ           = True
    ENABLE_TSUNAMI      = True

    MIN_SENDO_SCALE     = 10
    EEW_MIN_SCALE       = 30
    USE_LOCAL_FILTER    = False
    LOCAL_REGION        = ""
    LOCAL_PREF          = ""
    LOCAL_EEW_MIN_SCALE = 10
    LOCAL_EQ_MIN_SCALE  = 10

    RECENT_IDS_MAX      = 20
    HISTORY_CACHE_MAX   = 250
    TSUNAMI_CACHE_MAX   = 250
    PROCESSED_WEATHER_IDS_MAX = 5000
    MAX_STATE_HISTORY_ITEMS = 500

    HISTORY_FETCH_LIMIT_EQ  = 100
    HISTORY_FETCH_LIMIT_TS  = 100

    WS_URL              = "wss://api.p2pquake.net/v2/ws"
    WOLFX_WS_URL        = "wss://ws-api.wolfx.jp/jma_eew"
    WS_BACKOFF_INIT     = 3
    WS_BACKOFF_MAX      = 30

    SYSTEM_VERSION      = "JPEQ TX System v0.31 Yakitama Final"
    ENABLE_MAP_URL      = True
    EEW_SEND_MODE       = "all"
    EEW_MAP_URL_MODE    = "first"
    ENABLE_DISCORD       = True
    DISCORD_WEBHOOK_URL  = os.environ.get("JPEQ_DISCORD_WEBHOOK", "")

    ENABLE_WARNING          = True
    ENABLE_MEGAQUAKE        = True
    ENABLE_TYPHOON_INFO     = True
    ENABLE_TYPHOON_MAP_URL  = True
    ENABLE_VOLCANO          = True
    ENABLE_RIVER_FLOOD      = True

    TYPHOON_INTERVAL_SEC    = 10800  # 3時間
    JMA_FEED_URL            = "https://www.data.jma.go.jp/developer/xml/feed/extra.xml"
    JMA_TYPHOON_FEED_URL    = "https://www.data.jma.go.jp/developer/xml/feed/extra_l.xml"
    JMA_EQVOL_FEED_URL      = "https://www.data.jma.go.jp/developer/xml/feed/eqvol.xml"
    JMA_FETCH_INTERVAL_SEC  = 60
    TSUNAMI_FALLBACK_INTERVAL_SEC = 120
    TSUNAMI_USE_JSON_FALLBACK = True
    STARTUP_SUPPRESS_SEC      = 60   # 起動後の警報送信抑制時間（秒）
    TSUNAMI_CANCEL_SUPPRESS_SEC = 90  # 津波解除電文の最小発令間隔（秒）
    TSUNAMI_CANCEL_UNKNOWN_SUPPRESS_SEC = 300  # JMA確認不可時の津波解除抑制時間（秒）
    WARNING_STALE_HOURS       = 6     # 警報が古いと見なす時間（時間）

    # J-ALERT
    ENABLE_JALERT             = True
    JALERT_GAS_URL = os.environ.get("JPEQ_JALERT_GAS_URL", "")
    JALERT_POLL_INTERVAL_SEC = 10  # 10秒間隔（即時性重視、サーバー負荷軽減）
    JALERT_MSG_INTERVAL_SEC = 3    # J-ALERT メッセージ間隔（EEWと同じ3秒）
    URGENT_MSG_INTERVAL_SEC = 3    # 緊急情報（EEW・津波・地震確定報）の送信間隔
    JALERT_YAHOO_URL = "https://emergency-weather.yahoo.co.jp/weather/jp/jalert/"
    JALERT_YAHOO_HOST = "emergency-weather.yahoo.co.jp"
    JALERT_YAHOO_PATH = "/weather/jp/jalert/"
    JALERT_FETCH_TIMEOUT = (5, 15)
    JALERT_FETCH_RETRIES = 2
    JALERT_FETCH_BACKOFF = 1.0

    MAX_NORMAL_QUEUE_SIZE = 100    # 通常キュー上限（超過分は破棄）
    MAX_MEMORIAL_QUEUE_SIZE = 100  # メモリアルキュー上限
    MAX_WEATHER_QUEUE_SIZE = 2000  # 気象警報キュー上限
    MAX_SEND_QUEUE_SIZE = 2200     # 統合送信キュー上限（超過時は低優先度から破棄）
    WEATHER_SEND_INTERVAL_SEC = 3
    RIVER_FLOOD_SEND_INTERVAL_SEC = 3
    TYPHOON_SEND_INTERVAL_SEC = 3
    RESOURCE_MONITOR_INTERVAL_SEC = 300  # リソースモニター出力間隔（秒）

    LOG_FILE_DIR       = "logs"
    LOG_MAX_FILE_SIZE  = 10 * 1024 * 1024

    DUPLICATE_SUPPRESS_SEC = 15.0

    @staticmethod
    def scale_to_label(scale: int) -> str:
        return {10: "1", 20: "2", 30: "3", 40: "4", 45: "5-", 50: "5+", 55: "6-", 60: "6+", 70: "7"}.get(scale, "?")

    @staticmethod
    def scale_to_priority(scale: int) -> str:
        if scale >= 60: return "CRITICAL"
        if scale >= 45: return "HIGH"
        if scale >= 30: return "MID"
        if scale >= 10: return "LOW"
        return "DROP"

    @staticmethod
    def save_defaults():
        data = {}
        for key in dir(Config):
            if key.isupper() and not key.startswith("__"):
                value = getattr(Config, key)
                if isinstance(value, (str, int, float, bool, list, dict, type(None))):
                    data[key] = value
        path = Path(__file__).resolve().parent / "data" / "config.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    @staticmethod
    def load_defaults():
        path = Path(__file__).resolve().parent / "data" / "config.json"
        if not path.exists():
            return
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            for key, value in data.items():
                if hasattr(Config, key):
                    setattr(Config, key, value)
        except Exception as e:
            log.warning("Failed to load config.json: %s", e)

config = Config

# ================================================================
# 監視対象エリア一覧（一次細分区域のみ）
# ================================================================
MONITORED_AREAS: dict[str, dict[str, str]] = {
    # 北海道
    "011000": {"ja": "宗谷地方", "en": "Soya Region, HOKKAIDO", "url_code": "011000"},
    "012010": {"ja": "上川地方", "en": "Kamikawa Region, HOKKAIDO", "url_code": "012000"},
    "012020": {"ja": "留萌地方", "en": "Rumoi Region, HOKKAIDO", "url_code": "012000"},
    "013010": {"ja": "網走地方", "en": "Abashiri Region, HOKKAIDO", "url_code": "013000"},
    "013020": {"ja": "北見地方", "en": "Kitami Region, HOKKAIDO", "url_code": "013000"},
    "013030": {"ja": "紋別地方", "en": "Mombetsu Region, HOKKAIDO", "url_code": "013000"},
    "014010": {"ja": "根室地方", "en": "Nemuro Region, HOKKAIDO", "url_code": "014100"},
    "014020": {"ja": "釧路地方", "en": "Kushiro Region, HOKKAIDO", "url_code": "014100"},
    "014030": {"ja": "十勝地方", "en": "Tokachi Region, HOKKAIDO", "url_code": "014030"},
    "015010": {"ja": "胆振地方", "en": "Iburi Region, HOKKAIDO", "url_code": "015000"},
    "015020": {"ja": "日高地方", "en": "Hidaka Region, HOKKAIDO", "url_code": "015000"},
    "016010": {"ja": "石狩地方", "en": "Ishikari Region, HOKKAIDO", "url_code": "016000"},
    "016020": {"ja": "空知地方", "en": "Sorachi Region, HOKKAIDO", "url_code": "016000"},
    "016030": {"ja": "後志地方", "en": "Shiribeshi Region, HOKKAIDO", "url_code": "016000"},
    "017010": {"ja": "渡島地方", "en": "Oshima Region, HOKKAIDO", "url_code": "017000"},
    "017020": {"ja": "檜山地方", "en": "Hiyama Region, HOKKAIDO", "url_code": "017000"},
    # 東北
    "020010": {"ja": "青森県津軽", "en": "Tsugaru AOMORI", "url_code": "020000"},
    "020020": {"ja": "青森県下北", "en": "Shimokita AOMORI", "url_code": "020000"},
    "020030": {"ja": "青森県三八上北", "en": "Sanpachi-Kamikita AOMORI", "url_code": "020000"},
    "030010": {"ja": "岩手県内陸", "en": "Inland IWATE", "url_code": "030000"},
    "030020": {"ja": "岩手県沿岸北部", "en": "Northern Coast IWATE", "url_code": "030000"},
    "030030": {"ja": "岩手県沿岸南部", "en": "Southern Coast IWATE", "url_code": "030000"},
    "040010": {"ja": "宮城県東部", "en": "Eastern MIYAGI", "url_code": "040000"},
    "040020": {"ja": "宮城県西部", "en": "Western MIYAGI", "url_code": "040000"},
    "050010": {"ja": "秋田県沿岸", "en": "Coastal AKITA", "url_code": "050000"},
    "050020": {"ja": "秋田県内陸", "en": "Inland AKITA", "url_code": "050000"},
    "060010": {"ja": "山形県村山", "en": "Murayama YAMAGATA", "url_code": "060000"},
    "060020": {"ja": "山形県置賜", "en": "Okitama YAMAGATA", "url_code": "060000"},
    "060030": {"ja": "山形県庄内", "en": "Shonai YAMAGATA", "url_code": "060000"},
    "060040": {"ja": "山形県最上", "en": "Mogami YAMAGATA", "url_code": "060000"},
    "070010": {"ja": "福島県中通り", "en": "Nakadori FUKUSHIMA", "url_code": "070000"},
    "070020": {"ja": "福島県浜通り", "en": "Hamadori FUKUSHIMA", "url_code": "070000"},
    "070030": {"ja": "福島県会津", "en": "Aizu FUKUSHIMA", "url_code": "070000"},
    # 関東
    "080010": {"ja": "茨城県北部", "en": "Northern IBARAKI", "url_code": "080000"},
    "080020": {"ja": "茨城県南部", "en": "Southern IBARAKI", "url_code": "080000"},
    "090010": {"ja": "栃木県南部", "en": "Southern TOCHIGI", "url_code": "090000"},
    "090020": {"ja": "栃木県北部", "en": "Northern TOCHIGI", "url_code": "090000"},
    "100010": {"ja": "群馬県南部", "en": "Southern GUNMA", "url_code": "100000"},
    "100020": {"ja": "群馬県北部", "en": "Northern GUNMA", "url_code": "100000"},
    "110010": {"ja": "埼玉県南部", "en": "Southern SAITAMA", "url_code": "110000"},
    "110020": {"ja": "埼玉県北部", "en": "Northern SAITAMA", "url_code": "110000"},
    "110030": {"ja": "埼玉県秩父地方", "en": "Chichibu SAITAMA", "url_code": "110000"},
    "120010": {"ja": "千葉県北西部", "en": "Northwestern CHIBA", "url_code": "120000"},
    "120020": {"ja": "千葉県北東部", "en": "Northeastern CHIBA", "url_code": "120000"},
    "120030": {"ja": "千葉県南部", "en": "Southern CHIBA", "url_code": "120000"},
    "130010": {"ja": "東京地方", "en": "Tokyo Region, TOKYO", "url_code": "130000"},
    "130020": {"ja": "伊豆諸島北部", "en": "Izu Islands North, TOKYO", "url_code": "130000"},
    "130030": {"ja": "伊豆諸島南部", "en": "Izu Islands South, TOKYO", "url_code": "130000"},
    "130040": {"ja": "小笠原諸島", "en": "Ogasawara Islands, TOKYO", "url_code": "130000"},
    "140010": {"ja": "神奈川県東部", "en": "Eastern KANAGAWA", "url_code": "140000"},
    "140020": {"ja": "神奈川県西部", "en": "Western KANAGAWA", "url_code": "140000"},
    # 甲信越
    "150010": {"ja": "新潟県下越", "en": "Kaetsu NIIGATA", "url_code": "150000"},
    "150020": {"ja": "新潟県中越", "en": "Chuetsu NIIGATA", "url_code": "150000"},
    "150030": {"ja": "新潟県上越", "en": "Joetsu NIIGATA", "url_code": "150000"},
    "150040": {"ja": "新潟県佐渡", "en": "Sado NIIGATA", "url_code": "150000"},
    "160010": {"ja": "富山県東部", "en": "Eastern TOYAMA", "url_code": "160000"},
    "160020": {"ja": "富山県西部", "en": "Western TOYAMA", "url_code": "160000"},
    "170010": {"ja": "石川県加賀", "en": "Kaga ISHIKAWA", "url_code": "170000"},
    "170020": {"ja": "石川県能登", "en": "Noto ISHIKAWA", "url_code": "170000"},
    "180010": {"ja": "福井県嶺北", "en": "Reihoku FUKUI", "url_code": "180000"},
    "180020": {"ja": "福井県嶺南", "en": "Reinan FUKUI", "url_code": "180000"},
    "190010": {"ja": "山梨県中・西部", "en": "Central/Western YAMANASHI", "url_code": "190000"},
    "190020": {"ja": "山梨県東部・富士五湖", "en": "Eastern/Fuji Five Lakes YAMANASHI", "url_code": "190000"},
    "200010": {"ja": "長野県北部", "en": "Northern NAGANO", "url_code": "200000"},
    "200020": {"ja": "長野県中部", "en": "Central NAGANO", "url_code": "200000"},
    "200030": {"ja": "長野県南部", "en": "Southern NAGANO", "url_code": "200000"},
    # 東海
    "210010": {"ja": "岐阜県美濃地方", "en": "Mino GIFU", "url_code": "210000"},
    "210020": {"ja": "岐阜県飛騨地方", "en": "Hida GIFU", "url_code": "210000"},
    "220010": {"ja": "静岡県中部", "en": "Central SHIZUOKA", "url_code": "220000"},
    "220020": {"ja": "静岡県伊豆", "en": "Izu SHIZUOKA", "url_code": "220000"},
    "220030": {"ja": "静岡県東部", "en": "Eastern SHIZUOKA", "url_code": "220000"},
    "220040": {"ja": "静岡県西部", "en": "Western SHIZUOKA", "url_code": "220000"},
    "230010": {"ja": "愛知県西部", "en": "Western AICHI", "url_code": "230000"},
    "230020": {"ja": "愛知県東部", "en": "Eastern AICHI", "url_code": "230000"},
    "240010": {"ja": "三重県北中部", "en": "North/Central MIE", "url_code": "240000"},
    "240020": {"ja": "三重県南部", "en": "Southern MIE", "url_code": "240000"},
    # 近畿
    "250010": {"ja": "滋賀県南部", "en": "Southern SHIGA", "url_code": "250000"},
    "250020": {"ja": "滋賀県北部", "en": "Northern SHIGA", "url_code": "250000"},
    "260010": {"ja": "京都府南部", "en": "Southern KYOTO", "url_code": "260000"},
    "260020": {"ja": "京都府北部", "en": "Northern KYOTO", "url_code": "260000"},
    "270000": {"ja": "大阪府", "en": "OSAKA", "url_code": "270000"},
    "280010": {"ja": "兵庫県南部", "en": "Southern HYOGO", "url_code": "280000"},
    "280020": {"ja": "兵庫県北部", "en": "Northern HYOGO", "url_code": "280000"},
    "290010": {"ja": "奈良県北部", "en": "Northern NARA", "url_code": "290000"},
    "290020": {"ja": "奈良県南部", "en": "Southern NARA", "url_code": "290000"},
    "300010": {"ja": "和歌山県北部", "en": "Northern WAKAYAMA", "url_code": "300000"},
    "300020": {"ja": "和歌山県南部", "en": "Southern WAKAYAMA", "url_code": "300000"},
    # 中国
    "310010": {"ja": "鳥取県東部", "en": "Eastern TOTTORI", "url_code": "310000"},
    "310020": {"ja": "鳥取県中・西部", "en": "Central/Western TOTTORI", "url_code": "310000"},
    "320010": {"ja": "島根県東部", "en": "Eastern SHIMANE", "url_code": "320000"},
    "320020": {"ja": "島根県西部", "en": "Western SHIMANE", "url_code": "320000"},
    "320030": {"ja": "島根県隠岐", "en": "Oki SHIMANE", "url_code": "320000"},
    "330010": {"ja": "岡山県南部", "en": "Southern OKAYAMA", "url_code": "330000"},
    "330020": {"ja": "岡山県北部", "en": "Northern OKAYAMA", "url_code": "330000"},
    "340010": {"ja": "広島県南部", "en": "Southern HIROSHIMA", "url_code": "340000"},
    "340020": {"ja": "広島県北部", "en": "Northern HIROSHIMA", "url_code": "340000"},
    # 四国
    "360010": {"ja": "徳島県北部", "en": "Northern TOKUSHIMA", "url_code": "360000"},
    "360020": {"ja": "徳島県南部", "en": "Southern TOKUSHIMA", "url_code": "360000"},
    "370000": {"ja": "香川県", "en": "KAGAWA", "url_code": "370000"},
    "380010": {"ja": "愛媛県中予", "en": "Chuyo EHIME", "url_code": "380000"},
    "380020": {"ja": "愛媛県東予", "en": "Toyo EHIME", "url_code": "380000"},
    "380030": {"ja": "愛媛県南予", "en": "Nanyo EHIME", "url_code": "380000"},
    "390010": {"ja": "高知県中部", "en": "Central KOCHI", "url_code": "390000"},
    "390020": {"ja": "高知県東部", "en": "Eastern KOCHI", "url_code": "390000"},
    "390030": {"ja": "高知県西部", "en": "Western KOCHI", "url_code": "390000"},
    # 九州北部
    "350010": {"ja": "山口県西部", "en": "Western YAMAGUCHI", "url_code": "350000"},
    "350020": {"ja": "山口県中部", "en": "Central YAMAGUCHI", "url_code": "350000"},
    "350030": {"ja": "山口県東部", "en": "Eastern YAMAGUCHI", "url_code": "350000"},
    "350040": {"ja": "山口県北部", "en": "Northern YAMAGUCHI", "url_code": "350000"},
    "400010": {"ja": "福岡県福岡地方", "en": "Fukuoka Region FUKUOKA", "url_code": "400000"},
    "400020": {"ja": "福岡県北九州地方", "en": "Kitakyushu Region FUKUOKA", "url_code": "400000"},
    "400030": {"ja": "福岡県筑豊地方", "en": "Chikuho FUKUOKA", "url_code": "400000"},
    "400040": {"ja": "福岡県筑後地方", "en": "Chikugo FUKUOKA", "url_code": "400000"},
    "410010": {"ja": "佐賀県南部", "en": "Southern SAGA", "url_code": "410000"},
    "410020": {"ja": "佐賀県北部", "en": "Northern SAGA", "url_code": "410000"},
    "420010": {"ja": "長崎県南部", "en": "Southern NAGASAKI", "url_code": "420000"},
    "420020": {"ja": "長崎県北部", "en": "Northern NAGASAKI", "url_code": "420000"},
    "420030": {"ja": "壱岐・対馬", "en": "Iki/Tsushima, NAGASAKI", "url_code": "420000"},
    "420040": {"ja": "長崎県五島", "en": "Goto Islands, NAGASAKI", "url_code": "420000"},
    "430010": {"ja": "熊本県熊本地方", "en": "Kumamoto Region KUMAMOTO", "url_code": "430000"},
    "430020": {"ja": "熊本県阿蘇地方", "en": "Aso Region KUMAMOTO", "url_code": "430000"},
    "430030": {"ja": "熊本県天草・芦北地方", "en": "Amakusa/Ashikita Region KUMAMOTO", "url_code": "430000"},
    "430040": {"ja": "熊本県球磨地方", "en": "Kuma Region KUMAMOTO", "url_code": "430000"},
    "440010": {"ja": "大分県中部", "en": "Central OITA", "url_code": "440000"},
    "440020": {"ja": "大分県北部", "en": "Northern OITA", "url_code": "440000"},
    "440030": {"ja": "大分県西部", "en": "Western OITA", "url_code": "440000"},
    "440040": {"ja": "大分県南部", "en": "Southern OITA", "url_code": "440000"},
    # 九州南部・奄美
    "450010": {"ja": "宮崎県南部平野部", "en": "Southern Plains MIYAZAKI", "url_code": "450000"},
    "450020": {"ja": "宮崎県北部平野部", "en": "Northern Plains MIYAZAKI", "url_code": "450000"},
    "450030": {"ja": "宮崎県南部山沿い", "en": "Southern Mountains MIYAZAKI", "url_code": "450000"},
    "450040": {"ja": "宮崎県北部山沿い", "en": "Northern Mountains MIYAZAKI", "url_code": "450000"},
    "460010": {"ja": "薩摩地方", "en": "Satsuma Region, KAGOSHIMA", "url_code": "460100"},
    "460020": {"ja": "大隅地方", "en": "Osumi Region, KAGOSHIMA", "url_code": "460100"},
    "460030": {"ja": "種子島・屋久島地方", "en": "Tanegashima-Yakushima Region, KAGOSHIMA", "url_code": "460100"},
    "460040": {"ja": "奄美地方", "en": "Amami Region, KAGOSHIMA", "url_code": "460040"},
    # 沖縄
    "471010": {"ja": "本島中南部", "en": "Central/Southern Main Island, OKINAWA", "url_code": "471000"},
    "471020": {"ja": "本島北部", "en": "Northern Main Island, OKINAWA", "url_code": "471000"},
    "471030": {"ja": "久米島", "en": "Kumejima, OKINAWA", "url_code": "471000"},
    "472000": {"ja": "大東島地方", "en": "Daito Islands, OKINAWA", "url_code": "472000"},
    "473000": {"ja": "宮古島地方", "en": "Miyako Islands, OKINAWA", "url_code": "473000"},
    "474010": {"ja": "石垣島地方", "en": "Ishigakijima Region, OKINAWA", "url_code": "474000"},
    "474020": {"ja": "与那国島地方", "en": "Yonagunijima Region, OKINAWA", "url_code": "474000"},
}

# 警報コード → 日本語名（グローバル定数）
WARNING_CODE_TO_NAME: dict[str, str] = {
    "02": "暴風雪警報", "03": "大雨警報", "04": "洪水警報",
    "05": "暴風警報", "06": "大雪警報", "07": "波浪警報",
    "08": "高潮警報", "09": "土砂災害警報",
    "10": "大雨注意報", "12": "大雪注意報", "13": "風雪注意報",
    "14": "雷注意報", "15": "強風注意報", "16": "波浪注意報",
    "17": "融雪注意報", "18": "洪水注意報", "19": "高潮注意報",
    "20": "濃霧注意報", "21": "乾燥注意報", "22": "なだれ注意報",
    "23": "低温注意報", "24": "霜注意報", "25": "着氷注意報",
    "26": "着雪注意報", "29": "土砂災害注意報",
    "32": "暴風雪特別警報", "33": "大雨特別警報",
    "35": "暴風特別警報", "36": "大雪特別警報",
    "37": "波浪特別警報", "38": "高潮特別警報",
    "39": "土砂災害特別警報",
    "43": "大雨危険警報", "48": "高潮危険警報",
    "49": "土砂災害危険警報",
}

_WARNING_TYPE_EN: dict[str, str] = {
    # 特別警報（基本形）
    "大雨特別警報": "Heavy Rain", "暴風特別警報": "Storm", "暴風雪特別警報": "Snowstorm",
    "大雪特別警報": "Heavy Snow", "高潮特別警報": "Storm Surge", "波浪特別警報": "High Wave",
    "氾濫特別警報": "Flood", "土砂災害特別警報": "Landslide",
    # 特別警報（複合形）
    "大雨特別警報（土砂災害）": "Heavy Rain (Landslide)",
    "大雨特別警報（浸水害）":   "Heavy Rain (Inundation)",
    "大雨特別警報（氾濫）":     "Heavy Rain (Flood)",
    "暴風特別警報（波浪）":     "Storm (High Wave)",
    "波浪特別警報（暴風）":     "High Wave (Storm)",
    # 危険警報
    "大雨危険警報": "Heavy Rain (Urgent)", "土砂災害危険警報": "Landslide (Urgent)",
    "氾濫危険警報": "Flood (Urgent)", "高潮危険警報": "Storm Surge (Urgent)",
    # 警報（基本形）
    "大雨警報": "Heavy Rain", "洪水警報": "Flood", "暴風警報": "Storm",
    "暴風雪警報": "Snowstorm", "大雪警報": "Heavy Snow", "高潮警報": "Storm Surge",
    "波浪警報": "High Wave", "地面現象警報": "Landslide", "土砂災害警報": "Landslide",
    "雷警報": "Thunderstorm", "氾濫警報": "Flood",
    # 警報（複合形）
    "大雨警報（土砂災害）": "Heavy Rain (Landslide)",
    "大雨警報（浸水害）":   "Heavy Rain (Inundation)",
    "大雨警報（氾濫）":     "Heavy Rain (Flood)",
    "洪水警報（氾濫）":     "Flood (Inundation)",
    "波浪警報（暴風）":     "High Wave (Storm)",
    "暴風警報（波浪）":     "Storm (High Wave)",
    "暴風雪警報（大雪）":   "Snowstorm (Heavy Snow)",
    "大雪警報（降雪）":     "Heavy Snow",
    "高潮警報（波浪）":     "Storm Surge (High Wave)",
    # 注意報（拡張用）
    "大雨注意報": "Heavy Rain", "土砂災害注意報": "Landslide", "洪水注意報": "Flood",
    "高潮注意報": "Storm Surge", "雷注意報": "Thunderstorm", "波浪注意報": "High Wave",
    "強風注意報": "Strong Wind", "風雪注意報": "Snow and Wind", "大雪注意報": "Heavy Snow",
    "濃霧注意報": "Dense Fog", "乾燥注意報": "Dry Air", "なだれ注意報": "Avalanche",
    "低温注意報": "Low Temperature", "霜注意報": "Frost", "着氷注意報": "Ice Accretion",
    "着雪注意報": "Wet Snow", "融雪注意報": "Snowmelt",
    # 注意報（複合形）
    "大雨注意報（土砂災害）": "Heavy Rain (Landslide)",
    "大雨注意報（浸水害）":   "Heavy Rain (Inundation)",
    "大雨注意報（氾濫）":     "Heavy Rain (Flood)",
    "波浪注意報（暴風）":     "High Wave (Storm)",
}

_DIRECTION_EN: dict[str, str] = {
    "北": "N", "北北東": "NNE", "北東": "NE", "東北東": "ENE",
    "東": "E", "東南東": "ESE", "南東": "SE", "南南東": "SSE",
    "南": "S", "南南西": "SSW", "南西": "SW", "西南西": "WSW",
    "西": "W", "西北西": "WNW", "北西": "NW", "北北西": "NNW",
    "なし": "",
}

# Sort Priority for Kind/Code (smaller = higher priority)
KIND_SORT_PRIORITY: dict[str, int] = {
    "00": 6, "02": 3, "03": 3, "04": 3, "05": 3, "06": 3, "07": 3, "08": 3, "09": 3,
    "10": 4, "12": 4, "13": 4, "14": 4, "15": 4, "16": 4, "17": 4, "18": 4, "19": 4,
    "20": 4, "21": 4, "22": 4, "23": 4, "24": 4, "25": 4, "26": 4, "27": 5, "29": 4,
    "32": 1, "33": 1, "35": 1, "36": 1, "37": 1, "38": 1, "39": 1,
    "43": 2, "48": 2, "49": 2,
}

_LEVEL_EQUIVALENT_PHENOMENA = {"Heavy Rain", "Flood", "Landslide", "Storm Surge"}

# ================================================================
# 噴火警戒レベル コード → レベル変換
# 気象庁防災情報XML 地震火山関連コード表に基づく
# ================================================================
_VOLCANO_CODE_TO_LEVEL: dict[str, int] = {
    "11": 1,  # 活火山であることに留意
    "12": 2,  # 火口周辺規制
    "13": 3,  # 入山規制
    "14": 4,  # 高齢者等避難
    "15": 5,  # 避難
    "41": 5,  # 噴火警報:避難等
    "42": 3,  # 噴火警報:入山規制等
    "43": 3,  # 火口周辺警報:入山規制等
    "44": 2,  # 噴火警報(周辺海域):周辺海域警戒
    "45": 1,  # 平常
    "46": 5,  # 噴火警報:当該居住地域厳重警戒
    "47": 4,  # 噴火警報:当該山麓厳重警戒
}

# ================================================================
# 指定河川洪水予報（VXKOii）判定用定数
# 令和8年度出水期以降のコード体系（表2）に基づく
# ================================================================
_RIVER_FLOOD_CODE_TO_LEVEL: dict[str, int] = {
    "10": 2,  # レベル２氾濫注意報解除
    "20": 2,  # レベル２氾濫注意報（発表）
    "21": 2,  # レベル２氾濫注意報
    "22": 2,  # レベル２氾濫注意報（警報解除）
    "30": 3,  # レベル３氾濫警報（発表）
    "31": 3,  # レベル３氾濫警報
    "40": 4,  # レベル４氾濫危険警報（発表）
    "41": 4,  # レベル４氾濫危険警報
    "51": 5,  # レベル５氾濫特別警報
    "53": 5,  # レベル５氾濫特別警報（氾濫水の予報）
}

_RIVER_FLOOD_LEVEL_LABEL = {5: "EMERGENCY", 4: "URGENT", 3: "WARNING", 2: "ADVISORY"}

def _warning_level(jp_name: str) -> int:
    if not jp_name:
        return 1
    if "特別警報" in jp_name:
        return 5
    if "危険警報" in jp_name:
        return 4
    if "警報" in jp_name and "注意報" not in jp_name:
        return 3
    if "注意報" in jp_name:
        return 2
    return 1

def calc_area_level(kinds: dict) -> int:
    max_lv = 1
    for name_jp in kinds.values():
        lv = _warning_level(name_jp)
        max_lv = max(max_lv, lv)
    return max_lv

def _extract_warning_core_type(jp_name: str) -> str:
    if not jp_name:
        return jp_name
    m: re.Match | None = re.match(r'レベル[１２３４５６７８９0-9]+', jp_name)
    if m:
        return jp_name[m.end():].strip()
    return jp_name

def translate_warning_kind_en(jp_name: str) -> str | None:
    if not jp_name:
        return None
    core: str = _extract_warning_core_type(jp_name)
    return _WARNING_TYPE_EN.get(core)

_unknown_warning_kinds: dict[str, list[str]] = {}
_UNKNOWN_WARNING_PATH = Path(__file__).resolve().parent / "data" / "unknown_warning_kinds.json"
_unknown_warning_lock = threading.Lock()
_unknown_warning_save_timer: threading.Timer | None = None

def _register_unknown_warning_kind(name_jp: str, area_code: str) -> None:
    global _unknown_warning_save_timer
    if not name_jp:
        return
    with _unknown_warning_lock:
        areas = _unknown_warning_kinds.setdefault(name_jp, [])
        if area_code in areas:
            return
        areas.append(area_code)
    log.warning("Unknown warning kind (not in _WARNING_TYPE_EN): %s (area=%s)", name_jp, area_code)
    if _unknown_warning_save_timer is not None:
        _unknown_warning_save_timer.cancel()
    _unknown_warning_save_timer = threading.Timer(30.0, _save_unknown_warning_kinds)
    _unknown_warning_save_timer.daemon = True
    _unknown_warning_save_timer.start()

def _save_unknown_warning_kinds() -> None:
    try:
        _UNKNOWN_WARNING_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _unknown_warning_lock:
            data = dict(_unknown_warning_kinds)
        with _UNKNOWN_WARNING_PATH.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("Failed to save unknown_warning_kinds.json: %s", e)

# ================================================================
# Dictionary & Translation
# ================================================================
_DICT_PATH = Path(__file__).resolve().parent / "data" / "jpeq_dict.json"

_dict: dict[str, dict[str, Any]] = {
    "pref": {}, "epicenter": {}, "sea_area": {},
    "normalization": {}, "obs_point": {},
    "river_names": {},
    "volcano_names": {},
    "volcano_warning_types": {
        "噴火警報(居住地域)": "Eruption Warning (Residential Areas)",
        "噴火警報(火口周辺)": "Eruption Warning (Crater Vicinity)",
        "噴火警報(周辺海域)": "Eruption Warning (Offshore)",
        "噴火予報": "Eruption Forecast",
        "噴火速報": "Eruption Flash Report",
    },
    "local": {}, "pref_code": {}, "warning_types": {},
}
_pref_code_to_en: dict[str, str] = {}
_dict_lock = threading.Lock()

_FACILITY_SUFFIXES = [
    '小学校', '中学校', '高等学校', '高校', '大学', '短期大学', '短大',
    '公民館', '役場', '役所', '市役所', '町役場', '村役場', '振興局',
    '消防署', '警察署', '病院', '診療所', '保健所',
    '青年会館', '体育館', '図書館', '市民センター', '住民センター',
    '合同庁舎', '防災センター', '支所', '出張所', '事務所',
    '気象台', '測候所', '観測所',
]

_PREF_SHORTHAND: dict[str, str] = {
    '北海': '北海道', '青森': '青森県', '岩手': '岩手県', '宮城': '宮城県',
    '秋田': '秋田県', '山形': '山形県', '福島': '福島県', '茨城': '茨城県',
    '栃木': '栃木県', '群馬': '群馬県', '埼玉': '埼玉県', '千葉': '千葉県',
    '東京': '東京都', '神奈川': '神奈川県', '新潟': '新潟県', '富山': '富山県',
    '石川': '石川県', '福井': '福井県', '山梨': '山梨県', '長野': '長野県',
    '岐阜': '岐阜県', '静岡': '静岡県', '愛知': '愛知県', '三重': '三重県',
    '滋賀': '滋賀県', '京都': '京都府', '大阪': '大阪府', '兵庫': '兵庫県',
    '奈良': '奈良県', '和歌山': '和歌山県', '鳥取': '鳥取県', '島根': '島根県',
    '岡山': '岡山県', '広島': '広島県', '山口': '山口県', '徳島': '徳島県',
    '香川': '香川県', '愛媛': '愛媛県', '高知': '高知県', '福岡': '福岡県',
    '佐賀': '佐賀県', '長崎': '長崎県', '熊本': '熊本県', '大分': '大分県',
    '宮崎': '宮崎県', '鹿児島': '鹿児島県', '沖縄': '沖縄県',
}

def _load_dict() -> None:
    global _dict, _pref_code_to_en
    if not _DICT_PATH.exists():
        log.warning("jpeq_dict.json not found: %s", _DICT_PATH)
        return
    try:
        with _DICT_PATH.open("r", encoding="utf-8") as f:
            loaded = json.load(f)
        with _dict_lock:
            for sec in _dict:
                if sec == "volcano_warning_types":
                    # 初期値とファイルの内容をマージ（ファイル優先）
                    merged = dict(_dict[sec])
                    merged.update(loaded.get(sec, {}))
                    _dict[sec] = merged
                else:
                    _dict[sec] = loaded.get(sec, {})
        _pref_code_to_en = {}
        for en_name, code in _dict["pref_code"].items():
            _pref_code_to_en[code] = en_name
        # 動的警報翻訳の読み込み
        for k, v in _dict.get("warning_types", {}).items():
            _WARNING_TYPE_EN[k] = v
        total = sum(len(v) for v in _dict.values())
        log.info("Loaded jpeq_dict.json: %d entries total", total)
    except Exception as e:
        log.warning("Failed loading jpeq_dict.json: %s", e)

def _save_dict() -> None:
    _DICT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _DICT_PATH.open("w", encoding="utf-8") as f:
        json.dump(_dict, f, ensure_ascii=False, indent=2)

_save_dict_timer: threading.Timer | None = None
def _register_unknown(name_jp: str) -> None:
    global _save_dict_timer
    if not name_jp or not name_jp.strip():
        return
    with _dict_lock:
        if name_jp in _dict["local"]:
            return
        _dict["local"][name_jp] = None
    log.info("Registered unknown name: %s", name_jp)
    epicenter_en.cache_clear()
    town_to_romaji.cache_clear()
    if _save_dict_timer is not None:
        _save_dict_timer.cancel()
    _save_dict_timer = threading.Timer(30.0, _save_dict_safe)
    _save_dict_timer.daemon = True
    _save_dict_timer.start()

def _save_dict_safe() -> None:
    try:
        _save_dict()
    except Exception as e:
        log.warning("Dict save error: %s", e)


def _normalize_river_name(name_jp: str) -> str:
    if not name_jp:
        return name_jp
    name_jp = name_jp.strip()
    name_jp = name_jp.replace("　", " ")
    name_jp = name_jp.replace("  ", " ")
    return name_jp


def _find_river_entry(river_name_jp: str) -> str | None:
    normalized = _normalize_river_name(river_name_jp)
    if not normalized:
        return None

    if normalized in _dict["river_names"]:
        return normalized

    for key in _dict["river_names"]:
        key_norm = _normalize_river_name(key)
        if normalized in key_norm or key_norm in normalized:
            return key

    return None


def _register_river_name(river_name_jp: str, pref_en_name: str) -> str | None:
    global _save_dict_timer
    if not river_name_jp or not river_name_jp.strip():
        return None

    key = _find_river_entry(river_name_jp)
    if key is not None:
        return key

    normalized = _normalize_river_name(river_name_jp)
    with _dict_lock:
        _dict["river_names"][normalized] = {
            "en": normalized,
            "pref": pref_en_name or "",
        }
    log.warning("Unknown river registered: %s (pref=%s)", normalized, pref_en_name)

    if _save_dict_timer is not None:
        _save_dict_timer.cancel()
    _save_dict_timer = threading.Timer(30.0, _save_dict_safe)
    _save_dict_timer.daemon = True
    _save_dict_timer.start()

    return normalized

def _remove_facility_suffix(name: str) -> str:
    for suffix in _FACILITY_SUFFIXES:
        if name.endswith(suffix):
            return name[:-len(suffix)].strip()
    return name

def _expand_pref_shorthand(name: str) -> str:
    for short, full in _PREF_SHORTHAND.items():
        if name.startswith(short) and not name.startswith(full):
            return full + name[len(short):]
    return name

def pref_en(pref_jp: str) -> str:
    return _dict["pref"].get(pref_jp, pref_jp)

def _apply_suffix_rules(name_jp: str) -> str:
    rules = [
        ("南方沖", 3, "Off South of ", True), ("北方沖", 3, "Off North of ", True),
        ("東方沖", 3, "Off East of ", True),  ("西方沖", 3, "Off West of ", True),
        ("南東沖", 3, "Off Southeast of ", True), ("北東沖", 3, "Off Northeast of ", True),
        ("南西沖", 3, "Off Southwest of ", True), ("北西沖", 3, "Off Northwest of ", True),
        ("沖", 1, "Off ", True), ("沿岸", 2, " Coast", False),
        ("湾", 1, " Bay", False), ("灘", 1, " Sea", False),
        ("海峡", 2, " Strait", False), ("半島", 2, " Peninsula", False),
        ("島", 1, " Island", False), ("列島", 2, " Islands", False),
        ("近海", 2, "Near ", True), ("付近", 2, "Near ", True),
    ]
    for suffix, cut, affix, is_prefix in rules:
        if name_jp.endswith(suffix):
            base = name_jp[:-cut] if cut > 0 else name_jp
            if not base or base == name_jp:
                continue
            base_en = epicenter_en(base)
            if base_en != base:
                return f"{affix}{base_en}" if is_prefix else f"{base_en}{affix}"
    return name_jp

@functools.lru_cache(maxsize=10000)
def epicenter_en(name_jp: str) -> str:
    if not name_jp:
        return ""
    canonical = _dict["normalization"].get(name_jp, name_jp)
    for sec in ("local", "epicenter", "sea_area", "pref"):
        val = _dict[sec].get(canonical)
        if val is not None:
            return val
    suffix_result = _apply_suffix_rules(canonical)
    if suffix_result != canonical:
        return suffix_result
    _register_unknown(name_jp)
    return name_jp

@functools.lru_cache(maxsize=10000)
def town_to_romaji(town_jp: str, pref_jp: str | None = None) -> str:
    if not town_jp:
        return pref_en(pref_jp) if pref_jp else ""
    canonical = _dict["normalization"].get(town_jp, town_jp)
    if canonical in _dict["local"] and _dict["local"][canonical] is not None:
        return _dict["local"][canonical]
    if canonical in _dict["obs_point"] and _dict["obs_point"][canonical] is not None:
        return _dict["obs_point"][canonical]
    stripped = _remove_facility_suffix(canonical)
    if stripped != canonical:
        for sec in ("local", "obs_point"):
            val = _dict[sec].get(stripped)
            if val is not None:
                return val
    expanded = _expand_pref_shorthand(canonical)
    if expanded != canonical:
        val = _dict["obs_point"].get(expanded)
        if val is not None:
            return val
    for sec in ("epicenter", "sea_area", "pref"):
        val = _dict[sec].get(canonical)
        if val is not None:
            return val
    if pref_jp:
        _register_unknown(town_jp)
        return pref_en(pref_jp)
    _register_unknown(town_jp)
    return town_jp

_load_dict()
epicenter_en.cache_clear()
town_to_romaji.cache_clear()

def get_dict_size() -> int:
    return sum(len(v) for v in _dict.values())

# ----- ログフィルタ -----
class EarthquakeLogFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "[[EARTHQUAKE]]" in msg or "[EEW ALERT]" in msg

class TsunamiLogFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return any(keyword in msg for keyword in (
            "[[TSUNAMI WR]]", "[MAJOR TSUNAMI WARNING]", "[TSUNAMI WARNING]",
            "[TSUNAMI ADVISORY]", "[[TSUNAMI WR]] [PARTIALLY LIFTED]", "[[TSUNAMI WR]] [ALL LIFTED]",
        ))

class EEWLogFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "[EEW" in msg or "handle_eew" in msg

class WeatherLogFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "[WX]" in msg or "[[WEATHER WARNING]]" in msg

class TyphoonLogFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "[TYPHOON" in msg or "[[TYPHOON" in msg or "[TY No." in msg

class JAlertLogFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "J-ALERT" in msg or "<<J-ALERT>>" in msg

class VolcanoLogFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "[VOLCANO" in msg or "[[VOLCANO" in msg

class SizeRotatingFileHandler(logging.FileHandler):
    def __init__(self, base_path: Path, max_bytes: int, encoding="utf-8", delay=False):
        self.base_path = base_path
        self.max_bytes = max_bytes
        self._current_path = self._find_next_path()
        super().__init__(self._current_path, encoding=encoding, delay=delay)

    def _find_next_path(self) -> Path:
        stem = self.base_path.stem
        suffix = self.base_path.suffix
        parent = self.base_path.parent
        if not self.base_path.exists():
            return self.base_path
        i = 1
        while True:
            p = parent / f"{stem}.{i}{suffix}"
            if not p.exists():
                return p
            if p.stat().st_size < self.max_bytes:
                return p
            i += 1

    def emit(self, record):
        if self.stream is None:
            self.stream = self._open()
        if self._current_path.stat().st_size >= self.max_bytes:
            self.close()
            self._current_path = self._find_next_path()
            self.baseFilename = str(self._current_path)
            self.stream = self._open()
        super().emit(record)

# ----- 津波グレード / Tsunami grade -----
GRADE_RANK: dict[str, int] = {"MajorWarning": 3, "Warning": 2, "Advisory": 1, "Watch": 1, "": 0}

def update_dict_offline() -> None:
    print("Please run build_jpeq_dict.py to update jpeq_dict.json")
    print(f"Current dict size: {get_dict_size()}")

# ================================================================
# History Store
# ================================================================
class _BoundedDict(OrderedDict):
    def __init__(self, maxlen: int):
        super().__init__()
        self._maxlen = maxlen
    def add(self, key: str, value: Any):
        if key in self: del self[key]
        self[key] = value
        while len(self) > self._maxlen: self.popitem(last=False)

class HistoryStore:
    def __init__(self, eq_file: Path = Path("data/eq_history.json"),
                 tsunami_file: Path = Path("data/tsunami_history.json")):
        self._lock = RLock()
        self.recent_ids: deque = deque(maxlen=Config.RECENT_IDS_MAX)
        self.history_cache = _BoundedDict(Config.HISTORY_CACHE_MAX)
        self.tsunami_cache = _BoundedDict(Config.TSUNAMI_CACHE_MAX)
        self._active_tsunami_grades: dict[str, str] = {}
        self._eq_file = eq_file
        self._tsunami_file = tsunami_file
        self._save_queue: queue.Queue = queue.Queue()
        self._stop_event = threading.Event()
        self._save_thread = threading.Thread(target=self._save_worker, daemon=True, name="history_saver")
        self._save_thread.start()
        self._tsunami_content_hash: dict[str, str] = {}
        self._load_eq_history()
        self._load_tsunami_history()

    def stop(self):
        self._stop_event.set()
        self._save_thread.join(timeout=2)

    def request_save(self, kind: str) -> None:
        self._save_queue.put(kind)

    def get_active_tsunami_grades(self) -> dict[str, str]:
        with self._lock: return dict(self._active_tsunami_grades)
    def set_active_tsunami_grades(self, grades: dict[str, str]) -> None:
        with self._lock: self._active_tsunami_grades = dict(grades)
    def clear_active_tsunami(self) -> None:
        with self._lock: self._active_tsunami_grades.clear()

    def has_tsunami_for_earthquake(self, eq_event_id: str) -> bool:
        with self._lock:
            for ts_event in self.tsunami_cache.values():
                eq_data = ts_event.get("earthquake", {}) or {}
                if str(eq_data.get("_id", "")) == eq_event_id:
                    return True
        return False
    def is_duplicate(self, event_id: str) -> bool:
        with self._lock: return event_id in self.recent_ids
    def is_tsunami_duplicate(self, event_id: str, areas_hash: str) -> bool:
        with self._lock:
            prev_hash = self._tsunami_content_hash.get(event_id)
            if prev_hash is not None and prev_hash == areas_hash:
                return True
            self._tsunami_content_hash[event_id] = areas_hash
            return False
    def mark_sent(self, event_id: str) -> None:
        with self._lock:
            if event_id not in self.recent_ids: self.recent_ids.append(event_id)
    def has_eq(self, event_id: str) -> bool:
        with self._lock: return event_id in self.history_cache
    def get_event(self, event_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self.history_cache.get(event_id)
    def list_eq(self) -> list[dict[str, Any]]:
        with self._lock:
            items = list(self.history_cache.values())
            items.sort(key=lambda x: x.get("earthquake", {}).get("time", ""), reverse=True)
            return items
    def list_tsunami(self) -> list[dict[str, Any]]:
        with self._lock:
            items = list(self.tsunami_cache.values())
            items.sort(key=lambda x: x.get("time", "") or x.get("issue", {}).get("time", ""), reverse=True)
            return items
    def add_eq(self, event_id: str, payload: dict[str, Any], save: bool = True) -> None:
        with self._lock:
            if "_id" not in payload and event_id: payload["_id"] = event_id
            if "saved_at" not in payload: payload["saved_at"] = datetime.now().isoformat()
            points = payload.get("points", []) or []
            if points:
                max_pt = max(points, key=lambda p: p.get("scale", 0) or 0)
                payload["max_point"] = {"addr": max_pt.get("addr", ""), "pref": max_pt.get("pref", ""), "scale": max_pt.get("scale", 0)}
            else:
                payload["max_point"] = None
            self.history_cache.add(event_id, payload)
            if save: self._save_queue.put("eq")
    def add_tsunami(self, event_id: str, payload: dict[str, Any], save: bool = True) -> None:
        with self._lock:
            if "_id" not in payload and event_id: payload["_id"] = event_id
            if "saved_at" not in payload: payload["saved_at"] = datetime.now().isoformat()
            self.tsunami_cache.add(event_id, payload)
            if save: self._save_queue.put("tsunami")
    def _save_eq_history(self):
        try:
            items = list(self.history_cache.values())[-Config.HISTORY_CACHE_MAX:]
            self._eq_file.parent.mkdir(parents=True, exist_ok=True)
            with self._eq_file.open("w", encoding="utf-8") as f: json.dump(items, f, ensure_ascii=False, indent=2)
        except Exception as e: log.warning("Failed to save eq history: %s", e)
    def _save_tsunami_history(self):
        try:
            items = list(self.tsunami_cache.values())[-Config.TSUNAMI_CACHE_MAX:]
            self._tsunami_file.parent.mkdir(parents=True, exist_ok=True)
            with self._tsunami_file.open("w", encoding="utf-8") as f: json.dump(items, f, ensure_ascii=False, indent=2)
        except Exception as e: log.warning("Failed to save tsunami history: %s", e)
    def _load_eq_history(self):
        if not self._eq_file.exists(): return
        try:
            with self._eq_file.open("r", encoding="utf-8") as f: content = f.read().strip()
            if not content: return
            eq_data = json.loads(content)
            if not isinstance(eq_data, list): return
            count = 0
            for ev in eq_data:
                eid = str(ev.get("_id") or ev.get("id") or "")
                if eid: self.history_cache.add(eid, ev); self.recent_ids.append(eid); count += 1
            log.info("Loaded %d earthquake history from file", count)
        except Exception as e: log.warning("Failed to load eq history: %s", e)
    def _load_tsunami_history(self):
        if not self._tsunami_file.exists(): return
        try:
            with self._tsunami_file.open("r", encoding="utf-8") as f: content = f.read().strip()
            if not content: return
            tsunami_data = json.loads(content)
            if not isinstance(tsunami_data, list): return
            count = 0
            for ev in tsunami_data:
                if ev.get("code") != 552: continue
                eid = str(ev.get("_id") or ev.get("id") or "")
                if eid: self.tsunami_cache.add(eid, ev); self.recent_ids.append(eid); count += 1
            log.info("Loaded %d tsunami history from file (filtered)", count)
        except Exception as e: log.warning("Failed to load tsunami history: %s", e)
    def _save_worker(self):
        while not self._stop_event.is_set():
            try:
                kind = self._save_queue.get(timeout=1)
                if kind == "eq": self._save_eq_history()
                elif kind == "tsunami": self._save_tsunami_history()
                self._save_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                log.warning("History save error: %s", e)
        while not self._save_queue.empty():
            try:
                kind = self._save_queue.get_nowait()
                if kind == "eq": self._save_eq_history()
                elif kind == "tsunami": self._save_tsunami_history()
                self._save_queue.task_done()
            except Exception:
                break

store = HistoryStore()

# ================================================================
# Filters & Message Split
# ================================================================
def _resolve_local_prefs() -> list[str]:
    if config.LOCAL_PREF:
        return [p.strip() for p in config.LOCAL_PREF.split(",") if p.strip()]
    if config.LOCAL_REGION:
        region_map = {
            "HOKKAIDO":           ["HOKKAIDO"],
            "TOHOKU":             ["AOMORI","IWATE","MIYAGI","AKITA","YAMAGATA","FUKUSHIMA"],
            "KANTO-KOSHINETSU":   ["IBARAKI","TOCHIGI","GUNMA","SAITAMA","CHIBA","TOKYO","KANAGAWA","YAMANASHI","NAGANO","NIIGATA"],
            "TOKAI":              ["GIFU","SHIZUOKA","AICHI","MIE"],
            "HOKURIKU":           ["TOYAMA","ISHIKAWA","FUKUI"],
            "KINKI":              ["SHIGA","KYOTO","OSAKA","HYOGO","NARA","WAKAYAMA"],
            "CHUGOKU":            ["TOTTORI","SHIMANE","OKAYAMA","HIROSHIMA","YAMAGUCHI"],
            "SHIKOKU":            ["TOKUSHIMA","KAGAWA","EHIME","KOCHI"],
            "KYUSHU":             ["FUKUOKA","SAGA","NAGASAKI","KUMAMOTO","OITA","MIYAZAKI","KAGOSHIMA"],
            "OKINAWA":            ["OKINAWA"],
        }
        return region_map.get(config.LOCAL_REGION, [])
    return []

def passes_eq_filter(maxScale: int, hypocenter: dict[str, Any], points: list[dict[str, Any]]) -> bool:
    if maxScale < config.MIN_SENDO_SCALE: return False
    if config.USE_LOCAL_FILTER:
        local_match = False
        local_prefs = _resolve_local_prefs()
        epi_name = (hypocenter or {}).get("name", "")
        if local_prefs and any(p in epi_name for p in local_prefs): local_match = True
        for pt in points or []:
            pref = pt.get("pref", ""); addr = pt.get("addr", "")
            if local_prefs and (any(p in pref for p in local_prefs) or any(p in addr for p in local_prefs)): local_match = True; break
        if not local_match: return False
        if maxScale < config.LOCAL_EQ_MIN_SCALE: return False
    return True

def passes_eew_filter(max_scale: int, areas: list[dict[str, Any]]) -> bool:
    if max_scale < config.EEW_MIN_SCALE: return False
    if config.USE_LOCAL_FILTER:
        local_match = False
        local_prefs = _resolve_local_prefs()
        for ar in areas or []:
            name = ar.get("name", "") or ar.get("pref", "")
            if local_prefs and any(p in name for p in local_prefs): local_match = True; break
        if not local_match: return False
        if max_scale < config.LOCAL_EEW_MIN_SCALE: return False
    return True

def filter_local_tsunami_areas(areas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not config.USE_LOCAL_FILTER: return areas
    out = []
    local_prefs = _resolve_local_prefs()
    for ar in areas or []:
        name = ar.get("name", "")
        if local_prefs and any(p in name for p in local_prefs):
            out.append(ar)
    return out

def split_message(text: str, max_bytes: int | None = None, mode: str = "normal") -> list[str]:
    if max_bytes is None:
        max_bytes = config.MAX_CHUNK_CHARS
    TAG_RESERVE = 10
    limit = max_bytes - TAG_RESERVE
    if len(text.encode("utf-8")) <= max_bytes:
        return [text]

    if mode == "memorial":
        url_pattern = re.compile(r'https://\S+')
        urls = list(url_pattern.finditer(text))
        protected_ranges = [(m.start(), m.end()) for m in urls]
        raw_chunks: list[str] = []
        remaining = text
        while len(remaining.encode("utf-8")) > limit:
            lo, hi = 0, len(remaining)
            while lo < hi:
                mid = (lo + hi + 1) // 2
                if len(remaining[:mid].encode("utf-8")) <= limit:
                    lo = mid
                else:
                    hi = mid - 1
            head = remaining[:lo]
            cut = lo
            for start, end in protected_ranges:
                if start < cut < end:
                    cut = start
                    break
            idx = head.rfind(" | ", 0, cut)
            if idx == -1:
                idx = head.rfind("/", 0, cut)
            if idx == -1:
                idx = head.rfind(" ", 0, cut)
            if idx > 0:
                raw_chunks.append(remaining[:idx])
                remaining = remaining[idx + 1:].lstrip()
            else:
                raw_chunks.append(head)
                remaining = remaining[cut:].lstrip()
            remaining = remaining.lstrip()
        if remaining:
            raw_chunks.append(remaining)
        if len(raw_chunks) >= 2:
            last = raw_chunks[-1]
            if (
                not re.search(r'https://', last)
                and len(last.encode("utf-8")) < 50
                and " | " not in last
                and "/" not in last
                and " " not in last
            ):
                candidate = raw_chunks[-2] + " " + last
                if len(candidate.encode("utf-8")) <= limit:
                    raw_chunks[-2] = candidate
                    raw_chunks.pop()
        chunks = raw_chunks
    else:
        lines = text.split("\n")
        chunks: list[str] = []
        current_lines: list[str] = []
        current_len = 0
        for line in lines:
            line_bytes = len(line.encode("utf-8"))
            if line_bytes > limit:
                if current_lines:
                    chunks.append("\n".join(current_lines))
                    current_lines = []
                    current_len = 0
                remaining = line
                while len(remaining.encode("utf-8")) > limit:
                    cut = 0
                    byte_count = 0
                    for i, ch in enumerate(remaining):
                        ch_bytes = len(ch.encode("utf-8"))
                        if byte_count + ch_bytes > limit:
                            break
                        byte_count += ch_bytes
                        cut = i + 1
                    if cut == 0:
                        cut = 1
                    part = remaining[:cut]
                    idx = part.rfind("|")
                    if idx > 0:
                        cut = idx + 1
                    chunks.append(remaining[:cut].strip())
                    remaining = remaining[cut:].strip()
                if remaining:
                    current_lines.append(remaining)
                    current_len = len(remaining.encode("utf-8"))
                continue

            if not current_lines:
                current_lines.append(line)
                current_len = line_bytes
            else:
                combined_len = current_len + 1 + line_bytes
                if combined_len <= limit:
                    current_lines.append(line)
                    current_len = combined_len
                else:
                    chunks.append("\n".join(current_lines))
                    current_lines = [line]
                    current_len = line_bytes
        if current_lines:
            chunks.append("\n".join(current_lines))

    if len(chunks) == 1:
        return chunks
    total = len(chunks)
    final_chunks = []
    for i, chunk in enumerate(chunks, start=1):
        tag = f" <{i}/{total}>"
        tagged = chunk + tag
        if len(tagged.encode("utf-8")) <= max_bytes:
            final_chunks.append(tagged)
        else:
            final_chunks.append(chunk)
    return final_chunks

def build_test_message(lora_available: bool | None = None,
                       mqtt_available: bool | None = None,
                       discord_available: bool | None = None,
                       tag: str = "[[TEST]]") -> str:
    if lora_available is None:
        lora_status = "ACTIVE" if config.USE_LORA else "INACTIVE"
    else:
        lora_status = "ACTIVE" if lora_available else "INACTIVE"

    if mqtt_available is None:
        mqtt_status = "ACTIVE" if config.USE_MQTT else "INACTIVE"
    else:
        mqtt_status = "ACTIVE" if mqtt_available else "INACTIVE"

    if discord_available is None:
        discord_status = "ACTIVE" if (config.ENABLE_DISCORD and config.DISCORD_WEBHOOK_URL) else "INACTIVE"
    else:
        discord_status = "ACTIVE" if discord_available else "INACTIVE"

    megaquake_status = "ACTIVE" if config.ENABLE_MEGAQUAKE else "INACTIVE"
    warning_status = "LEVEL ≥ 3" if config.ENABLE_WARNING else "INACTIVE"
    jalet_status = "ACTIVE" if config.ENABLE_JALERT else "INACTIVE"

    if lora_status == "ACTIVE" and mqtt_status == "ACTIVE" and discord_status == "ACTIVE":
        status_tag = "<ALL WORKING>"
    elif lora_status == "INACTIVE" and mqtt_status == "INACTIVE" and discord_status == "INACTIVE":
        status_tag = "<NOT WORKING>"
    else:
        status_tag = "<PARTIALLY WORKING>"

    eew_min_label = config.scale_to_label(config.EEW_MIN_SCALE)
    eq_min_label = config.scale_to_label(config.MIN_SENDO_SCALE)

    items = [
        f"LoRa:{lora_status}",
        f"MQTT:{mqtt_status}",
        f"Discord:{discord_status}",
        f"EQ Intensity:EEW ≥ {eew_min_label}, EQ ≥ {eq_min_label}",
        "TSUNAMI WR:ACTIVE",
        f"MEGAQUAKE:{megaquake_status}",
        f"WEATHER WR:{warning_status}",
        f"J-ALERT:{jalet_status}"
    ]

    msg = f"{tag} {status_tag}|" + "|".join(items)

    return msg

# ================================================================
# Tsunami / EEW / EQ handlers
# ================================================================
def _area_en(name_jp: str) -> str:
    val = _dict["sea_area"].get(name_jp)
    if val is not None: return val
    return epicenter_en(name_jp) or name_jp

def _bucket_areas(areas: list[dict[str, Any]]):
    major, warn, advisory = [], [], []
    for ar in areas or []:
        grade = ar.get("grade", ""); name = _area_en(ar.get("name", ""))
        if not name: continue
        if grade == "MajorWarning": major.append(name)
        elif grade == "Warning": warn.append(name)
        elif grade in ("Watch", "Advisory"): advisory.append(name)
        else: advisory.append(name)
    return major, warn, advisory

def handle_tsunami(payload: dict[str, Any], send_fn: Callable[[str], None], state: dict[str, Any]) -> None:
    try:
        eid = str(payload.get("_id") or payload.get("id") or "")
        cancelled = payload.get("cancelled", False)
        areas = payload.get("areas", []) or []
        log.info("TSUNAMI received: code=%s, cancelled=%s, areas=%s", payload.get("code"), cancelled, len(areas))
        now = datetime.now().strftime("%Y/%m/%d %H:%M")
        prev_grades = store.get_active_tsunami_grades()

        if cancelled:
            if prev_grades:
                filtered_prev = filter_local_tsunami_areas(
                    [{"name": k, "grade": v} for k, v in prev_grades.items()])
                if filtered_prev:
                    last_ts = state.get("tsunami_last_issue_time", 0)
                    elapsed = time.time() - last_ts
                    if elapsed < config.TSUNAMI_CANCEL_SUPPRESS_SEC:
                        log.warning("Tsunami cancel suppressed (issued %.0fs ago)", elapsed)
                        return
                    jma_status = _check_tsunami_active_jma(use_json=config.TSUNAMI_USE_JSON_FALLBACK)
                    if jma_status is True:
                        log.warning("Tsunami cancel blocked by JMA cross-check")
                        return
                    elif jma_status is None:
                        # JMA確認が取れない場合は、通常より長い抑制時間を適用して誤解除を防ぐ
                        if elapsed < config.TSUNAMI_CANCEL_UNKNOWN_SUPPRESS_SEC:
                            log.warning("Tsunami cancel suppressed due to unknown JMA status (issued %.0fs ago)", elapsed)
                            return
                    send_fn("[[TSUNAMI WR]] <ALL LIFTED>")
                    store.clear_active_tsunami()
            return

        areas = filter_local_tsunami_areas(areas)
        if not areas:
            return
        areas_str = json.dumps(sorted((a.get("name", ""), a.get("grade", "")) for a in areas), ensure_ascii=False)
        areas_hash = hashlib.sha256(areas_str.encode("utf-8")).hexdigest()
        if eid and store.is_tsunami_duplicate(eid, areas_hash):
            return
        major, warn, advisory = _bucket_areas(areas)
        new_grades = {ar.get("name", ""): ar.get("grade", "") for ar in areas if ar.get("name")}
        def _grade_label(grade: str) -> str:
            if grade == "MajorWarning": return "MAJOR TSUNAMI WARNING"
            if grade == "Warning": return "TSUNAMI WARNING"
            if grade in ("Advisory", "Watch"): return "TSUNAMI ADVISORY"
            return grade
        if not prev_grades:
            state["tsunami_last_issue_time"] = time.time()

            eq_data = payload.get("earthquake", {}) or {}
            hypo = eq_data.get("hypocenter", {}) or {}
            epi_name = epicenter_en(hypo.get("name", ""))
            mag = hypo.get("magnitude", "")
            tsunami_header = f"[[TSUNAMI WR]] {now}"
            if epi_name:
                tsunami_header += f"|{epi_name}"
                if mag and mag not in ("", "-"):
                    try:
                        f_mag = float(mag)
                        tsunami_header += f"|M:{int(f_mag)}" if f_mag == int(f_mag) else f"|M:{f_mag:.1f}"
                    except Exception as e:
                        log.warning("Failed to parse tsunami magnitude '%s': %s", mag, e)
            send_fn(tsunami_header)

            if major: send_fn("[MAJOR TSUNAMI WARNING]" + "|".join(major))
            if warn: send_fn("[TSUNAMI WARNING]" + "|".join(warn))
            if advisory: send_fn("[TSUNAMI ADVISORY]" + "|".join(advisory))
            store.set_active_tsunami_grades(new_grades)
        else:
            transitions: dict[str, list[str]] = defaultdict(list)
            lifted_areas: list[str] = []
            for area_jp, old_grade in prev_grades.items():
                new_grade = new_grades.get(area_jp, "")
                old_rank = GRADE_RANK.get(old_grade, 0); new_rank = GRADE_RANK.get(new_grade, 0)
                en = _area_en(area_jp)
                if new_rank > old_rank:
                    transitions[f"U/G: {_grade_label(new_grade)}"].append(en)
                elif new_rank < old_rank:
                    if new_grade == "":
                        lifted_areas.append(en)
                    else:
                        transitions[f"D/G: {_grade_label(new_grade)}"].append(en)
            for key, names in transitions.items():
                send_fn(f"[{key}]" + "|".join(names))
            for area in lifted_areas:
                send_fn(f"[TSUNAMI WR] <LIFTED> {area}")
            if major: send_fn("[MAJOR TSUNAMI WARNING] " + "|".join(major))
            if warn: send_fn("[TSUNAMI WARNING] " + "|".join(warn))
            if advisory: send_fn("[TSUNAMI ADVISORY] " + "|".join(advisory))
            store.set_active_tsunami_grades(new_grades)
        if eid: store.add_tsunami(eid, payload); store.mark_sent(eid)
    except Exception as e:
        log.exception("handle_tsunami crashed: %s", e)
        send_fn(f"[ERROR] Tsunami processing failed: {str(e)[:50]}")

def _check_tsunami_active_jma(use_json: bool = True) -> bool | None:
    try:
        r = request_with_retry(
            "GET",
            config.JMA_FEED_URL,
            retries=2,
            backoff=1.0,
            timeout=(5, 15),
        )
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            ATOM_NS = "http://www.w3.org/2005/Atom"
            for entry in root.findall(f"{{{ATOM_NS}}}entry"):
                entry_id_elem = entry.find(f"{{{ATOM_NS}}}id")
                entry_id = entry_id_elem.text if entry_id_elem is not None else ""
                if "VTSE41" in entry_id:
                    link = entry.find(f"{{{ATOM_NS}}}link")
                    href = link.get("href") if link is not None else ""
                    if href:
                        try:
                            r2 = request_with_retry(
                                "GET",
                                href,
                                retries=2,
                                backoff=1.0,
                                timeout=(3, 10),
                            )
                            if r2.status_code == 200:
                                detail_root = ET.fromstring(r2.content)
                                for item in detail_root.iter():
                                    tag = item.tag.split('}')[-1] if '}' in item.tag else item.tag
                                    if tag == 'Item':
                                        status_elem = None
                                        for child in item:
                                            ctag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                                            if ctag == 'Status':
                                                status_elem = child
                                                break
                                        if status_elem is not None and status_elem.text:
                                            if "解除" in status_elem.text:
                                                continue
                                            return True
                        except Exception:
                            continue
        else:
            log.warning("JMA feed fetch failed during tsunami cross-check, trying JSON fallback")
    except Exception as e:
        log.warning(f"JMA XML cross-check error: {e}, trying JSON fallback")

    if not use_json:
        return None

    JSON_URL = "https://www.data.jma.go.jp/multi/data/VTSE41/warning.json"
    try:
        r = request_with_retry(
            "GET",
            JSON_URL,
            retries=2,
            backoff=1.0,
            timeout=(5, 15),
            headers={"Cache-Control": "no-cache"},
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                tsunami_codes = {"51", "52", "53", "54", "55"}
                for item in data:
                    if isinstance(item, dict):
                        code = str(item.get("code", ""))
                        if code in tsunami_codes:
                            return True
                return False
        else:
            log.warning("Multi-lang JSON fetch failed during tsunami cross-check")
    except Exception as e:
        log.warning(f"Multi-lang JSON cross-check error: {e}")

    return None

_WOLFX_INTENSITY_MAP = {"1": 10, "2": 20, "3": 30, "4": 40, "5-": 45, "5+": 50, "6-": 55, "6+": 60, "7": 70}

def wolfx_to_internal_eew(wolfx_data: dict[str, Any]) -> dict[str, Any]:
    is_warn = wolfx_data.get("isWarn", False); is_cancel = wolfx_data.get("isCancel", False)
    hypo_name = wolfx_data.get("Hypocenter", ""); magnitude = wolfx_data.get("Magunitude", -1)
    max_intensity_str = wolfx_data.get("MaxIntensity", "1"); intensity_scale = _WOLFX_INTENSITY_MAP.get(max_intensity_str, 10)
    lat = wolfx_data.get("Latitude", ""); lon = wolfx_data.get("Longitude", ""); depth = wolfx_data.get("Depth", "")
    event_id = str(wolfx_data.get("EventID", "")); serial = wolfx_data.get("Serial", 1)
    origin_time_display = ""
    origin_time_str = wolfx_data.get("OriginTime", "")
    if origin_time_str:
        parts = origin_time_str.split(" ")
        if len(parts) >= 2: origin_time_display = f"{parts[0]} {':'.join(parts[1].split(':')[:2])}"
    areas = []
    if hypo_name: areas.append({"name": hypo_name, "scaleTo": intensity_scale, "scaleFrom": 0})
    return {
        "code": 562 if is_warn else 561, "_id": event_id, "serial": serial, "isCancel": is_cancel,
        "earthquake": {"hypocenter": {"name": hypo_name, "magnitude": magnitude if magnitude != -1 else None, "latitude": lat, "longitude": lon, "depth": depth}, "origin_time": origin_time_display},
        "areas": areas, "_wolfx_original": wolfx_data
    }

def wolfx_to_internal_eq(wolfx_data: dict[str, Any]) -> dict[str, Any] | None:
    eq = wolfx_data.get("earthquake", {})
    if not eq:
        return None
    hypo = eq.get("hypocenter", {})
    return {
        "code": 551,
        "_id": str(wolfx_data.get("eventId", "")),
        "earthquake": {
            "time": wolfx_data.get("originTime", ""),
            "hypocenter": {
                "name": hypo.get("name", ""),
                "latitude": hypo.get("latitude", ""),
                "longitude": hypo.get("longitude", ""),
                "depth": hypo.get("depth", ""),
                "magnitude": hypo.get("magnitude", ""),
            },
            "maxScale": eq.get("maxScale", 0),
        },
        "points": wolfx_data.get("points", []),
    }

def _max_predicted_scale(eew: dict[str, Any]) -> int:
    areas = eew.get("areas", []) or []
    max_scale = 0
    for ar in areas:
        scale = max(ar.get("scaleTo", 0) or 0, ar.get("scaleFrom", 0) or 0)
        max_scale = max(max_scale, scale)
    return max_scale

_MAX_EEW_IDS = 5000

class _OrderedSet:
    def __init__(self, maxlen: int = _MAX_EEW_IDS):
        self._lock = threading.Lock()
        self._data: dict[str, None] = {}
        self._maxlen = maxlen

    def add(self, item: str):
        self.add_if_not_exists(item)

    def add_if_not_exists(self, item: str) -> bool:
        with self._lock:
            if item in self._data:
                return False
            self._data[item] = None
            if len(self._data) > self._maxlen:
                oldest = next(iter(self._data))
                del self._data[oldest]
            return True

    def __contains__(self, item: str) -> bool:
        with self._lock:
            return item in self._data

    def discard(self, item: str):
        with self._lock:
            self._data.pop(item, None)

    def clear(self):
        with self._lock:
            self._data.clear()

    def __len__(self):
        with self._lock:
            return len(self._data)

_UNSENT_FINAL_PATH = Path(__file__).resolve().parent / "data" / "unsent_final_queue.json"
_unsent_final_lock = threading.Lock()


def _load_unsent_finals() -> dict[str, Any]:
    if not _UNSENT_FINAL_PATH.exists():
        return {}
    try:
        with _UNSENT_FINAL_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_unsent_final(event_id: str, payload: dict[str, Any]) -> None:
    with _unsent_final_lock:
        data = _load_unsent_finals()
        data[event_id] = {
            "payload": payload,
            "saved_at": time.time(),
        }
        _UNSENT_FINAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _UNSENT_FINAL_PATH.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


def _remove_unsent_final(event_id: str) -> None:
    with _unsent_final_lock:
        data = _load_unsent_finals()
        if event_id in data:
            del data[event_id]
        _UNSENT_FINAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _UNSENT_FINAL_PATH.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


def handle_eew(payload: dict[str, Any], send_fn: Callable[[str], None], state: dict[str, Any],
               eew_send_lora_only: Callable[[str], None] | None = None,
               eew_send_discord_combined: Callable[[str, str], None] | None = None) -> None:
    try:
        eid = str(payload.get("_id") or payload.get("id") or "")
        eew_data = payload.get("earthquake", {}) or {}; hypo = eew_data.get("hypocenter", {}) or {}
        max_scale = _max_predicted_scale(payload); areas = payload.get("areas", []) or []
        is_cancel = payload.get("isCancel", False); serial = payload.get("serial")

        log.info(
            "EEW received: eid=%s serial=%s max_scale=%s areas=%s cancel=%s",
            eid,
            serial,
            max_scale,
            len(areas),
            is_cancel,
        )

        if serial is None and max_scale == 0 and not areas and not is_cancel:
            log.debug("EEW invalid data skipped: eid=%s", eid)
            return

        if not is_cancel:
            processed = state.setdefault("eew_processed_ids", _OrderedSet())
            norm_serial = str(serial).zfill(4) if serial is not None else None
            key = f"{eid}:{norm_serial}" if norm_serial is not None else eid
            if not processed.add_if_not_exists(key):
                log.info("EEW duplicate skipped: eid=%s serial=%s", eid, serial)
                return

        if config.EEW_SEND_MODE == "first_cancel":
            seen_eids_set = state.setdefault("eew_seen_eids", _OrderedSet())
            if serial is not None: is_first = (int(serial) == 1)
            else: is_first = seen_eids_set.add_if_not_exists(eid) if eid else False
            if not is_first and not is_cancel: return
        if is_cancel:
            last_msg_dict = state.setdefault("eew_last_msg", {})
            last_eew_msg = last_msg_dict.pop(eid, "")
            if last_eew_msg: send_fn(f"[EEW] <CANCELLED> {last_eew_msg}")
            state["eew_lock"] = False
            state.pop("eew_hypocenter", None); state.pop("eew_magnitude", None)
            state.get("eew_last_sent", {}).pop(eid, None)
            state.get("eew_serial_counter", {}).pop(eid, None)
            timers = state.get("eew_throttle_timers", {})
            if eid in timers:
                try: timers[eid].cancel()
                except Exception as e: log.warning("Failed to cancel EEW timer: %s", e)
                del timers[eid]
            seen = state.get("eew_seen_eids")
            if seen:
                seen.discard(eid)
            return
        if not passes_eew_filter(max_scale, areas):
            log.info(
                "EEW filtered by scale/local filter: serial=%s max_scale=%s min_scale=%s",
                serial,
                max_scale,
                config.EEW_MIN_SCALE,
            )
            return
        seen_eids_set = state.setdefault("eew_seen_eids", _OrderedSet())
        sent_eids_set = state.setdefault("eew_sent_ids", _OrderedSet())
        if serial is not None: is_first_report = (int(serial) == 1)
        else: is_first_report = seen_eids_set.add_if_not_exists(eid) if eid else False
        if eid and serial is not None:
            seen_eids_set.add(eid)
        origin_time_display = eew_data.get("origin_time", "")
        if serial is not None: report_num = int(serial)
        else:
            counter = state.setdefault("eew_serial_counter", {})
            if len(counter) > _MAX_EEW_IDS:
                keys_to_remove = list(counter.keys())[:_MAX_EEW_IDS//2]
                for k in keys_to_remove:
                    del counter[k]
            report_num = counter.get(eid, 0) + 1
            counter[eid] = report_num
        if is_first_report and origin_time_display:
            parts = [f"[EEW #{report_num}] {origin_time_display}"]
        else:
            parts = [f"[EEW #{report_num}]"]
        name_jp = hypo.get("name", "")
        if name_jp: parts.append(epicenter_en(name_jp))
        mag = hypo.get("magnitude")
        if mag is not None and mag not in ("", "-"):
            try:
                f_mag = float(mag)
                if f_mag >= 0: parts.append(f"M:{int(f_mag)}" if f_mag == int(f_mag) else f"M:{f_mag:.1f}")
            except Exception as e:
                log.warning("Failed to parse EEW magnitude '%s': %s", mag, e)
        if max_scale > 0: parts.append(f"I:{config.scale_to_label(max_scale)}")
        depth = hypo.get("depth")
        if depth is not None and depth not in ("", "-"):
            try:
                f_depth = float(depth)
                if f_depth > 0: parts.append(f"D:{int(f_depth)}km")
            except Exception as e:
                log.warning("Failed to parse EEW depth '%s': %s", depth, e)
        lat = hypo.get("latitude"); lon = hypo.get("longitude")
        INVALID_COORDS = (None, "-", "", -200, "-200")
        if lat not in INVALID_COORDS and lon not in INVALID_COORDS:
            try:
                f_lat, f_lon = float(lat), float(lon)
                if -90 <= f_lat <= 90 and -180 <= f_lon <= 180:
                    lat_str = f"{abs(f_lat):.1f}{'N' if f_lat >= 0 else 'S'}"
                    lon_str = f"{abs(f_lon):.1f}{'E' if f_lon >= 0 else 'W'}"
                    parts.append(f"({lat_str}, {lon_str})")
            except Exception as e:
                log.warning("Failed to parse EEW coordinates lat=%s lon=%s: %s", lat, lon, e)
        if len(parts) > 1:
            if is_first_report:
                msg = parts[0] + "|" + "|".join(parts[1:])
            else:
                msg = parts[0] + " " + "|".join(parts[1:])
        else:
            msg = parts[0]

        is_first_send = sent_eids_set.add_if_not_exists(eid) if eid else False
        eew_map_url = ""
        if config.EEW_MAP_URL_MODE != "none" and lat not in INVALID_COORDS and lon not in INVALID_COORDS:
            try:
                f_lat, f_lon = float(lat), float(lon)
                if -90 <= f_lat <= 90 and -180 <= f_lon <= 180:
                    add_url = False
                    if config.EEW_MAP_URL_MODE == "all" or config.EEW_MAP_URL_MODE == "first" and is_first_send: add_url = True
                    if add_url:
                        eew_map_url = f"https://jpeqtx.github.io/map/map.html?lat={f_lat:.1f}&lng={f_lon:.1f}&type=ep"
            except Exception as e:
                log.warning("Failed to generate EEW map URL: %s", e)

        last_msg_dict = state.setdefault("eew_last_msg", {})
        # 修正2: キャンセル報のために、ヘッダーを除いたイベント詳細のみを保存
        detail_only = " | ".join(parts[1:])
        last_msg_dict[eid] = detail_only
        state["eew_lock"] = True; state["eew_hypocenter"] = hypo; state["eew_magnitude"] = eew_data.get("magnitude", None)
        THROTTLE_INTERVAL = 3.0
        timers = state.setdefault("eew_throttle_timers", {}); last_sent = state.setdefault("eew_last_sent", {}).get(eid, 0.0)
        if serial is not None and int(serial) >= 4:
            now_ts = time.time()
            if eid in timers:
                try: timers[eid].cancel()
                except Exception as e: log.warning("Failed to cancel EEW throttle timer: %s", e)
                del timers[eid]
            elapsed = now_ts - last_sent
            if elapsed < THROTTLE_INTERVAL:
                def _send_latest():
                    if eew_map_url:
                        tag = f"[EEW #{report_num}]" if report_num else "[EEW]"
                        map_line = f"{tag} Epicenter Map URL {eew_map_url}"
                        if eew_send_lora_only:
                            eew_send_lora_only(msg)
                            eew_send_lora_only(map_line)
                        else:
                            send_fn(msg)
                            send_fn(map_line)
                        if eew_send_discord_combined:
                            eew_send_discord_combined(msg, map_line)
                    else:
                        send_fn(msg)
                    state["eew_last_sent"][eid] = time.time()
                    if eid in timers: del timers[eid]
                delay = THROTTLE_INTERVAL - elapsed
                t = threading.Timer(delay, _send_latest); t.daemon = True; timers[eid] = t; t.start()
                return
        log.info(
            "EEW sending: eid=%s serial=%s report_num=%s max_scale=%s",
            eid,
            serial,
            report_num,
            max_scale,
        )

        if eew_map_url:
            tag = f"[EEW #{report_num}]" if report_num else "[EEW]"
            map_line = f"{tag} Epicenter Map URL {eew_map_url}"
            if eew_send_lora_only:
                eew_send_lora_only(msg)
                eew_send_lora_only(map_line)
            else:
                send_fn(msg)
                send_fn(map_line)
            if eew_send_discord_combined:
                eew_send_discord_combined(msg, map_line)
        else:
            send_fn(msg)
        if eid:
            state["eew_last_sent"][eid] = time.time()
    except Exception as e:
        log.exception("handle_eew crashed: %s", e)
        send_fn(f"[ERROR] EEW processing failed: {str(e)[:50]}")

def _eew_tag(state: dict[str, Any]) -> str:
    return " [EEW ACTIVE]" if state.get("eew_lock") else ""

def _parse_time(time_str: str) -> str:
    if not time_str: return ""
    parts = time_str.split(":")
    if len(parts) >= 3: return ":".join(parts[:2])
    return time_str

def handle_event(payload: dict[str, Any], send_fn: Callable[[str], None], state: dict[str, Any], code: int | None = None) -> bool:
    try:
        eid = str(payload.get("_id") or payload.get("id") or "")
        eq = payload.get("earthquake", {}) or {}; hypo = eq.get("hypocenter", {}) or {}
        points = payload.get("points", []) or []; max_scale = eq.get("maxScale", 0) or 0
        if not passes_eq_filter(max_scale, hypo, points): return False
        if eid and store.has_eq(eid):
            existing = store.get_event(eid)
            # 554 (Preliminary) からの更新は許可
            if existing and existing.get("code") == 554:
                pass  # 下の更新処理へ進む
            else:
                # 既に完全な情報で保存されているかチェック
                existing_eq = existing.get("earthquake", {}) if existing else {}
                existing_hypo = existing_eq.get("hypocenter", {}) if existing_eq else {}
                existing_mag = existing_hypo.get("magnitude")
                existing_lat = existing_hypo.get("latitude")

                # 既存の情報が完全（Mと座標がある）なら、今回の情報を重複とみなす
                has_existing_full = (existing_mag is not None and existing_mag not in ("", "-") and
                                     existing_lat is not None and existing_lat not in ("", "-", None, -200, "-200"))
                if has_existing_full:
                    return True

                # 既存の情報が不完全で、今回の情報も不完全なら重複とみなす
                mag_val = hypo.get("magnitude")
                lat_val = hypo.get("latitude")
                has_current_full = (mag_val is not None and mag_val not in ("", "-") and
                                    lat_val is not None and lat_val not in ("", "-", None, -200, "-200"))
                if not has_current_full:
                    return True
                # 既存が不完全で、今回が完全なら、更新を許可する（下の処理へ）
        if eid:
            lat = hypo.get("latitude")
            lon = hypo.get("longitude")
            INVALID_COORDS = (None, "-", "", -200, "-200")
            map_url = ""
            if (
                config.ENABLE_MAP_URL
                and lat not in INVALID_COORDS
                and lon not in INVALID_COORDS
            ):
                try:
                    f_lat, f_lon = float(lat), float(lon)
                    if -90 <= f_lat <= 90 and -180 <= f_lon <= 180:
                        map_url = f"https://jpeqtx.github.io/map/map.html?lat={f_lat:.1f}&lng={f_lon:.1f}&type=ep"
                        mag_val = hypo.get("magnitude")
                        if mag_val is not None and mag_val not in ("", "-"):
                            try:
                                f_mag = float(mag_val)
                                if f_mag >= 0:
                                    map_url += f"&mag={f_mag:.1f}"
                            except Exception:
                                pass
                except Exception:
                    pass
            if map_url:
                payload["map_url"] = map_url
            else:
                payload.pop("map_url", None)
            store.add_eq(eid, payload)
            store.mark_sent(eid)
        return True
    except Exception as e:
        log.exception("handle_event crashed: %s", e)
        send_fn(f"[ERROR] Event processing failed: {str(e)[:50]}")
        return False

def _build_felt_area(points: list[dict[str, Any]]) -> str:
    if not points: return ""
    pref_max: dict[str, int] = defaultdict(int)
    for pt in points:
        pref = pt.get("pref", ""); scale = pt.get("scale", 0) or 0
        if scale < config.MIN_SENDO_SCALE: continue
        pref_max[pref] = max(pref_max[pref], scale)
    if not pref_max: return ""
    items = sorted(pref_max.items(), key=lambda kv: kv[1], reverse=True)
    chunks = [f"{pref_en(pref)} {config.scale_to_label(scale)}" for pref, scale in items]
    return "|".join(chunks)

def _cleanup_eew_state(state: dict[str, Any], eid: str):
    state.get("eew_last_sent", {}).pop(eid, None)
    state.get("eew_serial_counter", {}).pop(eid, None)
    timers = state.get("eew_throttle_timers", {})
    if eid in timers:
        try: timers[eid].cancel()
        except Exception:
            log.warning("Failed to cancel EEW timer for %s during cleanup", eid)
        del timers[eid]
    state.get("eew_last_msg", {}).pop(eid, None)
    for key in ("eew_seen_eids", "eew_sent_ids", "eew_processed_ids"):
        s = state.get(key)
        if s:
            s.discard(eid)

def _periodic_cleanup_eew_state(state: dict[str, Any]):
    now = time.time()
    for key in ("eew_last_sent",):
        d = state.get(key, {})
        expired = [eid for eid, ts in d.items() if now - ts > 3600]
        for eid in expired:
            _cleanup_eew_state(state, eid)

def handle_eq(payload: dict[str, Any], send_fn: Callable[[str], None], state: dict[str, Any]) -> None:
    try:
        eq = payload.get("earthquake", {}) or {}; hypo = eq.get("hypocenter", {}) or {}
        points: list[dict[str, Any]] = payload.get("points", []) or []; max_scale = eq.get("maxScale", 0) or 0
        eew_hypo = state.pop("eew_hypocenter", None); eew_mag = state.pop("eew_magnitude", None)
        state["eew_lock"] = False
        if eew_hypo:
            if not hypo.get("name"): hypo["name"] = eew_hypo.get("name", "")
            if not hypo.get("latitude") and not hypo.get("longitude"):
                hypo["latitude"] = eew_hypo.get("latitude", ""); hypo["longitude"] = eew_hypo.get("longitude", "")
            if not hypo.get("magnitude") and eew_mag: hypo["magnitude"] = eew_mag
        if not passes_eq_filter(max_scale, hypo, points): return
        eew_tag = _eew_tag(state)
        time_str = _parse_time(eq.get("time", ""))
        epi = epicenter_en(hypo.get("name", "")) or "Unknown"

        # 不完全な情報（M・座標・深さのいずれか欠落）は暫定値扱い
        mag_val = hypo.get("magnitude")
        lat_val = hypo.get("latitude")
        lon_val = hypo.get("longitude")
        depth_val = hypo.get("depth")
        INVALID = ("", "-", None, -200, "-200")
        has_full_info = (
            mag_val is not None
            and mag_val not in INVALID
            and lat_val is not None
            and lat_val not in INVALID
            and lon_val is not None
            and lon_val not in INVALID
            and depth_val is not None
            and depth_val not in INVALID
        )

        # PRELIM連続送信抑制（同一イベントで30秒以内の再送信を防止）
        eid = str(payload.get("_id") or payload.get("id") or "")
        if not has_full_info and eid:
            now_ts = time.time()
            prelim_last = state.setdefault("prelim_last_sent", {})
            last = prelim_last.get(eid, 0)
            if now_ts - last < 30:
                log.info(
                    "PRELIM suppressed for %s (%.0fs since last prelim)",
                    eid,
                    now_ts - last,
                )
                return
            prelim_last[eid] = now_ts

        if not has_full_info or state.get("is_preliminary"):
            header_tag = " [PRELIM]"
        else:
            header_tag = " [FINAL]"

        # 未送信FINAL永続化：確定報を受信したら保存
        eid = str(payload.get("_id") or payload.get("id") or "")
        if header_tag == " [FINAL]" and eid:
            _save_unsent_final(eid, payload)

        eq_prefix = "EQ"

        send_fn(f"[[EARTHQUAKE]] {time_str} {epi}{eew_tag}{header_tag}")
        priority = config.scale_to_priority(max_scale)
        if priority == "DROP": return
        header = f"[{eq_prefix}]"
        num_parts = []
        mag = hypo.get("magnitude")
        if mag is not None and mag not in ("", "-"):
            try:
                f = float(mag)
                if f >= 0: num_parts.append(f"M:{int(f)}" if f == int(f) else f"M:{f:.1f}")
            except Exception as e:
                log.warning("Failed to parse EQ magnitude '%s': %s", mag, e)
        num_parts.append(f"I:{config.scale_to_label(max_scale)}")
        depth = hypo.get("depth")
        if depth is not None and depth not in ("", "-"):
            try:
                f_depth = float(depth)
                if f_depth > 0: num_parts.append(f"D:{int(f_depth)}km")
            except Exception as e:
                log.warning("Failed to parse EQ depth '%s': %s", depth, e)
        lat = hypo.get("latitude"); lon = hypo.get("longitude")
        INVALID_COORDS = (None, "-", "", -200, "-200")
        if lat not in INVALID_COORDS and lon not in INVALID_COORDS:
            try:
                f_lat, f_lon = float(lat), float(lon)
                if -90 <= f_lat <= 90 and -180 <= f_lon <= 180:
                    lat_str = f"{abs(f_lat):.1f}{'N' if f_lat >= 0 else 'S'}"
                    lon_str = f"{abs(f_lon):.1f}{'E' if f_lon >= 0 else 'W'}"
                    num_parts.append(f"({lat_str}, {lon_str})")
            except Exception as e:
                log.warning("Failed to parse EQ coordinates lat=%s lon=%s: %s", lat, lon, e)

        if epi:
            msg1 = f"{header} {epi}|" + "|".join(num_parts) + eew_tag
        else:
            msg1 = f"{header}|" + "|".join(num_parts) + eew_tag

        eq_map_url = ""
        if config.ENABLE_MAP_URL and lat not in INVALID_COORDS and lon not in INVALID_COORDS:
            try:
                f_lat, f_lon = float(lat), float(lon)
                if -90 <= f_lat <= 90 and -180 <= f_lon <= 180:
                    eq_map_url = f"https://jpeqtx.github.io/map/map.html?lat={f_lat:.1f}&lng={f_lon:.1f}&type=ep"
                    # マグニチュードを追加（波紋アニメーション用）
                    mag_val = hypo.get("magnitude")
                    if mag_val is not None and mag_val not in ("", "-"):
                        try:
                            f_mag = float(mag_val)
                            if f_mag >= 0:
                                eq_map_url += f"&mag={f_mag:.1f}"
                        except Exception:
                            pass
            except Exception as e:
                log.warning("Failed to generate EQ map URL: %s", e)

        if eq_map_url:
            payload["map_url"] = eq_map_url
        send_fn(msg1)
        if eq_map_url:
            map_msg = f"[{eq_prefix}] Epicenter Map URL {eq_map_url}"
            send_fn(map_msg)
        if points:
            max_pt = max(points, key=lambda p: p.get("scale", 0) or 0)
            addr = max_pt.get("addr", ""); pref = max_pt.get("pref", "")
            if addr: tail = town_to_romaji(addr, pref)
            elif pref: tail = pref_en(pref)
            else: tail = "Unknown"
            max_scale_label = config.scale_to_label(max_pt.get("scale", 0) or 0)
            send_fn(f"[{eq_prefix}] Max:{tail} I:{max_scale_label}{eew_tag}")
        felt = _build_felt_area(points)
        if felt: send_fn(f"[{eq_prefix}] FELT AREA: {felt}{eew_tag}")

        eid = str(payload.get("_id") or payload.get("id") or "")
        if eid and not store.has_tsunami_for_earthquake(eid):
            send_fn(f"[{eq_prefix}] NO TSUNAMI THREAT")

        if eid:
            # 送信が完了したので未送信FINALから削除
            if header_tag == " [FINAL]":
                _remove_unsent_final(eid)
            # 履歴にURLを確実に保存するため、ここで再保存する
            store.add_eq(eid, payload)
            _cleanup_eew_state(state, eid)
    except Exception as e:
        log.exception("handle_eq crashed: %s", e)
        send_fn(f"[ERROR] EQ processing failed: {str(e)[:50]}")

# ================================================================
# Meshtastic MQTT
# ================================================================
try:
    from meshtastic.protobuf import mesh_pb2, mqtt_pb2, portnums_pb2
    _HAS_PROTO = True
except Exception: _HAS_PROTO = False

try:
    import paho.mqtt.client as mqtt
    _HAS_MQTT = True
except Exception: _HAS_MQTT = False

try:
    from Crypto.Cipher import AES
    _HAS_CRYPTO = True
except Exception: _HAS_CRYPTO = False

def _xor_hash(data: bytes) -> int:
    h = 0
    for b in data: h ^= b
    return h & 0xFF

def _expand_psk(psk_b64: str) -> bytes:
    DEFAULT_PSK = bytes([0xd4,0xf1,0xbb,0x3a,0x20,0x29,0x07,0x59,0xf0,0xbc,0xff,0xab,0xcf,0x4e,0x69,0x01])
    raw = base64.b64decode(psk_b64 + "==")
    if len(raw) <= 1:
        if len(raw) == 0 or raw[0] == 0: return b""
        kb = bytearray(DEFAULT_PSK); kb[15] = (kb[15] + raw[0] - 1) & 0xFF
        return bytes(kb)
    if len(raw) in (16, 32): return raw
    return (raw + b"\x00"*16)[:16]

def _aes_ctr(key: bytes, packet_id: int, from_node: int, data: bytes) -> bytes:
    from Crypto.Util import Counter
    nonce = (packet_id & 0xFFFFFFFFFFFFFFFF).to_bytes(8,"little") + (from_node & 0xFFFFFFFF).to_bytes(4,"little") + b"\x00\x00\x00\x00"
    ctr = Counter.new(128, initial_value=int.from_bytes(nonce, "big"))
    return AES.new(key, AES.MODE_CTR, counter=ctr).encrypt(data)

class MeshtasticMqtt:
    def __init__(self, on_text: Callable[[str, int, str], None] | None = None):
        self.on_text = on_text; self._client = None; self._running = False
        self._node_num = self._parse_node_id(config.LORA_NODE_ID)
        self._refresh_channel_key()
    def _refresh_channel_key(self):
        self._key = _expand_psk(config.LORA_CHANNEL_KEY)
        self._channel_hash = (_xor_hash(config.LORA_CHANNEL.encode()) ^ _xor_hash(self._key)) & 0xFF
    @staticmethod
    def _parse_node_id(nid: str) -> int:
        try: return int(nid.lstrip("!"), 16)
        except Exception: return 0xb90f0fde
    @staticmethod
    def _set_packet_from(packet, node_num: int):
        if hasattr(packet, "from_"): packet.from_ = node_num
        else: setattr(packet, "from", node_num)
    @staticmethod
    def _get_packet_from(packet) -> int:
        if hasattr(packet, "from_"): return packet.from_
        return getattr(packet, "from")
    def start(self):
        if not (config.USE_MQTT and _HAS_MQTT): return
        self._node_num = self._parse_node_id(config.LORA_NODE_ID); self._refresh_channel_key()
        self._client = mqtt.Client(client_id=f"jpeq-tx-{random.randint(1000,9999)}", callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self._client.username_pw_set(config.LORA_MQTT_USER, config.LORA_MQTT_PASS)
        if config.LORA_MQTT_TLS: self._client.tls_set()
        self._client.on_connect = self._on_connect; self._client.on_message = self._on_message
        try:
            self._client.connect(config.LORA_MQTT_BROKER, config.LORA_MQTT_PORT, 60)
            self._client.loop_start()
            log.info("MQTT connecting to %s:%d", config.LORA_MQTT_BROKER, config.LORA_MQTT_PORT)
        except Exception as e: log.error("MQTT connect failed: %s", e)
    def stop(self):
        self._running = False
        if self._client:
            try: self._client.loop_stop(); self._client.disconnect()
            except Exception: pass
    def _on_connect(self, client, userdata, flags, rc, *args):
        if rc == 0:
            self._running = True
            topic = config.MQTT_TOPIC_RX + "+/+/" + config.LORA_CHANNEL + "/#"
            try: client.subscribe(topic); log.info("MQTT subscribed: %s", topic)
            except Exception as e: log.warning("MQTT subscribe failed: %s", e)
        else:
            self._running = False
            log.warning("MQTT connect rc=%s", rc)
    def _on_message(self, client, userdata, msg):
        if not self.on_text or not _HAS_PROTO or not _HAS_CRYPTO: return
        try:
            envelope = mqtt_pb2.ServiceEnvelope(); envelope.ParseFromString(msg.payload)
            packet = envelope.packet; sender = self._get_packet_from(packet); sender_id = "!" + format(sender, "08x")
            if packet.HasField("encrypted"):
                plaintext = _aes_ctr(self._key, packet.id, sender, packet.encrypted)
                data = mesh_pb2.Data()
                try: data.ParseFromString(plaintext)
                except Exception: return
            elif packet.HasField("decoded"): data = packet.decoded
            else: return
            if data.portnum == portnums_pb2.PortNum.TEXT_MESSAGE_APP:
                if sender_id == config.LORA_NODE_ID: return
                text = data.payload.decode("utf-8", errors="replace")
                self.on_text(text, config.CHANNEL_INDEX, sender_id)
        except Exception as e: log.debug("MQTT recv parse error: %s", e)
    def send_text(self, text: str, channel_index: int | None = None, destination: int | None = None) -> int | None:
        if not self._running or not self._client: log.warning("MQTT not running, drop: %s", text[:50]); return None
        if not _HAS_PROTO or not _HAS_CRYPTO: return None
        try:
            data = mesh_pb2.Data(); data.portnum = portnums_pb2.PortNum.TEXT_MESSAGE_APP; data.payload = text.encode("utf-8")
            data_bytes = data.SerializeToString(); packet_id = random.getrandbits(32)
            ciphertext = _aes_ctr(self._key, packet_id, self._node_num, data_bytes)
            packet = mesh_pb2.MeshPacket(); self._set_packet_from(packet, self._node_num)
            packet.to = destination if destination is not None else 0xFFFFFFFF
            packet.channel = self._channel_hash; packet.id = packet_id; packet.want_ack = False
            packet.hop_limit = 3; packet.via_mqtt = True; packet.hop_start = 3; packet.encrypted = ciphertext
            envelope = mqtt_pb2.ServiceEnvelope(); envelope.packet.CopyFrom(packet)
            envelope.channel_id = config.LORA_CHANNEL; envelope.gateway_id = config.LORA_NODE_ID
            topic = f"{config.MQTT_TOPIC_TX}2/e/{config.LORA_CHANNEL}/{config.LORA_NODE_ID}"
            info = self._client.publish(topic, envelope.SerializeToString(), qos=0)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                log.error("MQTT publish failed: rc=%s", info.rc)
                return None
            return packet_id
        except Exception as e: log.error("MQTT send failed: %s", e); return None

# ================================================================
# TX System
# ================================================================
from typing import NamedTuple


class WeatherChange(NamedTuple):
    kinds: dict[str, str]
    issued_times: dict[str, str]
    old_kinds: dict[str, str]

class PrioritySendItem:
    def __init__(self, priority: int, kind: str, text: str, target: str | None = None):
        self.priority = priority
        self.kind = kind
        self.text = text
        self.target = target


class UnifiedSendQueue:
    def __init__(self, max_size: int = 2200):
        self._heap = []
        self._counter = 0
        self._lock = threading.Lock()
        self._max_size = max_size

    def put(self, item: PrioritySendItem):
        with self._lock:
            heapq.heappush(self._heap, (item.priority, self._counter, item))
            self._counter += 1
            if len(self._heap) > self._max_size:
                # 優先度の低い（priorityが最大の）要素を探して削除する
                max_priority_idx = 0
                max_priority = -1
                for idx, (priority, _, _) in enumerate(self._heap):
                    if priority > max_priority:
                        max_priority = priority
                        max_priority_idx = idx
                self._heap.pop(max_priority_idx)
                heapq.heapify(self._heap)

    def get(self) -> PrioritySendItem:
        with self._lock:
            if not self._heap:
                raise queue.Empty
            _, _, item = heapq.heappop(self._heap)
            return item

    def qsize(self) -> int:
        with self._lock:
            return len(self._heap)

    def empty(self) -> bool:
        with self._lock:
            return len(self._heap) == 0

    def count_by_kind(self) -> dict[str, int]:
        with self._lock:
            counts: dict[str, int] = {}
            for _, _, item in self._heap:
                counts[item.kind] = counts.get(item.kind, 0) + 1
            return counts

class TxSystem:
    _log_initialized = False
    _STARTUP_SUPPRESS_SEC = 120
    # これらの定数は Config 側へ統合済みのため削除する。

    # 警報・特別警報・危険警報 → 注意報コード（ダウングレード判定用）
    _WARNING_TO_ADVISORY: dict[str, str] = {
        "03": "10", "43": "10", "33": "10",
        "05": "15", "35": "15",
        "02": "13", "32": "13",
        "06": "12", "36": "12",
        "07": "16", "37": "16",
        "08": "19", "48": "19", "38": "19",
        "09": "29", "49": "29", "39": "29",
        "04": "18",
    }

    # 下位警報 → 上位警報（格上げ検出用）
    _WARNING_TO_UPGRADE: dict[str, list[str]] = {
        "03": ["43", "33"],
        "43": ["33"],
        "05": ["35"],
        "02": ["32"],
        "06": ["36"],
        "07": ["37"],
        "08": ["48", "38"],
        "48": ["38"],
        "09": ["49", "39"],
        "49": ["39"],
    }

    # 上位警報 → 下位警報（警報間の降格検出用）
    _WARNING_TO_LOWER_WARNING: dict[str, list[str]] = {
        "33": ["43", "03"],
        "43": ["03"],
        "35": ["05"],
        "32": ["02"],
        "36": ["06"],
        "37": ["07"],
        "38": ["48", "08"],
        "48": ["08"],
        "39": ["49", "09"],
        "49": ["09"],
    }

    def __init__(self, log_callback: Callable[[str, str], None] | None = None):
        self.log_callback = log_callback
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = {"eew_lock": False, "last_eq_send": 0}
        self.p2pquake_connected = False
        self._p2pquake_last_received = 0.0
        self._last_test_send = 0
        self._last_test_send_time = 0
        self._scheduled_sent = set()
        self._memorial_sent_today: set = set()
        self._discord_eq_lines = []
        self._send_queue = UnifiedSendQueue(max_size=config.MAX_SEND_QUEUE_SIZE)
        self._loopback_skip_flag = False
        self._recent_sent_hashes: dict[str, float] = {}
        self._final_guard_hashes: dict[str, float] = {}
        self._dedup_lock = threading.Lock()
        self._ws_error_count: dict[str, int] = {}
        self.mqtt: MeshtasticMqtt | None = None
        self.lora_iface = None
        # 先にLoRa初期化を行い、成功時のみノードIDを取得する
        if config.USE_LORA and not config.USE_DUMMY:
            self._init_lora()

        # MQTT初期化はノードID取得後に行う
        if config.USE_MQTT:
            if config.LORA_NODE_ID:
                self.mqtt = MeshtasticMqtt()
            else:
                self._ui_log("ERROR", "MQTT initialization skipped: node ID is not available")

        self._weather_state: dict[str, dict[str, Any]] = {}
        self._processed_weather_ids: dict[str, float] = {}
        self._weather_lifted: dict[str, list[tuple[str, str, str, bool, str]]] = {}
        self._startup_in_progress = True
        self._last_warning_send_time = 0.0
        self._last_sent_kinds_snapshot: dict[str, frozenset] = {}
        self._last_typhoon_send = 0.0
        self._typhoon_last_hash: dict[str, str] = {}
        self._typhoon_state: dict[str, dict[str, Any]] = {}
        self._typhoon_state_path = Path(__file__).resolve().parent / "data" / "typhoon_state.json"
        self._typhoon_state_lock = threading.Lock()
        self._typhoon_history: list[dict[str, Any]] = []
        self._typhoon_last_hash: dict[str, str] = {}
        self._load_typhoon_state()
        self._weather_lock = Lock()

        # 河川氾濫警報の状態管理
        self._river_flood_state: dict[str, dict[str, Any]] = {}
        self._river_flood_lifted: dict[str, list[tuple[str, str, str, bool, str]]] = {}
        self._river_flood_last_sent_snapshot: dict[str, frozenset] = {}
        self._river_flood_processed_ids: dict[str, float] = {}
        self._river_flood_state_path = Path(__file__).resolve().parent / "data" / "river_flood_state.json"
        self._river_flood_lock = threading.Lock()
        self._river_flood_history: list[dict[str, Any]] = []
        self._river_flood_initial_pass_done = False
        self._load_river_flood_state()

        # 火山警報の状態管理
        self._volcano_state: dict[str, dict[str, Any]] = {}
        self._volcano_processed_ids: dict[str, float] = {}
        self._volcano_lifted: dict[str, list[tuple[str, str, str, bool, str, int]]] = {}
        self._volcano_last_sent_snapshot: dict[str, frozenset] = {}
        self._volcano_state_path = Path(__file__).resolve().parent / "data" / "volcano_state.json"
        self._volcano_lock = threading.Lock()
        self._volcano_history: list[dict[str, Any]] = []
        self._load_volcano_state()

        # 各フィードの接続成功ログを初回のみ出力するためのフラグ
        self._feed_info_logged = {
            "r8": False,
            "river": False,
            "volcano": False,
        }

        # 各フィードの最終取得成功時刻と連続失敗回数
        self._feed_last_success: dict[str, float] = {
            "r8": 0.0, "river": 0.0, "volcano": 0.0,
            "typhoon": 0.0, "megaquake": 0.0, "jalert": 0.0,
        }
        self._feed_fail_count: dict[str, int] = {
            "r8": 0, "river": 0, "volcano": 0,
            "typhoon": 0, "megaquake": 0, "jalert": 0,
        }

        # Monitor 子タブ用 URL 履歴
        self.earthquake_urls: deque = deque(maxlen=100)
        self.typhoon_urls: deque = deque(maxlen=100)

        self._jalert_queue = queue.Queue()
        self._jalert_send_lock = threading.Lock()
        self._jalert_sent_hashes: dict[str, float] = {}
        self._tx_counters: dict[str, int] = {
            "eew": 0, "eq": 0, "tsunami": 0, "volcano": 0,
            "weather": 0, "river_flood": 0, "typhoon": 0,
            "jalert": 0, "memorial": 0, "megaquake": 0,
            "test": 0, "other": 0,
        }
        self._tx_counters_lock = threading.Lock()
        self._log_handlers: list[logging.Handler] = []
        self._discord_queue: queue.Queue = queue.Queue()
        self._discord_thread = threading.Thread(
            target=self._discord_worker,
            daemon=True,
            name="discord_worker",
        )
        self._discord_thread.start()
        self._init_weather_state()
        self._r8_class10_to_office = None
        self._init_weather_state_from_r8()

    def _init_weather_state(self):
        for code in MONITORED_AREAS:
            self._weather_state[code] = {
                "kinds": {},
                "unsent": False,
                "updated_at": 0,
                "issued_times": {},
                "issued_at": {},
                "last_seen": {}
            }

    def _init_weather_state_from_r8(self):
        self._r8_class10_to_office = self._build_r8_class10_office_map()
        self._ui_log("INFO", "R8 office mapping built, fetching initial state...")
        self._update_weather_state_from_r8()
        self._ui_log("INFO", "Initial R8 weather state loaded")

    def _fetch_and_parse_xml(self, url: str, retries: int = 2, timeout: tuple = (5, 15)):
        r = request_with_retry(
            "GET",
            url,
            retries=retries,
            backoff=1.0,
            timeout=timeout,
        )
        if r is None or r.status_code != 200:
            status = r.status_code if r is not None else "None"
            raise RuntimeError(f"status={status}")
        return ET.fromstring(r.content)

    def _find_local_first(self, root, local_name):
        for elem in root.iter():
            tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            if tag == local_name:
                return elem
        return None

    def _findall_local(self, root, local_name):
        result = []
        for elem in root.iter():
            tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            if tag == local_name:
                result.append(elem)
        return result

    def _parse_report_datetime(self, detail_root) -> tuple[str, str] | None:
        head = self._find_local_first(detail_root, 'Head')
        if head is None:
            return None
        rdt = self._find_local_first(head, 'ReportDateTime')
        if rdt is None or not rdt.text:
            return None
        m = re.search(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})', rdt.text)
        if m:
            date_str = f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
            time_str = f"{m.group(4)}:{m.group(5)}"
            return (date_str, time_str)
        return None

    def _is_warning_kind(self, kind_name: str) -> bool:
        if not kind_name:
            return False
        return "警報" in kind_name and "注意報" not in kind_name

    def _is_warning_stale(self, issued_at_ts: float | None, max_age_hours: int = None) -> bool:
        if max_age_hours is None:
            max_age_hours = config.WARNING_STALE_HOURS
        if issued_at_ts is None:
            return False
        try:
            age_seconds = time.time() - issued_at_ts
            return age_seconds >= max_age_hours * 3600
        except Exception:
            return False

    def _translate_ja_to_en(self, text: str) -> str:
        try:
            r = request_with_retry(
                "POST",
                config.JALERT_GAS_URL,
                retries=2,
                backoff=1.0,
                timeout=(5, 10),
                json={"text": text, "sourceLanguage": "ja", "targetLanguages": ["en"]},
            )
            if r is not None and r.status_code == 200:
                result = r.json()
                return result[0] if result else text
        except Exception as e:
            self._ui_log(
                "WARN",
                f"J-ALERT translation error: url={config.JALERT_GAS_URL} error={e}",
            )
        return text

    def _filter_warning_only(self, kinds_dict: dict[str, str]) -> dict[str, str]:
        filtered = {}
        for code, name in kinds_dict.items():
            if not self._is_warning_kind(name):
                continue
            filtered[code] = name
        return filtered

    def _fetch_jalert_raw(self) -> str | None:
        url = config.JALERT_YAHOO_URL

        headers = {
            "Cache-Control": "no-cache",
            "Host": config.JALERT_YAHOO_HOST,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
        }

        try:
            r = request_with_retry(
                "GET",
                url,
                retries=config.JALERT_FETCH_RETRIES,
                backoff=config.JALERT_FETCH_BACKOFF,
                timeout=config.JALERT_FETCH_TIMEOUT,
                headers=headers,
                verify=True,
            )
            if r is None:
                self._ui_log("WARN", "J-ALERT fetch completely failed")
                return None
            r.encoding = 'utf-8'
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                p = soup.select_one("p.jalertInfo-item")
                if p:
                    html_str = str(p).replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
                    return BeautifulSoup(html_str, "html.parser").get_text("\n").strip()
                else:
                    self._ui_log("WARN", "J-ALERT target element not found")
                    return None
            else:
                self._ui_log("WARN", f"J-ALERT fetch failed: status={r.status_code}")
                return None
        except Exception as e:
            self._ui_log("WARN", f"J-ALERT fetch error: {e}")
            return None

    def _fetch_jalert_raw_all(self) -> list[str]:
        url = config.JALERT_YAHOO_URL

        headers = {
            "Cache-Control": "no-cache",
            "Host": config.JALERT_YAHOO_HOST,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
        }

        try:
            r = request_with_retry(
                "GET",
                url,
                retries=config.JALERT_FETCH_RETRIES,
                backoff=config.JALERT_FETCH_BACKOFF,
                timeout=config.JALERT_FETCH_TIMEOUT,
                headers=headers,
                verify=True,
            )
            if r is None:
                self._ui_log("WARN", "J-ALERT fetch completely failed")
                self._feed_fail_count["jalert"] = self._feed_fail_count.get("jalert", 0) + 1
                return []
            r.encoding = 'utf-8'
            if r.status_code != 200:
                self._ui_log("WARN", f"J-ALERT fetch failed: status={r.status_code}")
                self._feed_fail_count["jalert"] = self._feed_fail_count.get("jalert", 0) + 1
                return []
            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.select("p.jalertInfo-item")
            if not items:
                self._ui_log("WARN", "J-ALERT target element not found")
                self._feed_fail_count["jalert"] = self._feed_fail_count.get("jalert", 0) + 1
                return []
            self._feed_last_success["jalert"] = time.time()
            self._feed_fail_count["jalert"] = 0
            results = []
            for p in items:
                html_str = str(p).replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
                text = BeautifulSoup(html_str, "html.parser").get_text("\n").strip()
                if text:
                    results.append(text)
            return results
        except Exception as e:
            self._ui_log("WARN", f"J-ALERT fetch error: {e}")
            self._feed_fail_count["jalert"] = self._feed_fail_count.get("jalert", 0) + 1
            return []

    def _fetch_jalert_test_from_url(self, url: str) -> str | None:
        try:
            r = request_with_retry(
                "GET",
                url,
                retries=config.JALERT_FETCH_RETRIES,
                backoff=config.JALERT_FETCH_BACKOFF,
                timeout=config.JALERT_FETCH_TIMEOUT,
                headers={"Cache-Control": "no-cache"},
            )
            if r is None:
                return None
            r.encoding = 'utf-8'
            soup = BeautifulSoup(r.text, "html.parser")
            div = soup.select_one("#urgency .mb20p")
            if not div:
                return None
            html_str = str(div).replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
            text = BeautifulSoup(html_str, "html.parser").get_text("\n").strip()
            return text
        except Exception as e:
            self._ui_log("ERROR", f"J-ALERT test URL fetch error: {e}")
            return None

    def _parse_jalert_text(self, raw_text: str) -> dict[str, str] | None:
        if "72時間以内に発表されている情報はありません" in raw_text:
            return None
        raw_text = re.sub(r'\n\s*\n', '\n', raw_text.strip())
        raw_text = re.sub(r'[ \t]+', ' ', raw_text)
        time_match = re.search(r'【発表時間】\s*\n(.*?)\n(.*?)\n', raw_text, re.DOTALL)
        if not time_match:
            return None
        time_str = time_match.group(1).strip()
        source_str = time_match.group(2).strip()
        body_match = re.search(r'【内容】\s*\n(.*?)\n\s*【対象地域】', raw_text, re.DOTALL)
        if not body_match:
            return None
        body_str = body_match.group(1).strip()
        area_match = re.search(r'【対象地域】\s*\n(.*?)$', raw_text, re.DOTALL)
        if not area_match:
            return None
        area_str = area_match.group(1).strip()
        return {"time": time_str, "source": source_str, "body": body_str, "area": area_str}

    def _build_jalert_messages(self, parsed: dict[str, str]) -> list[str]:
        body_en = self._translate_ja_to_en(parsed["body"])
        first_dot = body_en.find('.')
        if first_dot != -1:
            body_en = body_en[:first_dot].upper() + body_en[first_dot:]
        else:
            body_en = body_en.upper()
        if first_dot != -1:
            first_sentence = body_en[:first_dot + 1]
            rest_of_body = body_en[first_dot + 1:].strip()
        else:
            first_sentence = body_en
            rest_of_body = ""
        area_en = self._translate_ja_to_en(parsed["area"])
        area_en = area_en.replace("Prefecture", "").strip()
        areas = [a.strip().upper() for a in area_en.split("\n") if a.strip()]
        area_line = " | ".join(areas)
        source_en = self._translate_ja_to_en(parsed["source"]).strip()
        time_jp = parsed["time"]
        time_match = re.search(r'(\d{4})年(\d{2})月(\d{2})日 (\d{2})時(\d{2})分', time_jp)
        if time_match:
            time_fmt = f"{time_match.group(1)}/{time_match.group(2)}/{time_match.group(3)} {time_match.group(4)}:{time_match.group(5)}"
        else:
            time_fmt = self._translate_ja_to_en(time_jp).strip()
        source_line = f"{source_en} {time_fmt}"
        lines = [f"<<J-ALERT>> {first_sentence}  {first_sentence}"]
        if rest_of_body:
            lines.append(f"<<J-ALERT>> {rest_of_body}")
        lines.append(f"<<J-ALERT>> {first_sentence}")
        lines.append(f"<<J-ALERT>> {area_line}")
        lines.append(f"<<J-ALERT>> {source_line}")
        return lines

    def _check_and_enqueue_jalert(self):
        raw_list = self._fetch_jalert_raw_all()
        if not raw_list:
            return
        enqueued_any = False
        now_ts = time.time()
        JALERT_HASH_TTL = 24 * 3600
        for raw in raw_list:
            raw_hash = hashlib.sha256(raw.encode()).hexdigest()
            with self._jalert_send_lock:
                # 期限切れのハッシュを削除
                expired = [h for h, ts in self._jalert_sent_hashes.items() if now_ts - ts > JALERT_HASH_TTL]
                for h in expired:
                    self._jalert_sent_hashes.pop(h, None)
                if raw_hash in self._jalert_sent_hashes:
                    continue
            if "72時間以内に発表されている情報はありません" in raw:
                continue
            parsed = self._parse_jalert_text(raw)
            if not parsed:
                continue
            messages = self._build_jalert_messages(parsed)
            for msg in messages:
                if config.ENABLE_JALERT:
                    self._send_queue.put(PrioritySendItem(0, "jalert", msg))
                else:
                    self._ui_log("INFO", f"J-ALERT send skipped (disabled): {msg[:60]}")
            with self._jalert_send_lock:
                self._jalert_sent_hashes[raw_hash] = now_ts
            if config.ENABLE_JALERT:
                enqueued_any = True
        if enqueued_any:
            self._ui_log("INFO", "J-ALERT package enqueued")

    # ===================================
    # R8 Weather Update
    # ===================================
    def _update_weather_state_from_r8(self):
        if not hasattr(self, '_r8_class10_to_office') or self._r8_class10_to_office is None:
            self._r8_class10_to_office = self._build_r8_class10_office_map()

        current_full, failed_offices = self._get_all_warnings_r8(self._r8_class10_to_office)

        if current_full is None:
            self._feed_fail_count["r8"] = self._feed_fail_count.get("r8", 0) + 1
            return

        self._feed_last_success["r8"] = time.time()
        self._feed_fail_count["r8"] = 0

        if not self._feed_info_logged.get("r8", False):
            self._ui_log("INFO", "R8 weather data fetched successfully")
            self._feed_info_logged["r8"] = True

        with self._weather_lock:
            for code in MONITORED_AREAS:
                prev_kinds = self._weather_state[code].get("kinds", {})
                new_kinds = {}
                if code in current_full:
                    for w in current_full[code]["warnings"]:
                        new_kinds[w["code"]] = w["name_jp"]
                issued_times = self._weather_state[code].get("issued_times", {})
                issued_at = self._weather_state[code].get("issued_at", {})
                last_seen = self._weather_state[code].get("last_seen", {})
                for kcode in new_kinds:
                    if kcode not in issued_times:
                        issued_times[kcode] = datetime.now().strftime("%H:%M")
                    if kcode not in issued_at:
                        # 初めて出現した警報コードは発令時刻を記録する
                        issued_at[kcode] = time.time()
                    # 継続して存在する場合は最終確認時刻のみ更新する
                    last_seen[kcode] = time.time()
                self._weather_state[code]["issued_at"] = issued_at
                self._weather_state[code]["last_seen"] = last_seen
                if code in self._weather_lifted:
                    self._weather_lifted[code] = [
                        item for item in self._weather_lifted[code] if item[0] not in new_kinds
                    ]
                    if not self._weather_lifted[code]:
                        del self._weather_lifted[code]
                lifted_kinds = {k: v for k, v in prev_kinds.items() if k not in new_kinds}
                if lifted_kinds and failed_offices == 0:
                    for kcode, kname in lifted_kinds.items():
                        it = issued_times.pop(kcode, "")
                        issued_at.pop(kcode, None)
                        last_seen.pop(kcode, None)
                        if not self._is_warning_kind(kname):
                            continue
                        upgrade_codes = self._WARNING_TO_UPGRADE.get(kcode, [])
                        if any(uc in new_kinds for uc in upgrade_codes):
                            continue
                        is_dg = False
                        new_kind_name = None
                        if new_kinds.get(kcode):
                            is_dg = True
                        else:
                            adv_code = self._WARNING_TO_ADVISORY.get(kcode)
                            if adv_code and adv_code in new_kinds:
                                is_dg = True
                                new_kind_name = new_kinds[adv_code]
                            else:
                                lower_codes = self._WARNING_TO_LOWER_WARNING.get(kcode, [])
                                for lc in lower_codes:
                                    if lc in new_kinds:
                                        is_dg = True
                                        new_kind_name = new_kinds[lc]
                                        break
                        self._weather_lifted.setdefault(code, []).append(
                            (kcode, kname, it, is_dg, code, new_kind_name)
                        )
                self._weather_state[code]["kinds"] = new_kinds
                self._weather_state[code]["issued_times"] = issued_times
                if prev_kinds != new_kinds:
                    self._weather_state[code]["unsent"] = True
                    self._weather_state[code]["updated_at"] = int(time.time())

        self._send_all_unsent()
        self._send_lifted_messages_if_ready()

    def _build_r8_class10_office_map(self):
        return {
            "011000": "011000", "012010": "012000", "012020": "012000",
            "013010": "013000", "013020": "013000", "013030": "013000",
            "014010": "014100", "014020": "014100", "014030": "014030",
            "015010": "015000", "015020": "015000",
            "016010": "016000", "016020": "016000", "016030": "016000",
            "017010": "017000", "017020": "017000",
            "020010": "020000", "020020": "020000", "020030": "020000",
            "030010": "030000", "030020": "030000", "030030": "030000",
            "040010": "040000", "040020": "040000",
            "050010": "050000", "050020": "050000",
            "060010": "060000", "060020": "060000", "060030": "060000", "060040": "060000",
            "070010": "070000", "070020": "070000", "070030": "070000",
            "080010": "080000", "080020": "080000",
            "090010": "090000", "090020": "090000",
            "100010": "100000", "100020": "100000",
            "110010": "110000", "110020": "110000", "110030": "110000",
            "120010": "120000", "120020": "120000", "120030": "120000",
            "130010": "130000", "130020": "130000", "130030": "130000", "130040": "130000",
            "140010": "140000", "140020": "140000",
            "150010": "150000", "150020": "150000", "150030": "150000", "150040": "150000",
            "160010": "160000", "160020": "160000",
            "170010": "170000", "170020": "170000",
            "180010": "180000", "180020": "180000",
            "190010": "190000", "190020": "190000",
            "200010": "200000", "200020": "200000", "200030": "200000",
            "210010": "210000", "210020": "210000",
            "220010": "220000", "220020": "220000", "220030": "220000", "220040": "220000",
            "230010": "230000", "230020": "230000",
            "240010": "240000", "240020": "240000",
            "250010": "250000", "250020": "250000",
            "260010": "260000", "260020": "260000",
            "270000": "270000",
            "280010": "280000", "280020": "280000",
            "290010": "290000", "290020": "290000",
            "300010": "300000", "300020": "300000",
            "310010": "310000", "310020": "310000",
            "320010": "320000", "320020": "320000", "320030": "320000",
            "330010": "330000", "330020": "330000",
            "340010": "340000", "340020": "340000",
            "350010": "350000", "350020": "350000", "350030": "350000", "350040": "350000",
            "360010": "360000", "360020": "360000",
            "370000": "370000",
            "380010": "380000", "380020": "380000", "380030": "380000",
            "390010": "390000", "390020": "390000", "390030": "390000",
            "400010": "400000", "400020": "400000", "400030": "400000", "400040": "400000",
            "410010": "410000", "410020": "410000",
            "420010": "420000", "420020": "420000", "420030": "420000", "420040": "420000",
            "430010": "430000", "430020": "430000", "430030": "430000", "430040": "430000",
            "440010": "440000", "440020": "440000", "440030": "440000", "440040": "440000",
            "450010": "450000", "450020": "450000", "450030": "450000", "450040": "450000",
            "460010": "460100", "460020": "460100", "460030": "460100", "460040": "460040",
            "471010": "471000", "471020": "471000", "471030": "471000",
            "472000": "472000",
            "473000": "473000",
            "474010": "474000", "474020": "474000",
        }

    def _get_all_warnings_r8(self, class10_to_office):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        office_codes = sorted(set(class10_to_office.values()))
        office_data = {}
        failed_offices = 0
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_office = {executor.submit(self._fetch_r8_json, office): office for office in office_codes}
            for future in as_completed(future_to_office):
                office = future_to_office[future]
                try:
                    data = future.result()
                    if data:
                        office_data[office] = data
                except Exception as e:
                    failed_offices += 1
                    self._ui_log("ERROR", f"R8 fetch error for office {office}: {e}")

        if failed_offices > 0:
            self._ui_log("WARN", f"R8 fetch completed with {failed_offices} failures")

        if not office_data:
            self._ui_log("ERROR", "R8 fetch completely failed: no office data available")
            return None, failed_offices

        results = {}
        for office_code, r8_data in office_data.items():
            sorted_entries = sorted(r8_data, key=lambda x: x.get("reportDatetime", ""))
            area_state = {}
            for entry in sorted_entries:
                for item in entry.get("warning", {}).get("class10Items", []):
                    area_code = item.get("areaCode", "")
                    if class10_to_office.get(area_code) != office_code:
                        continue
                    if area_code not in area_state:
                        area_state[area_code] = {}
                    for kind in item.get("kinds", []):
                        code = kind.get("code", "")
                        status = kind.get("status", "")
                        area_state[area_code][code] = {
                            "status": status,
                            "reportDatetime": entry.get("reportDatetime", "")
                        }
            for area_code, kinds in area_state.items():
                active = []
                for code, info in kinds.items():
                    if info["status"] in ("解除", "発表警報・注意報はなし"):
                        continue
                    name_jp = WARNING_CODE_TO_NAME.get(code)
                    if name_jp is None:
                        r8_level = self._get_warning_level_from_code(code)
                        if r8_level >= 5:
                            fallback_name = f"不明特別警報 (code:{code})"
                            en_translation = f"UNK EMERGENCY WARNING (code:{code})"
                        elif r8_level == 4:
                            fallback_name = f"不明危険警報 (code:{code})"
                            en_translation = f"UNK URGENT WARNING (code:{code})"
                        elif r8_level == 3:
                            fallback_name = f"不明警報 (code:{code})"
                            en_translation = f"UNK WARNING (code:{code})"
                        else:
                            fallback_name = f"不明注意報 (code:{code})"
                            en_translation = f"UNK ADVISORY (code:{code})"
                        with _dict_lock:
                            _dict["warning_types"][fallback_name] = en_translation
                        _WARNING_TYPE_EN[fallback_name] = en_translation
                        name_jp = fallback_name
                        level = r8_level
                        self._ui_log("WARN", f"Unknown warning code: {code} level={level} area={area_code}")
                        _register_unknown_warning_kind(fallback_name, area_code)
                        _register_unknown(name_jp)
                    else:
                        level = self._get_warning_level_from_code(code)
                    active.append({"code": code, "name_jp": name_jp, "level": level})
                if active:
                    results[area_code] = {"warnings": active}
        return results, failed_offices

    @staticmethod
    def _fetch_r8_json(office_code):
        R8_URL = "https://www.jma.go.jp/bosai/warning/data/r8/%s.json"
        r = request_with_retry(
            "GET",
            R8_URL % office_code,
            retries=3,
            backoff=1.0,
            timeout=(5, 15),
        )
        if r is None or r.status_code != 200:
            status = r.status_code if r is not None else "None"
            raise RuntimeError(f"R8 fetch failed for office {office_code}: status={status}")
        return r.json()

    @staticmethod
    def _get_warning_level_from_code(code):
        if code in ("32", "33", "35", "36", "37", "38", "39"):
            return 5
        if code in ("43", "48", "49"):
            return 4
        if code in ("02", "03", "04", "05", "06", "07", "08", "09"):
            return 3
        if code in ("10", "12", "13", "14", "15", "16", "17", "18", "19",
                    "20", "21", "22", "23", "24", "25", "26", "29"):
            return 2
        return 1

    # ===================================
    # Weather Warning: Send Pending Updates
    # ===================================
    def _send_all_unsent(self):
        if self._startup_in_progress:
            return
        if not hasattr(self, '_startup_complete_time') or (time.time() - self._startup_complete_time) < self._STARTUP_SUPPRESS_SEC:
            with self._weather_lock:
                for code in self._weather_state:
                    self._weather_state[code]["unsent"] = False
            return

        messages = []
        with self._weather_lock:
            for code, state in self._weather_state.items():
                if not state["unsent"]:
                    continue
                if code not in MONITORED_AREAS:
                    state["unsent"] = False
                    continue

                warning_only = self._filter_warning_only(state["kinds"])
                if warning_only:
                    issued = state.get("issued_times", {})
                    fresh = {}
                    for kcode, kname in warning_only.items():
                        issued_at_ts = state.get("issued_at", {}).get(kcode)
                        if not self._is_warning_stale(issued_at_ts):
                            fresh[kcode] = kname
                    warning_only = fresh
                if warning_only:
                    current_snapshot = frozenset(warning_only.items())
                    if current_snapshot == self._last_sent_kinds_snapshot.get(code):
                        state["unsent"] = False
                        continue
                    messages.append((code, warning_only, state))
                else:
                    self._last_sent_kinds_snapshot.pop(code, None)
                state["unsent"] = False

        if messages:
            messages.sort(key=lambda t: t[2]["updated_at"])
            changed: dict[str, WeatherChange] = {}
            for code, warning_only, state in messages:
                issued = state.get("issued_times", {})
                old_kinds = self._last_sent_kinds_snapshot.get(code, frozenset())
                old_kinds_dict = dict(old_kinds) if old_kinds else {}
                changed[code] = WeatherChange(warning_only, issued, old_kinds_dict)
                self._last_sent_kinds_snapshot[code] = frozenset(warning_only.items())
            self._send_weather_messages(changed)
            self._last_warning_send_time = time.time()

    def _send_weather_messages(self, changed: dict[str, WeatherChange]):
        if not config.ENABLE_WARNING:
            self._ui_log("INFO", "Weather warning send skipped (disabled)")
            return
        if self._startup_in_progress:
            return
        if not hasattr(self, '_startup_complete_time') or (time.time() - self._startup_complete_time) < self._STARTUP_SUPPRESS_SEC:
            return

        actionable = False
        for wc in changed.values():
            if wc.kinds:
                actionable = True
                break
        if not actionable:
            return

        now = datetime.now().strftime("%Y/%m/%d %H:%M")
        header_prefix = "[[WEATHER WARNING]]"

        local_prefs = _resolve_local_prefs() if config.USE_LOCAL_FILTER else []
        first_area = True
        for area_code, wc in changed.items():
            # ローカルフィルターが有効な場合、対象外エリアをスキップ
            if local_prefs:
                area_info = MONITORED_AREAS.get(area_code)
                if not area_info:
                    continue
                area_en = area_info.get("en", "")
                if not any(pref in area_en for pref in local_prefs):
                    continue

            warning_kinds = wc.kinds
            issued_times = wc.issued_times
            old_kinds_dict = wc.old_kinds
            state = self._weather_state.get(area_code)
            if state is None:
                continue

            if not warning_kinds:
                continue

            kinds_en = {}
            for code, jp_name in warning_kinds.items():
                en_name = translate_warning_kind_en(jp_name)
                if en_name:
                    kinds_en[code] = en_name
                else:
                    kinds_en[code] = jp_name
                    self._ui_log("WARN", f"Untranslatable warning: {jp_name} (area={area_code})")
                    _register_unknown_warning_kind(jp_name, area_code)
                    _register_unknown(jp_name)

            if not kinds_en:
                continue

            # 旧警報コード→旧レベルの対応を作成（英語名も併用）
            old_code_to_level: dict[str, int] = {}
            old_name_to_level: dict[str, int] = {}
            for old_code, old_jp in old_kinds_dict.items():
                lv = _warning_level(old_jp)
                old_code_to_level[old_code] = lv
                old_en = translate_warning_kind_en(old_jp)
                if old_en:
                    old_name_to_level[old_en] = lv

            seen_names: dict[str, int] = {}
            since_parts = []
            sorted_kinds = sorted(kinds_en.items(), key=lambda kv: (KIND_SORT_PRIORITY.get(kv[0], 99), kv[0]))
            for code, en_name in sorted_kinds:
                jp_name = warning_kinds.get(code, "")
                new_lv = _warning_level(jp_name)

                # 同一コードの旧レベルを優先
                old_lv = old_code_to_level.get(code, 0)
                if old_lv == 0:
                    # コードが異なる場合（注意報→警報など）は英語名から旧レベルを引く
                    old_lv = old_name_to_level.get(en_name, 0)

                report_time = issued_times.get(code, datetime.now().strftime("%H:%M"))

                if old_lv > 0 and old_lv != new_lv:
                    level_prefix = f"L:{old_lv}->{new_lv} "
                else:
                    level_prefix = f"L:{new_lv} "

                if en_name in seen_names:
                    since_parts.append(f"{level_prefix}{en_name}[{code}] since {report_time}")
                else:
                    since_parts.append(f"{level_prefix}{en_name} since {report_time}")
                seen_names[en_name] = seen_names.get(en_name, 0) + 1
            combined_names = "|".join(since_parts)


            is_upgrade = False
            upgrade_new_lv = 0
            for code, en_name in kinds_en.items():
                new_lv = _warning_level(warning_kinds.get(code, ""))
                # 同一コードの旧レベル、または英語名一致の旧レベルを確認
                candidate_old_lv = old_code_to_level.get(code, 0)
                if candidate_old_lv == 0:
                    candidate_old_lv = old_name_to_level.get(en_name, 0)
                if candidate_old_lv > 0 and candidate_old_lv < new_lv:
                    is_upgrade = True
                    upgrade_new_lv = new_lv
                    break


            has_emergency = any("特別警報" in jp for jp in warning_kinds.values())
            has_urgent    = any("危険警報" in jp for jp in warning_kinds.values())
            
            def _extract_base_en(en_name: str) -> str:
                return en_name.split(" (", 1)[0].strip()

            if has_emergency:
                max_level = 5
                level_label = "EMERGENCY"
            elif has_urgent:
                max_level = 4
                level_label = "URGENT"
            else:
                max_level = 3
                level_label = ""

            area_info = MONITORED_AREAS.get(area_code)
            if area_info is None:
                continue
            child_name = area_info["en"]
            url_code = area_info.get("url_code", area_code)
            jma_url = f"https://www.jma.go.jp/bosai/warning/#area_type=offices&area_code={url_code}"

            if is_upgrade:
                combined_names = "|".join(since_parts)
                if level_label:
                    full_tag = f"[WX] <U/G LEVEL {upgrade_new_lv} {level_label}> {child_name} ({combined_names})"
                else:
                    full_tag = f"[WX] <U/G LEVEL {upgrade_new_lv}> {child_name} ({combined_names})"
            else:
                is_no_change = (warning_kinds == old_kinds_dict)

                if level_label:
                    tag_prefix = "<N/C LEVEL" if is_no_change else "<LEVEL"
                    full_tag = f"[WX] {tag_prefix} {max_level} {level_label}> {child_name} ({combined_names})"
                else:
                    tag_prefix = "<N/C LEVEL" if is_no_change else "<LEVEL"
                    full_tag = f"[WX] {tag_prefix} {max_level}> {child_name} ({combined_names})"

            if first_area:
                header_msg = f"{header_prefix} {now}"
                self._send_queue.put(PrioritySendItem(5, "normal", header_msg))
                self._send_discord(header_msg)
                first_area = False

            if max_level >= 5:
                body_priority = 1
            elif max_level == 4:
                body_priority = 2
            else:
                body_priority = 3

            body_msg = f"{full_tag}"
            self._send_queue.put(PrioritySendItem(10 + body_priority, "normal", body_msg))
            self._send_discord(body_msg)

            url_msg = f"[WX] Warning Info URL {jma_url}"
            self._send_queue.put(PrioritySendItem(10 + body_priority, "normal", url_msg))
            self._send_discord(url_msg, skip_dedup=True)

    # ===================================
    # Weather Warning: LIFTED
    # ===================================
    def _validate_and_fix_state(self):
        with self._weather_lock:
            for code, state in self._weather_state.items():
                area_lifted_codes = {item[0] for item in self._weather_lifted.get(code, []) if len(item) >= 1}
                for kind_code in list(state["kinds"].keys()):
                    if kind_code in area_lifted_codes:
                        log.warning(f"Inconsistency detected: area={code} kind_code={kind_code} in both lifted and state. Removing from state.")
                        state["kinds"].pop(kind_code, None)
                        state["issued_times"].pop(kind_code, None)

    _LIFTED_SEND_DELAY_SEC = 30

    def _send_lifted_messages_if_ready(self):
        self._validate_and_fix_state()
        if not config.ENABLE_WARNING:
            self._ui_log("INFO", "Weather lifted send skipped (disabled)")
            return
        if self._startup_in_progress:
            return
        if not hasattr(self, '_startup_complete_time') or (time.time() - self._startup_complete_time) < self._STARTUP_SUPPRESS_SEC:
            return
        if time.time() - self._last_warning_send_time < self._LIFTED_SEND_DELAY_SEC:
            return
        with self._weather_lock:
            if not self._weather_lifted:
                return
            lifted = dict(self._weather_lifted)
            self._weather_lifted.clear()
        for code, lifted_list in lifted.items():
            area_info = MONITORED_AREAS.get(code)
            if area_info is None:
                continue
            child_name = area_info["en"]
            parts = []
            overall_tag = "LIFTED"
            for item in lifted_list:
                if len(item) == 6:
                    _, kind_name_jp, issued_time, is_dg, _, new_kind_name = item
                elif len(item) == 5:
                    _, kind_name_jp, issued_time, is_dg, _ = item
                    new_kind_name = None
                else:
                    continue

                en_name = translate_warning_kind_en(kind_name_jp)
                if en_name is None:
                    en_name = kind_name_jp

                old_lv = _warning_level(kind_name_jp)

                if is_dg:
                    if new_kind_name:
                        new_lv = _warning_level(new_kind_name)
                        if new_lv >= 3:
                            overall_tag = f"D/G LEVEL {new_lv}"
                        else:
                            overall_tag = "LIFTED D/G"
                        level_prefix = f"L:{old_lv}->{new_lv} "
                    else:
                        overall_tag = "LIFTED D/G"
                        level_prefix = f"L:{old_lv}->? "
                else:
                    level_prefix = f"L:{old_lv}->1 "

                if issued_time:
                    desc = f"{level_prefix}{en_name} since {issued_time}"
                else:
                    desc = f"{level_prefix}{en_name}"

                if is_dg or self._is_warning_kind(kind_name_jp):
                    parts.append(desc)

            if parts:
                combined = "|".join(parts)
                msg = f"[WX] <{overall_tag}> {child_name} ({combined})"
                self.send_text(msg)
                self._send_discord(msg)

                # 現在のエリアの最大レベルが3以上の場合のみURLを送信
                current_kinds = self._weather_state.get(code, {}).get("kinds", {})
                current_max_level = calc_area_level(current_kinds)
                if current_max_level >= 3:
                    url_code = area_info.get("url_code", code)
                    jma_url = f"https://www.jma.go.jp/bosai/warning/#area_type=offices&area_code={url_code}"
                    url_msg = f"[WX] Warning Info URL {jma_url}"
                    self.send_text(url_msg)
                    self._send_discord(url_msg, skip_dedup=True)

    # ===================================
    # River Flood Feed
    # ===================================
    def _load_river_flood_state(self):
        try:
            if not self._river_flood_state_path.exists():
                self._river_flood_history = getattr(self, "_river_flood_history", [])
                return
            with self._river_flood_state_path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
            with self._river_flood_lock:
                self._river_flood_state = raw.get("state", {})
                self._river_flood_processed_ids = raw.get("processed_ids", {})
                self._river_flood_history = raw.get("history", [])
            # 古い状態を除去（updated_atが2時間以上前のものは信頼しない）
            now_ts = time.time()
            stale_keys = [
                key for key, val in self._river_flood_state.items()
                if val.get("level", 1) >= 3 and (now_ts - val.get("updated_at", 0)) >= 7200
            ]
            with self._river_flood_lock:
                for key in stale_keys:
                    self._river_flood_state.pop(key, None)
            self._ui_log("INFO", f"Loaded river flood state: {len(self._river_flood_state)} active rivers, {len(self._river_flood_history)} history records (removed {len(stale_keys)} stale)")
        except Exception as e:
            self._ui_log("WARN", f"Failed to load river flood state: {e}")
            with self._river_flood_lock:
                self._river_flood_state = {}
                self._river_flood_processed_ids = {}
                self._river_flood_history = getattr(self, "_river_flood_history", [])

    def _save_river_flood_state(self):
        try:
            self._river_flood_state_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = Path(str(self._river_flood_state_path) + ".tmp")
            with self._river_flood_lock:
                # 履歴の上限適用
                if len(self._river_flood_history) > config.MAX_STATE_HISTORY_ITEMS:
                    del self._river_flood_history[:-config.MAX_STATE_HISTORY_ITEMS]
                data = {
                    "state": dict(self._river_flood_state),
                    "processed_ids": dict(self._river_flood_processed_ids),
                    "history": list(getattr(self, "_river_flood_history", [])),
                }
            with temp_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(temp_path, self._river_flood_state_path)
        except Exception as e:
            self._ui_log("WARN", f"Failed to save river flood state: {e}")

    def _load_volcano_state(self):
        try:
            if not self._volcano_state_path.exists():
                self._volcano_history = getattr(self, "_volcano_history", [])
                return
            with self._volcano_state_path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
            with self._volcano_lock:
                self._volcano_state = raw.get("state", {})
                self._volcano_processed_ids = raw.get("processed_ids", {})
                self._volcano_history = raw.get("history", [])
            self._ui_log("INFO", f"Loaded volcano state: {len(self._volcano_state)} active volcanoes, {len(self._volcano_history)} history records")
        except Exception as e:
            self._ui_log("WARN", f"Failed to load volcano state: {e}")
            with self._volcano_lock:
                self._volcano_state = {}
                self._volcano_processed_ids = {}
                self._volcano_history = getattr(self, "_volcano_history", [])

    def _save_volcano_state(self):
        try:
            self._volcano_state_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = Path(str(self._volcano_state_path) + ".tmp")
            with self._volcano_lock:
                # 履歴の上限適用
                if len(self._volcano_history) > config.MAX_STATE_HISTORY_ITEMS:
                    del self._volcano_history[:-config.MAX_STATE_HISTORY_ITEMS]
                data = {
                    "state": dict(self._volcano_state),
                    "processed_ids": dict(self._volcano_processed_ids),
                    "history": list(getattr(self, "_volcano_history", [])),
                }
            with temp_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(temp_path, self._volcano_state_path)
        except Exception as e:
            self._ui_log("WARN", f"Failed to save volcano state: {e}")

    def _check_volcano_feed(self):
        """eqvol.xml から VFVO50（噴火警報・予報）を検出し、火山警報を処理する"""
        try:
            r = request_with_retry(
                "GET",
                config.JMA_EQVOL_FEED_URL,
                retries=2,
                backoff=1.0,
                timeout=(5, 15),
            )
            if r is None or r.status_code != 200:
                if r is not None:
                    self._ui_log("ERROR", f"Volcano feed fetch failed: status={r.status_code}")
                self._feed_fail_count["volcano"] = self._feed_fail_count.get("volcano", 0) + 1
                return
            root = ET.fromstring(r.content)
            self._feed_last_success["volcano"] = time.time()
            self._feed_fail_count["volcano"] = 0
            if not self._feed_info_logged.get("volcano", False):
                self._ui_log("INFO", "Volcano feed fetched successfully")
                self._feed_info_logged["volcano"] = True
        except Exception as e:
            self._ui_log("ERROR", f"Volcano feed fetch error: {e}")
            self._feed_fail_count["volcano"] = self._feed_fail_count.get("volcano", 0) + 1
            return

        ATOM_NS = "http://www.w3.org/2005/Atom"
        for entry in root.findall(f"{{{ATOM_NS}}}entry"):
            eid_elem = entry.find(f"{{{ATOM_NS}}}id")
            eid = eid_elem.text if eid_elem is not None else ""
            if not eid:
                continue
            if "VFVO50" not in eid:
                continue

            if eid in self._volcano_processed_ids:
                continue

            link = entry.find(f"{{{ATOM_NS}}}link")
            href = link.get("href") if link is not None else ""
            if not href:
                continue

            try:
                detail_root = self._fetch_and_parse_xml(href)
            except Exception as e:
                self._ui_log("ERROR", f"Volcano: failed to fetch detail for {eid}: {e}")
                continue

            if self._handle_volcano_info(detail_root, eid):
                with self._volcano_lock:
                    self._volcano_processed_ids[eid] = time.time()
                    if len(self._volcano_processed_ids) > config.PROCESSED_WEATHER_IDS_MAX:
                        oldest_keys = sorted(self._volcano_processed_ids, key=lambda k: self._volcano_processed_ids[k])[:len(self._volcano_processed_ids) - config.PROCESSED_WEATHER_IDS_MAX]
                        for k in oldest_keys:
                            self._volcano_processed_ids.pop(k, None)
                self._save_volcano_state()

        self._send_volcano_all_unsent()
        self._send_volcano_lifted_messages_if_ready()

    def _handle_volcano_info(self, detail_root, eid: str) -> bool:

        # Head/Title を取得（例: "火山名  十勝岳  噴火警報（火口周辺）"）
        head_elem = self._find_local_first(detail_root, 'Head')
        title = ""
        if head_elem is not None:
            title_elem = self._find_local_first(head_elem, 'Title')
            if title_elem is not None and title_elem.text:
                title = title_elem.text.strip()

        # InfoType（取消など）を取得
        info_type_elem = self._find_local_first(detail_root, 'InfoType')
        info_type = info_type_elem.text if info_type_elem is not None else ""

        volcano_code = ""
        volcano_name_jp = ""
        condition = ""
        level = 0
        warning_type_jp = ""
        old_level_from_last_kind = 0

        # VolcanoInfo のうち、type が「噴火警報・予報（対象火山）」のものを探す
        target_volcano_info = None
        for volcano_info in detail_root.iter():
            tag = volcano_info.tag.split('}')[-1] if '}' in volcano_info.tag else volcano_info.tag
            if tag != "VolcanoInfo":
                continue
            if volcano_info.get("type", "") == "噴火警報・予報（対象火山）":
                target_volcano_info = volcano_info
                break
        if target_volcano_info is None:
            self._ui_log("WARN", f"Volcano: target VolcanoInfo not found ({eid})")
            return False

        # 対象火山のAreaからコードと名前を取得
        for area in target_volcano_info.iter():
            atag = area.tag.split('}')[-1] if '}' in area.tag else area.tag
            if atag != "Area":
                continue
            code_elem = self._find_local_first(area, 'Code')
            name_elem = self._find_local_first(area, 'Name')
            if code_elem is not None and code_elem.text:
                volcano_code = code_elem.text.strip()
            if name_elem is not None and name_elem.text:
                volcano_name_jp = name_elem.text.strip()
            break

        if not volcano_name_jp:
            self._ui_log("WARN", f"Volcano: could not parse volcano name ({eid})")
            return False

        # Kind を取得（Item 直下のみ、LastKind を除外）
        for item in target_volcano_info.iter():
            itag = item.tag.split('}')[-1] if '}' in item.tag else item.tag
            if itag != "Item":
                continue
            # Item 直下の Kind / LastKind を探す
            for child in item:
                ctag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                if ctag == "Kind":
                    condition_elem = self._find_local_first(child, 'Condition')
                    code_elem = self._find_local_first(child, 'Code')

                    if condition_elem is not None and condition_elem.text:
                        condition = condition_elem.text.strip()
                    if code_elem is not None and code_elem.text:
                        code = code_elem.text.strip()
                        level = _VOLCANO_CODE_TO_LEVEL.get(code, 0)
                elif ctag == "LastKind":
                    last_code_elem = self._find_local_first(child, 'Code')
                    if last_code_elem is not None and last_code_elem.text:
                        last_code = last_code_elem.text.strip()
                        last_level = _VOLCANO_CODE_TO_LEVEL.get(last_code, 0)
                        if last_level > 0:
                            old_level_from_last_kind = last_level
            break

        # 警報種別名は Head/Title から抽出（例: "火山名  十勝岳  噴火警報（火口周辺）"）
        if title:
            m = re.search(r'(噴火警報[（(][^）)]*[）)]|噴火予報|噴火速報|火山の状況に関する解説情報)', title)
            if m:
                warning_type_jp = m.group(1).strip()
            else:
                parts = title.split()
                if parts:
                    warning_type_jp = parts[-1].strip()

        # InfoType が取消の場合は解除扱い
        if info_type == "取消":
            condition = "解除"

        # 火山名をキーとして辞書から英語名・都道府県を取得
        volcano_entry = _dict.get("volcano_names", {}).get(volcano_name_jp)
        if isinstance(volcano_entry, dict):
            volcano_en = volcano_entry.get("en", volcano_name_jp) or volcano_name_jp
            pref_en_name = volcano_entry.get("pref", "")
            if volcano_code == "":
                volcano_code = str(volcano_entry.get("code", ""))
        else:
            # 未登録の火山は自動登録する
            volcano_en = volcano_name_jp
            pref_en_name = ""
            with _dict_lock:
                _dict["volcano_names"][volcano_name_jp] = {
                    "en": volcano_name_jp,
                    "pref": "",
                    "code": int(volcano_code) if volcano_code.isdigit() else 0,
                }
            self._ui_log("WARN", f"Unknown volcano registered: {volcano_name_jp}")

        dt = self._parse_report_datetime(detail_root)
        if dt:
            report_date, report_time = dt
        else:
            report_time = datetime.now().strftime("%H:%M")

        with self._volcano_lock:
            key = volcano_name_jp

            if condition == "解除":
                if key in self._volcano_state:
                    old_state = self._volcano_state.pop(key, {})
                    old_level = old_state.get("level", 1)
                    issued_time = old_state.get("issued_time", report_time)
                    self._volcano_lifted.setdefault(key, []).append(
                        (key, "Eruption Warning", issued_time, False, key, 1, old_level)
                    )
                    self._volcano_history.append({
                        "ts": datetime.now().isoformat(),
                        "name": key,
                        "old_level": old_level,
                        "new_level": 1,
                        "condition": condition,
                        "sent": False,
                    })
            elif condition == "引下げ":
                if key in self._volcano_state:
                    old_state = self._volcano_state[key]
                    old_level = old_state.get("level", 1)
                    issued_time = old_state.get("issued_time", report_time)
                    if level == 0:
                        self._ui_log("WARN", f"Volcano: could not parse new level for downgrade ({eid})")
                        return False
                    if level <= 1:
                        self._volcano_state.pop(key, None)
                        self._volcano_lifted.setdefault(key, []).append(
                            (key, "Eruption Warning", issued_time, False, key, 1, old_level)
                        )
                        self._volcano_history.append({
                            "ts": datetime.now().isoformat(),
                            "name": key,
                            "old_level": old_level,
                            "new_level": 1,
                            "condition": condition,
                            "sent": False,
                        })
                    else:
                        self._volcano_state[key] = {
                            "level": level,
                            "issued_time": issued_time,
                            "updated_at": int(time.time()),
                            "pref": pref_en_name,
                            "volcano_en": volcano_en,
                            "code": volcano_code,
                            "condition": condition,
                            "warning_type_jp": warning_type_jp,
                            "old_level": old_level_from_last_kind if old_level_from_last_kind > 0 else old_level,
                        }
                        self._volcano_lifted.setdefault(key, []).append(
                            (key, "Eruption Warning", issued_time, True, key, level, old_level)
                        )
                        self._volcano_history.append({
                            "ts": datetime.now().isoformat(),
                            "name": key,
                            "old_level": old_level,
                            "new_level": level,
                            "condition": condition,
                            "sent": level >= 2,
                        })
            else:
                prev_state = self._volcano_state.get(key)
                prev_level = prev_state.get("level", 0) if prev_state else 0
                old_warning_type_jp = prev_state.get("warning_type_jp", "") if prev_state else ""
                new_level = level if level > 0 else (prev_level or 1)
                if old_level_from_last_kind > 0:
                    resolved_old_level = old_level_from_last_kind
                elif prev_level > 0:
                    resolved_old_level = prev_level
                else:
                    resolved_old_level = 0
                self._volcano_state[key] = {
                    "level": new_level,
                    "issued_time": report_time if prev_level == 0 else (prev_state.get("issued_time", report_time) if condition == "継続" else report_time),
                    "updated_at": int(time.time()),
                    "pref": pref_en_name,
                    "volcano_en": volcano_en,
                    "code": volcano_code,
                    "condition": condition,
                    "warning_type_jp": warning_type_jp,
                    "old_warning_type_jp": old_warning_type_jp,
                    "old_level": resolved_old_level,
                }
                self._volcano_history.append({
                    "ts": datetime.now().isoformat(),
                    "name": key,
                    "old_level": prev_level,
                    "new_level": new_level,
                    "condition": condition,
                    "sent": new_level >= 2,
                })

        self._ui_log("INFO", f"Volcano info processed: {volcano_name_jp} condition={condition} level={level}")
        return True

    def _translate_volcano_warning_type(self, jp_name: str) -> str:
        if not jp_name:
            return jp_name
        normalized = unicodedata.normalize('NFKC', jp_name)
        volcano_types = _dict.get("volcano_warning_types", {})
        if isinstance(volcano_types, dict):
            for key, val in volcano_types.items():
                if unicodedata.normalize('NFKC', key) == normalized:
                    return val
        return jp_name

    def _volcano_level_label(self, level: int) -> str:
        return {
            1: "NORMAL",
            2: "DO NOT APPROACH CRATER",
            3: "DO NOT APPROACH VOLCANO",
            4: "PREPARE TO EVACUATE",
            5: "EVACUATE",
        }.get(level, f"LEVEL {level}")

    def _send_volcano_messages(self, messages):
        now_str = datetime.now().strftime("%Y/%m/%d %H:%M")
        for key, state, old_level in messages:
            new_level = state.get("level", 1)
            issued_time = state.get("issued_time", "")
            volcano_en = state.get("volcano_en", key)
            condition = state.get("condition", "")
            warning_type_jp = state.get("warning_type_jp", "噴火警報")

            # 警報種別の英語名を取得
            warning_type_en = self._translate_volcano_warning_type(warning_type_jp)

            # ヘッダー行を送信
            header_msg = f"[[VOLCANO WR]] {now_str}|{volcano_en}|Level {new_level}"
            self._send_with_discord(header_msg)

            if old_level == 0:
                tag = f"<LEVEL {new_level} {self._volcano_level_label(new_level)}>"
            elif condition == "引上げ":
                tag = f"<U/G LEVEL {new_level} {self._volcano_level_label(new_level)}>"
            elif condition == "引下げ":
                if new_level <= 2:
                    tag = "<LIFTED D/G>"
                else:
                    tag = f"<D/G LEVEL {new_level} {self._volcano_level_label(new_level)}>"
            elif condition == "継続":
                tag = f"<N/C LEVEL {new_level} {self._volcano_level_label(new_level)}>"
            elif condition == "切替":
                old_warning_type_jp = state.get("old_warning_type_jp", "")
                old_warning_type_en = self._translate_volcano_warning_type(old_warning_type_jp) if old_warning_type_jp else "Unknown"
                tag = f"<SWITCH {old_warning_type_en}->{warning_type_en}>"
            else:
                tag = f"<LEVEL {new_level} {self._volcano_level_label(new_level)}>"

            level_prefix = f"L:{old_level}->{new_level} " if old_level > 0 and old_level != new_level else f"L:{new_level} "
            body_msg = f"[VOLCANO] {tag} {volcano_en} ({level_prefix}{warning_type_en} since {issued_time})"
            url_msg = "[VOLCANO] Info URL https://www.jma.go.jp/bosai/volcano/"

            self._send_with_discord(body_msg)
            self._send_with_discord(url_msg, skip_dedup=True)

    def _send_volcano_all_unsent(self):
        if not config.ENABLE_VOLCANO:
            self._ui_log("INFO", "Volcano send skipped (disabled)")
            with self._volcano_lock:
                for key, state in list(self._volcano_state.items()):
                    if state.get("level", 1) < 2:
                        continue
                    current_snapshot = frozenset({
                        (key, state.get("level", 0), state.get("condition", ""), state.get("warning_type_jp", ""))
                    })
                    self._volcano_last_sent_snapshot[key] = current_snapshot
            return
        if self._startup_in_progress:
            return
        if not hasattr(self, '_startup_complete_time') or (time.time() - self._startup_complete_time) < self._STARTUP_SUPPRESS_SEC:
            return

        messages = []
        with self._volcano_lock:
            for key, state in list(self._volcano_state.items()):
                if state.get("level", 1) < 2:
                    continue
                current_snapshot = frozenset({
                    (key, state.get("level", 0), state.get("condition", ""), state.get("warning_type_jp", ""))
                })
                prev_snapshot = self._volcano_last_sent_snapshot.get(key)
                if current_snapshot == prev_snapshot:
                    continue
                old_level = state.get("old_level", 0)
                if old_level == 0 and prev_snapshot:
                    for item in prev_snapshot:
                        if item[0] == key:
                            old_level = item[1]
                            break
                messages.append((key, state, old_level))
                self._volcano_last_sent_snapshot[key] = current_snapshot

        if messages:
            self._send_volcano_messages(messages)

    def _send_volcano_lifted_messages_if_ready(self):
        if not config.ENABLE_VOLCANO:
            self._ui_log("INFO", "Volcano lifted send skipped (disabled)")
            with self._volcano_lock:
                self._volcano_lifted.clear()
            return
        if self._startup_in_progress:
            return
        if not hasattr(self, '_startup_complete_time') or (time.time() - self._startup_complete_time) < self._STARTUP_SUPPRESS_SEC:
            return
        if time.time() - self._last_warning_send_time < self._LIFTED_SEND_DELAY_SEC:
            return

        with self._volcano_lock:
            if not self._volcano_lifted:
                return
            lifted = dict(self._volcano_lifted)
            self._volcano_lifted.clear()

        for key, lifted_list in lifted.items():
            parts = []
            overall_tag = "LIFTED"
            for item in lifted_list:
                if len(item) == 6:
                    _, _, issued_time, _, _, new_level = item
                    old_lv = self._volcano_state.get(key, {}).get("level", 0)
                elif len(item) == 7:
                    _, _, issued_time, _, _, new_level, old_lv = item
                else:
                    continue
                if old_lv < 2:
                    continue
                if new_level <= 1:
                    overall_tag = "LIFTED"
                elif new_level == 2:
                    overall_tag = "LIFTED D/G"
                else:
                    overall_tag = f"D/G LEVEL {new_level}"

                level_prefix = f"L:{old_lv}->{new_level} " if old_lv != new_level else f"L:{new_level} "
                desc = f"{level_prefix}Eruption Warning since {issued_time}" if issued_time else f"{level_prefix}Eruption Warning"
                parts.append(desc)

            if parts:
                combined = "|".join(parts)
                volcano_entry = _dict.get("volcano_names", {}).get(key)
                if isinstance(volcano_entry, dict):
                    volcano_en = volcano_entry.get("en", key) or key
                else:
                    volcano_en = key
                msg = f"[VOLCANO] <{overall_tag}> {volcano_en} ({combined})"
                self.send_text(msg)
                self._send_discord(msg)

    def _check_river_flood_feed(self):
        """extra.xml から VXKOii（指定河川洪水予報）を検出し状態を更新する"""
        try:
            r = request_with_retry(
                "GET",
                config.JMA_FEED_URL,
                retries=2,
                backoff=1.0,
                timeout=(5, 15),
            )
            if r is None or r.status_code != 200:
                if r is not None:
                    self._ui_log(
                        "ERROR",
                        f"River flood feed fetch failed: status={r.status_code}",
                    )
                self._feed_fail_count["river"] = self._feed_fail_count.get("river", 0) + 1
                return
            root = ET.fromstring(r.content)
            self._feed_last_success["river"] = time.time()
            self._feed_fail_count["river"] = 0
            if not self._feed_info_logged.get("river", False):
                self._ui_log("INFO", "River flood feed fetched successfully")
                self._feed_info_logged["river"] = True
        except Exception as e:
            self._ui_log(
                "ERROR",
                f"River flood feed fetch error: url={config.JMA_FEED_URL} error={e}",
            )
            self._feed_fail_count["river"] = self._feed_fail_count.get("river", 0) + 1
            return

        ATOM_NS = "http://www.w3.org/2005/Atom"
        entries = root.findall(f"{{{ATOM_NS}}}entry")

        def _entry_sort_key(entry):
            updated_elem = entry.find(f"{{{ATOM_NS}}}updated")
            if updated_elem is not None and updated_elem.text:
                try:
                    return datetime.fromisoformat(updated_elem.text.replace("Z", "+00:00"))
                except Exception:
                    pass
            return datetime.now(timezone.utc) - timedelta(days=1)

        # 古いエントリから順に処理し、最新エントリの状態が最終状態となるようにする
        entries.sort(key=_entry_sort_key, reverse=False)

        is_initial_pass = not self._river_flood_initial_pass_done

        for entry in entries:
            eid_elem = entry.find(f"{{{ATOM_NS}}}id")
            eid = eid_elem.text if eid_elem is not None else ""
            if not eid or "VXKO" not in eid:
                continue
            if eid in self._river_flood_processed_ids:
                continue

            # 古いエントリは処理しない（24時間以上前のものはスキップ）
            updated_elem = entry.find(f"{{{ATOM_NS}}}updated")
            if updated_elem is not None and updated_elem.text:
                try:
                    entry_time = datetime.fromisoformat(updated_elem.text.replace("Z", "+00:00"))
                    age_sec = (datetime.now(timezone.utc) - entry_time).total_seconds()
                    if age_sec >= 86400:
                        continue
                except Exception:
                    pass  # 日時解析に失敗した場合は処理を続行

            link = entry.find(f"{{{ATOM_NS}}}link")
            href = link.get("href") if link is not None else ""
            if not href:
                continue

            try:
                detail_root = self._fetch_and_parse_xml(href)
            except Exception as e:
                self._ui_log("ERROR", f"River flood: failed to fetch detail for {eid}: {e}")
                continue

            if self._handle_river_flood_info(detail_root, eid):
                with self._river_flood_lock:
                    self._river_flood_processed_ids[eid] = time.time()
                    if len(self._river_flood_processed_ids) > config.PROCESSED_WEATHER_IDS_MAX:
                        oldest_keys = sorted(self._river_flood_processed_ids, key=lambda k: self._river_flood_processed_ids[k])[:len(self._river_flood_processed_ids) - config.PROCESSED_WEATHER_IDS_MAX]
                        for k in oldest_keys:
                            self._river_flood_processed_ids.pop(k, None)
                self._save_river_flood_state()

        if is_initial_pass:
            # 初回パス（起動直後の状態再構築）では一切メッセージを送信しない。
            # 再構築された最新 state にスナップショットを同期させ、以降の差分検出の基準とする。
            with self._river_flood_lock:
                self._river_flood_last_sent_snapshot.clear()
                for river_name, state in self._river_flood_state.items():
                    if state.get("level", 1) >= 3:
                        self._river_flood_last_sent_snapshot[river_name] = frozenset({(river_name, state["level"])})
                self._river_flood_lifted.clear()
            self._river_flood_initial_pass_done = True
            self._ui_log("INFO", f"River flood initial state reconstruction complete: {len(self._river_flood_state)} active rivers")
            return

        self._send_river_flood_all_unsent()
        self._send_river_flood_lifted_messages_if_ready()

    def _handle_river_flood_info(self, detail_root, eid: str) -> bool:
        head_elem = self._find_local_first(detail_root, 'Head')
        title = ""
        if head_elem is not None:
            title_elem = self._find_local_first(head_elem, 'Title')
            if title_elem is not None and title_elem.text:
                title = title_elem.text.strip()

        # 動作確認済みのため、河川氾濫XML全体のDEBUG出力は削除する。

        # 河川予報区域名・コード・レベルを取得
        river_name_jp = ""
        river_code = ""
        pref_jp_list = []
        pref_code_list = []
        level = 0
        is_lifted = False
        is_downgrade = False

        for info in detail_root.iter():
            tag = info.tag.split('}')[-1] if '}' in info.tag else info.tag
            if tag != "Information":
                continue
            info_type = info.get("type", "")
            if info_type == "指定河川洪水予報（予報区域）":
                for area in info.iter():
                    atag = area.tag.split('}')[-1] if '}' in area.tag else area.tag
                    if atag == "Name" and area.text:
                        river_name_jp = area.text.strip()
                    elif atag == "Code" and area.text:
                        river_code = area.text.strip()
                for kind in info.iter():
                    ktag = kind.tag.split('}')[-1] if '}' in kind.tag else kind.tag
                    if ktag == "Code" and kind.text and kind.text.strip() in _RIVER_FLOOD_CODE_TO_LEVEL:
                        code = kind.text.strip()
                        level = _RIVER_FLOOD_CODE_TO_LEVEL[code]
                        if code == "10":
                            is_lifted = True
                        elif code == "22":
                            is_downgrade = True
            elif info_type == "指定河川洪水予報（府県予報区等）":
                for area in info.iter():
                    atag = area.tag.split('}')[-1] if '}' in area.tag else area.tag
                    if atag == "Name" and area.text:
                        pref_jp_list.append(area.text.strip())
                    elif atag == "Code" and area.text:
                        pref_code_list.append(area.text.strip())

        if not river_name_jp or level == 0:
            self._ui_log("WARN", f"River flood: could not parse title '{title}' ({eid})")
            return False

        info_type_elem = self._find_local_first(detail_root, 'InfoType')
        info_type = info_type_elem.text if info_type_elem is not None else ""
        if info_type == "取消":
            is_lifted = True

        dt = self._parse_report_datetime(detail_root)
        if dt:
            report_date, report_time = dt
        else:
            report_time = datetime.now().strftime("%H:%M")

        # 都道府県英語名を生成
        if pref_jp_list:
            pref_en_list = [pref_en(p) for p in pref_jp_list if p in _dict.get("pref", {})]
            pref_en_name = ", ".join(pref_en_list) if pref_en_list else ", ".join(pref_jp_list)
        else:
            pref_en_name = ""

        # XMLから取得した河川名で river_names を検索
        matched_key = _find_river_entry(river_name_jp)
        if matched_key is None:
            # 未登録の河川は自動登録
            matched_key = _register_river_name(river_name_jp, pref_en_name)

        river_entry = _dict.get("river_names", {}).get(matched_key or river_name_jp)
        if isinstance(river_entry, dict):
            river_en = river_entry.get("en", river_name_jp) or river_name_jp
            pref_en_name = river_entry.get("pref", pref_en_name)
        else:
            river_en = river_entry if river_entry else river_name_jp

        state_key = river_name_jp
        with self._river_flood_lock:
            if is_lifted:
                # 内部状態に存在しない場合、履歴から最新のレベルを取得する
                if state_key in self._river_flood_state:
                    old_state = self._river_flood_state.pop(state_key, {})
                    old_level = old_state.get("level", level)
                    issued_time = old_state.get("issued_time", report_time)
                else:
                    # 履歴を逆順に走査し、同一河川の最新エントリの new_level を old_level として採用
                    hist_old_level = 0
                    for h in reversed(getattr(self, "_river_flood_history", [])):
                        if h.get("name") == river_name_jp:
                            hist_old_level = h.get("new_level", 0)
                            break
                    if hist_old_level > 0:
                        old_level = hist_old_level
                        issued_time = report_time
                        self._ui_log("INFO", f"River flood lift: state not found for {state_key}, using history old_level={hist_old_level}")
                    else:
                        old_level = level
                        issued_time = report_time
                        self._ui_log("WARN", f"River flood lift: state not found for {state_key} (code={river_code}, level={level}); using level as fallback")

                if is_downgrade:
                    # 降格時はレベル2の状態を保持する
                    self._river_flood_state[state_key] = {
                        "level": 2,
                        "issued_time": report_time,
                        "updated_at": int(time.time()),
                        "pref": pref_en_name,
                        "river_en": river_en,
                        "river_name_jp": river_name_jp,
                        "river_code": river_code,
                    }
                    self._river_flood_lifted.setdefault(river_name_jp, []).append(
                        (river_name_jp, river_code, old_level, 2, issued_time, is_downgrade)
                    )
                    self._river_flood_history.append({
                        "ts": datetime.now().isoformat(),
                        "name": river_name_jp,
                        "code": river_code,
                        "old_level": old_level,
                        "new_level": 2,
                        "condition": "引下げ",
                        "sent": False,
                    })
                else:
                    # 完全解除時のみ、必要なら解除通知を送る
                    if not (level == 2 and old_level == 2):
                        # 状態から削除し、解除メッセージ用の情報を専用リストに保存
                        self._river_flood_state.pop(state_key, None)
                        self._river_flood_lifted.setdefault(river_name_jp, []).append(
                            (river_name_jp, river_code, old_level, 1, issued_time, is_downgrade)
                        )
                    self._river_flood_history.append({
                        "ts": datetime.now().isoformat(),
                        "name": river_name_jp,
                        "code": river_code,
                        "old_level": old_level,
                        "new_level": 1,
                        "condition": "解除",
                        "sent": False,
                    })
            else:
                prev_state = self._river_flood_state.get(state_key)
                prev_level = prev_state.get("level", 0) if prev_state else 0
                if prev_level != level:
                    self._river_flood_state[state_key] = {
                        "level": level,
                        "issued_time": report_time,
                        "updated_at": int(time.time()),
                        "pref": pref_en_name,
                        "river_en": river_en,
                        "river_name_jp": river_name_jp,
                        "river_code": river_code,
                    }
                    self._river_flood_history.append({
                        "ts": datetime.now().isoformat(),
                        "name": river_name_jp,
                        "code": river_code,
                        "old_level": prev_level,
                        "new_level": level,
                        "condition": "更新" if prev_level > 0 else "発表",
                        "sent": level >= 3,
                    })
        return True

    def _send_river_flood_all_unsent(self):
        if not config.ENABLE_RIVER_FLOOD:
            self._ui_log("INFO", "River flood send skipped (disabled)")
            with self._river_flood_lock:
                for state_key, state in list(self._river_flood_state.items()):
                    if state.get("level", 1) < 3:
                        continue
                    current_snapshot = frozenset({(state_key, state["level"])})
                    self._river_flood_last_sent_snapshot[state_key] = current_snapshot
            return
        if self._startup_in_progress:
            return
        if not hasattr(self, '_startup_complete_time') or (time.time() - self._startup_complete_time) < self._STARTUP_SUPPRESS_SEC:
            return

        changed_rivers = []
        now_ts = time.time()
        with self._river_flood_lock:
            for state_key, state in list(self._river_flood_state.items()):
                if state.get("level", 1) < 3:
                    continue
                # 古い状態は送信しない
                if (now_ts - state.get("updated_at", 0)) >= 7200:
                    continue
                current_snapshot = frozenset({(state_key, state["level"])})
                prev_snapshot = self._river_flood_last_sent_snapshot.get(state_key)
                if current_snapshot == prev_snapshot:
                    continue

                old_level = 0
                if prev_snapshot:
                    for item in prev_snapshot:
                        if item[0] == state_key:
                            old_level = item[1]
                            break

                changed_rivers.append((state_key, state, old_level))
                self._river_flood_last_sent_snapshot[state_key] = current_snapshot

        if changed_rivers:
            self._send_river_flood_messages(changed_rivers)

    def _send_river_flood_messages(self, changed_rivers):
        local_prefs = _resolve_local_prefs() if config.USE_LOCAL_FILTER else []
        for state_key, state, old_level in changed_rivers:
            new_level = state.get("level", 3)
            issued_time = state.get("issued_time", "")
            pref_name = state.get("pref", "")
            river_en = state.get("river_en", state.get("river_name_jp", state_key))

            if local_prefs and pref_name and not any(pref in pref_name for pref in local_prefs):
                continue

            location = f"{river_en}, {pref_name}" if pref_name else river_en
            level_label = _RIVER_FLOOD_LEVEL_LABEL.get(new_level, "")

            if old_level == 0:
                tag = f"<LEVEL {new_level}"
                if level_label:
                    tag += f" {level_label}"
                tag += ">"
            elif old_level < new_level:
                tag = f"<U/G LEVEL {new_level}"
                if level_label:
                    tag += f" {level_label}"
                tag += ">"
            elif old_level > new_level:
                tag = f"<D/G LEVEL {new_level}"
                if level_label:
                    tag += f" {level_label}"
                tag += ">"
            else:
                tag = f"<N/C LEVEL {new_level}"
                if level_label:
                    tag += f" {level_label}"
                tag += ">"

            if old_level > 0 and old_level != new_level:
                level_prefix = f"L:{old_level}->{new_level} "
            else:
                level_prefix = f"L:{new_level} "

            body_msg = f"[WX] {tag} {location} ({level_prefix}Flood since {issued_time})"
            url_msg = "[WX] Flood Info URL https://www.jma.go.jp/bosai/flood/"

            self._send_with_discord(body_msg)
            self._send_with_discord(url_msg, skip_dedup=True)

    def _send_river_flood_lifted_messages_if_ready(self):
        if not config.ENABLE_RIVER_FLOOD:
            self._ui_log("INFO", "River flood lifted send skipped (disabled)")
            with self._river_flood_lock:
                self._river_flood_lifted.clear()
            return
        if self._startup_in_progress:
            return
        if not hasattr(self, '_startup_complete_time') or (time.time() - self._startup_complete_time) < self._STARTUP_SUPPRESS_SEC:
            return
        if time.time() - self._last_warning_send_time < self._LIFTED_SEND_DELAY_SEC:
            return

        local_prefs = _resolve_local_prefs() if config.USE_LOCAL_FILTER else []
        with self._river_flood_lock:
            if not self._river_flood_lifted:
                return
            lifted = dict(self._river_flood_lifted)
            self._river_flood_lifted.clear()

        for river_name_jp, lifted_list in lifted.items():
            if not lifted_list:
                continue

            # より新しい情報を優先（issued_time が最大のものを選択）
            latest_item = max(lifted_list, key=lambda x: x[4])  # issued_timeはインデックス4

            river_name_jp, river_code, old_lv, new_level, issued_time, is_downgrade = latest_item

            # レベル2以下の解除・降格は送信しない
            if old_lv < 3:
                continue
            en_name = "Flood"

            if new_level == 1:
                overall_tag = "LIFTED"
            elif new_level == 2:
                overall_tag = "LIFTED D/G"
            else:
                overall_tag = f"D/G LEVEL {new_level}"

            if old_lv > 0 and old_lv != new_level:
                level_prefix = f"L:{old_lv}->{new_level} "
            else:
                level_prefix = f"L:{new_level} "

            desc = f"{level_prefix}{en_name} since {issued_time}" if issued_time else f"{level_prefix}{en_name}"

            river_entry = _dict.get("river_names", {}).get(river_name_jp)
            if isinstance(river_entry, dict):
                river_en = river_entry.get("en", river_name_jp) or river_name_jp
                pref_name = river_entry.get("pref", "")
            else:
                river_en = river_name_jp
                pref_name = ""
            if local_prefs and pref_name and not any(pref in pref_name for pref in local_prefs):
                continue
            location = f"{river_en}, {pref_name}" if pref_name else river_en
            msg = f"[WX] <{overall_tag}> {location} ({desc})"
            self.send_text(msg)
            self._send_discord(msg)

    # ===================================
    # Megaquake Feed Check
    # ===================================
    def _check_megaquake_feed(self):
        """extra.xml から VXSE61/62 を検出し、_handle_megaquake_info を呼び出す"""
        try:
            r = request_with_retry(
                "GET",
                config.JMA_FEED_URL,
                retries=2,
                backoff=1.0,
                timeout=(5, 15),
            )
            if r is None or r.status_code != 200:
                if r is not None:
                    self._ui_log(
                        "ERROR",
                        f"Megaquake feed fetch failed: status={r.status_code}",
                    )
                self._feed_fail_count["megaquake"] = self._feed_fail_count.get("megaquake", 0) + 1
                return
            root = ET.fromstring(r.content)
            self._feed_last_success["megaquake"] = time.time()
            self._feed_fail_count["megaquake"] = 0
        except Exception as e:
            self._ui_log(
                "ERROR",
                f"Megaquake feed fetch error: url={config.JMA_FEED_URL} error={e}",
            )
            self._feed_fail_count["megaquake"] = self._feed_fail_count.get("megaquake", 0) + 1
            return

        ATOM_NS = "http://www.w3.org/2005/Atom"
        for entry in root.findall(f"{{{ATOM_NS}}}entry"):
            eid_elem = entry.find(f"{{{ATOM_NS}}}id")
            eid = eid_elem.text if eid_elem is not None else ""
            if not eid:
                continue
            if "VXSE61" not in eid and "VXSE62" not in eid:
                continue

            if eid in self._processed_weather_ids:
                continue

            link = entry.find(f"{{{ATOM_NS}}}link")
            href = link.get("href") if link is not None else ""
            if not href:
                continue

            try:
                detail_root = self._fetch_and_parse_xml(href)
            except Exception as e:
                self._ui_log("ERROR", f"Megaquake: failed to fetch detail for {eid}: {e}")
                continue

            self._handle_megaquake_info(detail_root, eid)

    # ================================================================
    # Typhoon (THI removed)
    # ================================================================
    def _fetch_typhoon_info(self) -> list[dict[str, Any]] | None:
        try:
            r = request_with_retry(
                "GET",
                config.JMA_TYPHOON_FEED_URL,
                retries=2,
                backoff=1.0,
                timeout=(5, 20),
                headers={"Cache-Control": "no-cache"},
            )
            if r is None or r.status_code != 200:
                if r is not None:
                    self._ui_log(
                        "ERROR",
                        f"Typhoon feed fetch failed: status={r.status_code}",
                    )
                self._feed_fail_count["typhoon"] = self._feed_fail_count.get("typhoon", 0) + 1
                return None
            root = ET.fromstring(r.content)
            self._feed_last_success["typhoon"] = time.time()
            self._feed_fail_count["typhoon"] = 0
        except Exception as e:
            self._ui_log(
                "ERROR",
                f"Typhoon feed fetch error: url={config.JMA_TYPHOON_FEED_URL} error={e}",
            )
            self._feed_fail_count["typhoon"] = self._feed_fail_count.get("typhoon", 0) + 1
            return None

        ATOM_NS = "http://www.w3.org/2005/Atom"
        latest_per_number: dict[str, tuple[datetime, dict[str, Any]]] = {}
        vptw60_count = 0
        total_entries = 0
        STALE_THRESHOLD_SEC = 24 * 3600
        now_utc = datetime.now(timezone.utc)

        for entry in root.findall(f"{{{ATOM_NS}}}entry"):
            total_entries += 1
            eid = entry.find(f"{{{ATOM_NS}}}id")
            if eid is None or eid.text is None:
                continue
            if not re.search(r"VPTW6[0-5]", eid.text):
                continue

            vptw60_count += 1
            updated_el = entry.find(f"{{{ATOM_NS}}}updated")
            if updated_el is not None and updated_el.text:
                try:
                    entry_time = datetime.fromisoformat(updated_el.text.replace("Z", "+00:00"))
                except ValueError:
                    entry_time = now_utc
            else:
                entry_time = now_utc

            age_sec = (now_utc - entry_time).total_seconds()
            if age_sec > STALE_THRESHOLD_SEC:
                continue

            link = entry.find(f"{{{ATOM_NS}}}link")
            href = link.get("href") if link is not None else ""
            if not href:
                self._ui_log("WARN", f"Typhoon: VPTW60 #{vptw60_count} has no link, skipping")
                continue

            try:
                detail_root = self._fetch_and_parse_xml(href)
            except Exception as e:
                self._ui_log("ERROR", f"Typhoon: failed to fetch/parse VPTW60 detail: {e}")
                continue

            body = None
            for elem in detail_root.iter():
                if elem.tag.split('}')[-1] == "Body":
                    body = elem
                    break
            if body is None:
                continue

            met_infos = None
            for child in body:
                if child.tag.split('}')[-1] == "MeteorologicalInfos":
                    met_infos = child
                    break
            if met_infos is None:
                continue

            def find_child(parent, local_name):
                for c in parent:
                    if c.tag.split('}')[-1] == local_name:
                        return c
                return None

            def find_all_children(parent, local_name):
                return [c for c in parent if c.tag.split('}')[-1] == local_name]

            for met_info in met_infos:
                if met_info.tag.split('}')[-1] != "MeteorologicalInfo":
                    continue
                dt = find_child(met_info, "DateTime")
                if dt is None or dt.get("type") != "実況":
                    continue

                current = met_info

                def get_property(prop_type_name):
                    for item in find_all_children(current, "Item"):
                        for kind in find_all_children(item, "Kind"):
                            for prop in find_all_children(kind, "Property"):
                                type_elem = find_child(prop, "Type")
                                if type_elem is not None and type_elem.text and type_elem.text.strip() == prop_type_name:
                                    return prop
                    return None

                name = ""; number = ""
                prop = get_property("呼称")
                if prop is not None:
                    tnp = find_child(prop, "TyphoonNamePart")
                    if tnp is not None:
                        name = (find_child(tnp, "Name").text or "").strip() if find_child(tnp, "Name") is not None else ""
                        number = (find_child(tnp, "Number").text or "").strip() if find_child(tnp, "Number") is not None else ""
                if not name or not number:
                    continue

                if number in latest_per_number:
                    prev_time, _ = latest_per_number[number]
                    if entry_time <= prev_time:
                        continue

                typhoon_class = ""
                prop = get_property("階級")
                if prop is not None:
                    cp = find_child(prop, "ClassPart")
                    if cp is not None:
                        tc = find_child(cp, "TyphoonClass")
                        if tc is not None and tc.text:
                            typhoon_class = tc.text.strip()

                pressure = ""; coord = ""; direction_jp = ""; speed_kmh = ""
                prop = get_property("中心")
                if prop is not None:
                    cp = find_child(prop, "CenterPart")
                    if cp is not None:
                        for child in cp:
                            local = child.tag.split('}')[-1]
                            text = (child.text or "").strip()
                            if local == "Coordinate" and child.get("type") == "中心位置（度）":
                                coord = text
                            elif local == "Direction":
                                direction_jp = text
                            elif local == "Speed" and child.get("unit") == "km/h":
                                speed_kmh = text
                            elif local == "Pressure":
                                pressure = text

                max_wind = ""; gust = ""
                storm_axes = []
                gale_axes = []
                prop = get_property("風")
                if prop is not None:
                    wp = find_child(prop, "WindPart")
                    if wp is not None:
                        for ws in find_all_children(wp, "WindSpeed"):
                            if ws.get("condition") == "中心付近" and ws.get("unit") == "m/s" and ws.get("type") == "最大風速":
                                max_wind = (ws.text or "").strip()
                            elif ws.get("unit") == "m/s" and ws.get("type") == "最大瞬間風速":
                                gust = (ws.text or "").strip()

                    known_area_tags = (
                        "WarningAreaPart", "WindAreaPart",
                        "StormAreaPart", "GaleAreaPart",
                        "WarningArea", "WindArea",
                        "StormArea", "GaleArea"
                    )
                    for warning_area in prop:
                        tag_name = warning_area.tag.split('}')[-1]
                        if not (tag_name in known_area_tags or "Area" in tag_name or
                                any(kw in tag_name for kw in ("Warning", "Storm", "Gale"))):
                            continue

                        area_type = warning_area.get("type", "")
                        is_storm = ("暴風" in area_type or "storm" in area_type.lower() or
                                    "Storm" in tag_name or "暴風" in tag_name)
                        is_gale = ("強風" in area_type or "gale" in area_type.lower() or
                                   "Gale" in tag_name or "強風" in tag_name)

                        if not is_storm and not is_gale:
                            continue

                        circles = []
                        for descendant in warning_area.iter():
                            if descendant.tag.split('}')[-1] == "Circle":
                                circles.append(descendant)
                        for circle in circles:
                            storm_found = []
                            gale_found = []

                            axes = find_child(circle, "Axes")
                            if axes is not None:
                                axis_list = find_all_children(axes, "Axis")
                                for axis in axis_list:
                                    direction_jp_axis = ""
                                    radius_km_axis = 0
                                    for child in axis:
                                        local = child.tag.split('}')[-1]
                                        if local == "Direction":
                                            dir_text = (child.text or "").strip()
                                            if not dir_text:
                                                dir_text = child.get("description", "").strip()
                                            direction_jp_axis = dir_text
                                        elif local == "Radius" and child.get("unit") == "km":
                                            try:
                                                radius_km_axis = int(float((child.text or "").strip()))
                                            except (ValueError, TypeError):
                                                pass
                                    if direction_jp_axis and radius_km_axis > 0:
                                        if is_storm:
                                            storm_found.append((direction_jp_axis, radius_km_axis))
                                        if is_gale:
                                            gale_found.append((direction_jp_axis, radius_km_axis))
                            else:
                                radius_elem = None
                                for descendant in circle.iter():
                                    if descendant.tag.split('}')[-1] == "Radius":
                                        radius_elem = descendant
                                        break
                                if radius_elem is not None:
                                    try:
                                        radius_val = int(float((radius_elem.text or "").strip()))
                                        if radius_val > 0:
                                            if is_storm:
                                                storm_found.append(("All", radius_val))
                                            if is_gale:
                                                gale_found.append(("All", radius_val))
                                    except (ValueError, TypeError):
                                        pass

                            storm_axes.extend(storm_found)
                            gale_axes.extend(gale_found)

                coord_fmt = ""
                lat_val = None
                lon_val = None
                if coord:
                    m = re.match(r'([+-]\d+\.?\d*)([+-]\d+\.?\d*)/?', coord)
                    if m:
                        lat = float(m.group(1)); lon = float(m.group(2))
                        lat_str = f"{abs(lat):.1f}{'N' if lat >= 0 else 'S'}"
                        lon_str = f"{abs(lon):.1f}{'E' if lon >= 0 else 'W'}"
                        coord_fmt = f"{lat_str},{lon_str}"
                        lat_val, lon_val = lat, lon

                direction_en = _DIRECTION_EN.get(direction_jp, direction_jp)

                typhoon_tag = f"[TY No.{number}]" if number else "[TY]"
                name_with_class = f"{typhoon_tag} {name}"

                parts = []
                parts.append(name_with_class)
                if pressure:
                    parts.append(f"Central Pressure:{pressure}hPa")
                wind_str = ""
                if max_wind:
                    wind_str += f"Max Sustained Winds:{max_wind}m/s"
                    if gust:
                        wind_str += f"(Gust {gust}m/s)"
                    parts.append(wind_str)
                if storm_axes:
                    storm_parts = []
                    for d, r in storm_axes:
                        if d == "全域":
                            storm_parts.append(f"All {r}km")
                        else:
                            storm_parts.append(f"{_DIRECTION_EN.get(d, d)} {r}km")
                    parts.append(f"Storm:{', '.join(storm_parts)}")
                if gale_axes:
                    gale_parts = []
                    for d, r in gale_axes:
                        if d == "全域":
                            gale_parts.append(f"All {r}km")
                        else:
                            gale_parts.append(f"{_DIRECTION_EN.get(d, d)} {r}km")
                    parts.append(f"Gale:{', '.join(gale_parts)}")
                if coord_fmt:
                    coord_with_space = coord_fmt.replace(",", ", ")
                    parts.append(f"Position:({coord_with_space})")
                if direction_en and speed_kmh:
                    parts.append(f"Movement:{direction_en} {speed_kmh}km/h")
                summary = "|".join(parts)

                url = ""
                if lat_val is not None and lon_val is not None and config.ENABLE_TYPHOON_MAP_URL:
                    url_params = f"lat={lat_val:.1f}&lng={lon_val:.1f}"
                    if storm_axes:
                        min_storm = min(r for _, r in storm_axes)
                        url_params += f"&storm={min_storm}"
                    if gale_axes:
                        min_gale = min(r for _, r in gale_axes)
                        url_params += f"&gale={min_gale}"
                    if direction_en:
                        url_params += f"&dir={direction_en}"
                    if speed_kmh:
                        url_params += f"&spd={speed_kmh}"
                    url_params += "&type=ty"
                    url = f"https://jpeqtx.github.io/map/map.html?{url_params}"

                latest_per_number[number] = (entry_time, {
                    "name": name,
                    "number": number,
                    "typhoon_class": typhoon_class,
                    "summary": summary,
                    "url": url,
                })

        typhoons = []
        JST = timezone(timedelta(hours=9))
        for number, (entry_time, data) in latest_per_number.items():
            age_sec = (now_utc - entry_time).total_seconds()
            if age_sec <= STALE_THRESHOLD_SEC:
                birth_str = entry_time.astimezone(JST).strftime("%Y/%m/%d %H:%M")
                data["birth"] = birth_str
                data["summary"] = data["summary"] + f"|Update:{birth_str}"
                typhoons.append(data)
            else:
                self._ui_log("DEBUG", f"Typhoon: {number} latest report is {age_sec:.0f}s old, excluding")

        if typhoons:
            typhoons.sort(key=lambda t: int(t["number"]) if t["number"] else 9999)
        return typhoons if typhoons else None

    def _send_typhoon_info_if_needed(self, force: bool = False):
        typhoons = self._fetch_typhoon_info()
        if not typhoons:
            return

        new_or_changed_typhoons = []
        for typhoon in typhoons:
            typhoon_key = typhoon["number"] or typhoon["name"]
            current_hash = hashlib.sha256(typhoon["summary"].encode()).hexdigest()
            if self._typhoon_last_hash.get(typhoon_key) != current_hash:
                self._typhoon_last_hash[typhoon_key] = current_hash
                new_or_changed_typhoons.append(typhoon)

        lifted_messages = self._check_typhoon_changes(typhoons)

        if not config.ENABLE_TYPHOON_INFO:
            self._ui_log("INFO", "Typhoon send skipped (disabled)")
            self._last_typhoon_send = time.time()
            return

        if force:
            active_typhoons_for_send = [t for t in typhoons if "台風" in t.get("typhoon_class", "")]
        else:
            active_typhoons_for_send = new_or_changed_typhoons

        if lifted_messages or active_typhoons_for_send:
            now_str = datetime.now().strftime("%Y/%m/%d %H:%M")
            header_msg = f"[[TYPHOON INFO]] {now_str} (Q.3h)"
            self._send_with_discord(header_msg)

            status_by_number: dict[str, list[str]] = {}
            for msg in lifted_messages:
                m = re.search(r"\[TY No\.(\d+)\]", msg)
                if m:
                    status_by_number.setdefault(m.group(1), []).append(msg)
                else:
                    self._send_with_discord(msg)

            active_typhoons = [t for t in active_typhoons_for_send if "台風" in t.get("typhoon_class", "")]
            for typhoon in active_typhoons:
                number = typhoon.get("number", "")
                if number and number in status_by_number:
                    for status_msg in status_by_number[number]:
                        self._send_with_discord(status_msg)

                chunks = split_message(typhoon["summary"])
                for chunk in chunks:
                    self._send_with_discord(chunk)
                if typhoon["url"] and config.ENABLE_TYPHOON_MAP_URL:
                    self.typhoon_urls.append(typhoon["url"])
                    typhoon_tag = f"[TY No.{typhoon.get('number', '')}]" if typhoon.get('number') else "[TY]"
                    map_msg = f"{typhoon_tag} Position Map URL {typhoon['url']}"
                    self._send_with_discord(map_msg)

        self._last_typhoon_send = time.time()

    def _load_typhoon_state(self):
        self._typhoon_history = []
        self._typhoon_last_hash = {}
        try:
            if self._typhoon_state_path.exists():
                with self._typhoon_state_path.open("r", encoding="utf-8") as f:
                    raw = json.load(f)
                self._typhoon_state = {}
                now = datetime.now()
                for num, info in raw.items():
                    if num == "_history":
                        self._typhoon_history = info
                        continue
                    if num == "_last_hash":
                        self._typhoon_last_hash = info
                        continue
                    clazz = info.get("class", "")
                    if "台風" in clazz:
                        self._typhoon_state[num] = info
                        continue
                    if "dissipated_at" in info:
                        try:
                            diss_dt = datetime.strptime(info["dissipated_at"], "%Y/%m/%d %H:%M")
                            if (now - diss_dt).total_seconds() < 24 * 3600:
                                self._typhoon_state[num] = info
                        except Exception:
                            log.warning("Failed to parse dissipated_at for typhoon %s", num)
                        continue
                    if "熱帯低気圧" in clazz or "温帯低気圧" in clazz:
                        last_seen_str = info.get("last_seen", "")
                        try:
                            last_seen_dt = datetime.strptime(last_seen_str, "%Y/%m/%d %H:%M")
                            if (now - last_seen_dt).total_seconds() < 24 * 3600:
                                self._typhoon_state[num] = info
                        except Exception:
                            log.warning("Failed to parse last_seen for typhoon %s", num)
                self._ui_log("INFO", f"Loaded typhoon state: {len(self._typhoon_state)} typhoons, {len(self._typhoon_history)} history records")
        except Exception as e:
            self._ui_log("WARN", f"Failed to load typhoon state: {e}")
            self._typhoon_state = {}
            self._typhoon_history = []
            self._typhoon_last_hash = {}

    def _save_typhoon_state(self):
        try:
            self._typhoon_state_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = Path(str(self._typhoon_state_path) + ".tmp")
            # 履歴の上限適用
            if len(self._typhoon_history) > config.MAX_STATE_HISTORY_ITEMS:
                del self._typhoon_history[:-config.MAX_STATE_HISTORY_ITEMS]
            data = dict(self._typhoon_state)
            data["_history"] = list(getattr(self, "_typhoon_history", []))
            data["_last_hash"] = dict(getattr(self, "_typhoon_last_hash", {}))
            with temp_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(temp_path, self._typhoon_state_path)
        except Exception as e:
            self._ui_log("WARN", f"Failed to save typhoon state: {e}")

    def _check_typhoon_changes(self, typhoons: list[dict[str, Any]]) -> list[str]:
        lifted_messages = []
        current_numbers = set()
        now = datetime.now()
        DISSIPATION_TIMEOUT_HOURS = 6

        with self._typhoon_state_lock:
            for typhoon in typhoons:
                number = typhoon.get("number", "")
                if not number:
                    continue
                current_numbers.add(number)
                typhoon_class = typhoon.get("typhoon_class", "")

                if number not in self._typhoon_state:
                    birth = typhoon.get("birth", now.strftime("%Y/%m/%d %H:%M"))
                    self._typhoon_state[number] = {
                        "name": typhoon.get("name", ""),
                        "class": typhoon_class,
                        "last_seen": now.strftime("%Y/%m/%d %H:%M"),
                        "birth": birth,
                        "url": typhoon.get("url", ""),
                        "summary": typhoon.get("summary", ""),
                    }
                    name = typhoon.get("name", "")
                    tag = f"[TY No.{number}]" if number else "[TY]"
                    lifted_messages.append(f"{tag} {name} <NEW BIRTH> since {birth}")
                    self._typhoon_history.append({
                        "ts": datetime.now().isoformat(),
                        "number": number,
                        "name": name,
                        "old_class": "",
                        "new_class": typhoon_class,
                        "condition": "NEW BIRTH",
                        "sent": True,
                    })
                else:
                    prev_class = self._typhoon_state[number].get("class", "")
                    if "dissipated_at" in self._typhoon_state[number]:
                        if "台風" in typhoon_class:
                            del self._typhoon_state[number]["dissipated_at"]
                            birth = typhoon.get("birth", now.strftime("%Y/%m/%d %H:%M"))
                            name = typhoon.get("name", "")
                            tag = f"[TY No.{number}]" if number else "[TY]"
                            lifted_messages.append(f"{tag} {name} <REBIRTH> since {birth}")
                            self._typhoon_state[number]["class"] = typhoon_class
                            self._typhoon_state[number]["last_seen"] = now.strftime("%Y/%m/%d %H:%M")
                            self._typhoon_state[number]["summary"] = typhoon.get("summary", "")
                            self._typhoon_history.append({
                                "ts": datetime.now().isoformat(),
                                "number": number,
                                "name": name,
                                "old_class": "dissipated",
                                "new_class": typhoon_class,
                                "condition": "REBIRTH",
                                "sent": True,
                            })
                    elif typhoon_class and typhoon_class != prev_class:
                        name = typhoon.get("name", "")
                        if "温帯低気圧" in typhoon_class:
                            lifted_messages.append(f"[TY No.{number}] {name} <LIFTED> DOWNGRADED TO EXTRATROPICAL CYCLONE")
                        elif "熱帯低気圧" in typhoon_class:
                            lifted_messages.append(f"[TY No.{number}] {name} <LIFTED> DOWNGRADED TO TROPICAL DEPRESSION")
                        elif "台風" in typhoon_class and ("温帯低気圧" in prev_class or "熱帯低気圧" in prev_class):
                            pass
                        self._typhoon_state[number]["class"] = typhoon_class
                        self._typhoon_state[number]["last_seen"] = now.strftime("%Y/%m/%d %H:%M")
                        self._typhoon_state[number]["summary"] = typhoon.get("summary", "")
                        self._typhoon_history.append({
                            "ts": datetime.now().isoformat(),
                            "number": number,
                            "name": name,
                            "old_class": prev_class,
                            "new_class": typhoon_class,
                            "condition": "CLASS CHANGE",
                            "sent": True,
                        })
                    else:
                        self._typhoon_state[number]["last_seen"] = now.strftime("%Y/%m/%d %H:%M")
                        self._typhoon_state[number]["url"] = typhoon.get("url", "")
                        self._typhoon_state[number]["summary"] = typhoon.get("summary", "")

            for number in list(self._typhoon_state.keys()):
                if number not in current_numbers:
                    last_seen_str = self._typhoon_state[number].get("last_seen", "")
                    try:
                        last_seen_dt = datetime.strptime(last_seen_str, "%Y/%m/%d %H:%M")
                        elapsed_hours = (now - last_seen_dt).total_seconds() / 3600
                    except Exception:
                        log.warning("Failed to parse last_seen for typhoon %s", number)
                        elapsed_hours = DISSIPATION_TIMEOUT_HOURS + 1
                    if elapsed_hours >= DISSIPATION_TIMEOUT_HOURS:
                        name = self._typhoon_state[number].get("name", "")
                        prev_class = self._typhoon_state[number].get("class", "")
                        if "dissipated_at" in self._typhoon_state[number]:
                            try:
                                diss_dt = datetime.strptime(self._typhoon_state[number]["dissipated_at"], "%Y/%m/%d %H:%M")
                                if (now - diss_dt).total_seconds() >= 24 * 3600:
                                    del self._typhoon_state[number]
                            except Exception:
                                log.warning("Failed to parse dissipated_at for typhoon %s", number)
                                del self._typhoon_state[number]
                        else:
                            if "温帯低気圧" not in prev_class and "熱帯低気圧" not in prev_class:
                                lifted_messages.append(f"[TY No.{number}] {name} <LIFTED> DISSIPATED")
                            self._typhoon_state[number]["dissipated_at"] = now.strftime("%Y/%m/%d %H:%M")
                            self._typhoon_history.append({
                                "ts": datetime.now().isoformat(),
                                "number": number,
                                "name": name,
                                "old_class": prev_class,
                                "new_class": "DISSIPATED",
                                "condition": "DISSIPATED",
                                "sent": True,
                            })

            self._save_typhoon_state()
        return lifted_messages

    def _send_typhoon_info_manual(self):
        typhoons = self._fetch_typhoon_info()
        if not typhoons:
            self._ui_log("INFO", "No typhoon information available.")
            return

        lifted_messages = self._check_typhoon_changes(typhoons)

        if not config.ENABLE_TYPHOON_INFO:
            self._ui_log("INFO", "Typhoon manual send skipped (disabled)")
            return

        for msg in lifted_messages:
            header_msg = f"[[TYPHOON INFO]] {datetime.now().strftime('%Y/%m/%d %H:%M')} (Q.3h)"
            self.send_text(header_msg)
            self._send_discord(header_msg)
            self.send_text(msg)
            self._send_discord(msg)

        active_typhoons = [t for t in typhoons if "台風" in t.get("typhoon_class", "")]
        for typhoon in active_typhoons:
            now_str = datetime.now().strftime("%Y/%m/%d %H:%M")
            header_msg = f"[[TYPHOON INFO]] {now_str} (Q.3h)"
            self.send_text(header_msg)
            self._send_discord(header_msg)
            self.send_text(typhoon["summary"])
            self._send_discord(typhoon["summary"])
            if typhoon["url"] and config.ENABLE_TYPHOON_MAP_URL:
                self.typhoon_urls.append(typhoon["url"])
                typhoon_tag = f"[TY No.{typhoon.get('number', '')}]" if typhoon.get('number') else "[TY]"
                map_msg = f"{typhoon_tag} Position Map URL {typhoon['url']}"
                self.send_text(map_msg)
                self._send_discord(map_msg)
        self._last_typhoon_send = time.time()

    def _send_all_current_warnings_as_summary(self):
        if self._startup_in_progress:
            self._ui_log("INFO", "Weather summary blocked: system still starting up")
            return

        with self._weather_lock:
            current: dict[str, WeatherChange] = {}
            for code, state in self._weather_state.items():
                warning_only = self._filter_warning_only(state["kinds"])
                if warning_only:
                    issued = state.get("issued_times", {})
                    current[code] = WeatherChange(warning_only, issued, {})
        if current:
            original_startup_time = getattr(self, '_startup_complete_time', 0)
            self._startup_complete_time = 0
            try:
                self._send_weather_messages(current)
            finally:
                self._startup_complete_time = original_startup_time
        else:
            self._ui_log("INFO", "Weather summary: no active warnings to send")

    # ===================================
    # Megaquake-Related Info
    # ===================================
    def _handle_megaquake_info(self, detail_root, eid: str):
        now = datetime.now()
        info_type_elem = self._find_local_first(detail_root, 'InfoType')
        info_type = info_type_elem.text if info_type_elem is not None else ""
        headline_elem = self._find_local_first(detail_root, 'Headline')
        headline_text = ""
        if headline_elem is not None:
            text_elem = self._find_local_first(headline_elem, 'Text')
            if text_elem is not None and text_elem.text:
                headline_text = text_elem.text.strip()
        dt = self._parse_report_datetime(detail_root)
        if dt:
            report_date, report_time = dt
        else:
            report_date = now.strftime("%Y/%m/%d")
            report_time = now.strftime("%H:%M")

        if "VXSE61" in eid:
            if info_type == "取消":
                header_msg = f"[[MEGAQUAKE INFO]] {report_date} {report_time}"
                body_msg = ("[AFTERSHOCK] <ADVISORY LIFTED> HOKKAIDO/SANRIKU Subsequent Earthquake Advisory\n"
                            "https://www.jma.go.jp/bosai/nceq/")
            else:
                header_msg = f"[[MEGAQUAKE INFO]] {report_date} {report_time}"
                body_msg = ("[AFTERSHOCK] <ADVISORY ACTIVE> HOKKAIDO/SANRIKU Subsequent Earthquake Advisory\n"
                            "https://www.jma.go.jp/bosai/nceq/")
            self._send_with_discord(header_msg)
            self._send_with_discord(body_msg)
        elif "VXSE62" in eid:
            surveying_keywords = ["調査中", "調査"]
            lifted_keywords = ["調査終了", "終了"]
            if any(kw in headline_text for kw in surveying_keywords):
                header_msg = f"[[MEGAQUAKE INFO]] {report_date} {report_time}"
                body_msg = ("[NANKAI TROUGH] <SURVEYING> Surveying for possible great earthquake along the Nankai Trough.\n"
                            "https://www.jma.go.jp/bosai/nteq/")
            elif "巨大地震警戒" in headline_text:
                header_msg = f"[[MEGAQUAKE WR]] {report_date} {report_time}"
                body_msg = ("[NANKAI TROUGH] <ALERT> High probability of a great earthquake.\n"
                            "https://www.jma.go.jp/bosai/nteq/")
            elif "巨大地震注意" in headline_text:
                header_msg = f"[[MEGAQUAKE WR]] {report_date} {report_time}"
                body_msg = ("[NANKAI TROUGH] <WARNING> Increased probability of a great earthquake.\n"
                            "https://www.jma.go.jp/bosai/nteq/")
            elif any(kw in headline_text for kw in lifted_keywords) or info_type == "取消":
                header_msg = f"[[MEGAQUAKE WR]] {report_date} {report_time}"
                body_msg = ("[NANKAI TROUGH] <LIFTED> Survey completed. No significant anomaly detected.\n"
                            "https://www.jma.go.jp/bosai/nteq/")
            else:
                header_msg = f"[[MEGAQUAKE INFO]] {report_date} {report_time}"
                body_msg = ("[NANKAI TROUGH] <SURVEYING> Surveying for possible great earthquake along the Nankai Trough.\n"
                            "https://www.jma.go.jp/bosai/nteq/")
            self._send_with_discord(header_msg)
            self._send_with_discord(body_msg)
        self._processed_weather_ids[eid] = time.time()
        if len(self._processed_weather_ids) > config.PROCESSED_WEATHER_IDS_MAX:
            oldest_keys = sorted(self._processed_weather_ids, key=lambda k: self._processed_weather_ids[k])[:len(self._processed_weather_ids) - config.PROCESSED_WEATHER_IDS_MAX]
            for k in oldest_keys:
                self._processed_weather_ids.pop(k, None)
        self._ui_log("INFO", f"MEGAQUAKE INFO sent: {headline_text[:60]}")

    # ===================================
    # Logging / LoRa
    # ===================================
    def _send_with_discord(self, text: str, skip_dedup: bool = False, urgent: bool = False):
        self.send_text(text, urgent=urgent, skip_dedup=skip_dedup)
        self._send_discord(text, skip_dedup=skip_dedup)

    def _ui_log(self, level: str, message: str):
        if self.log_callback:
            try: self.log_callback(level, message)
            except Exception: pass
        log.info("[%s] %s", level, message)

    @staticmethod
    def _categorize_text(text: str) -> str:
        if text.startswith("[EEW"):
            return "eew"
        if text.startswith("[[EARTHQUAKE]]") or text.startswith("[EQ]"):
            return "eq"
        if (text.startswith("[[TSUNAMI WR]]")
                or text.startswith("[MAJOR TSUNAMI WARNING]")
                or text.startswith("[TSUNAMI WARNING]")
                or text.startswith("[TSUNAMI ADVISORY]")
                or text.startswith("[U/G:")
                or text.startswith("[D/G:")):
            return "tsunami"
        if text.startswith("[[VOLCANO") or text.startswith("[VOLCANO"):
            return "volcano"
        if text.startswith("[[WEATHER WARNING]]"):
            return "weather"
        if text.startswith("[WX]"):
            if "Flood Info URL" in text or "Flood since" in text:
                return "river_flood"
            return "weather"
        if (text.startswith("[[TYPHOON INFO]]")
                or text.startswith("[TY No.")
                or text.startswith("[TY]")):
            return "typhoon"
        if text.startswith("<<J-ALERT>>"):
            return "jalert"
        if text.startswith("[[MEMORIAL]]"):
            return "memorial"
        if text.startswith("[[MEGAQUAKE"):
            return "megaquake"
        if text.startswith("[[SCH_TX]]") or text.startswith("[[TEST]]"):
            return "test"
        return "other"

    def _init_lora(self):
        try:
            import meshtastic.serial_interface
            self.lora_iface = meshtastic.serial_interface.SerialInterface(devPath=config.SERIAL_PORT)
            self._ui_log("INFO", f"LoRa serial opened: {config.SERIAL_PORT}")

            # 接続デバイスから自ノード番号を取得し、Configへ反映する
            node_info = self.lora_iface.getMyNodeInfo()
            node_num = node_info.get("num") if node_info else None
            if node_num:
                config.LORA_NODE_ID = "!" + format(int(node_num), "08x")
                self._ui_log("INFO", f"LoRa node ID acquired: {config.LORA_NODE_ID}")
            else:
                self._ui_log("ERROR", "LoRa node ID acquisition failed: no node number returned")
                self.lora_iface.close()
                self.lora_iface = None
        except Exception as e:
            self._ui_log("ERROR", f"LoRa init failed: {e}")
            self.lora_iface = None

    # ===================================
    # Outbound Send API
    # ===================================
    def _is_recent_duplicate(self, text: str, channel: str = "lora") -> bool:
        import hashlib
        h = channel + ":" + hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()
        now = time.time()
        with self._dedup_lock:
            expired = [k for k, ts in self._recent_sent_hashes.items() if now - ts > config.DUPLICATE_SUPPRESS_SEC]
            for k in expired:
                del self._recent_sent_hashes[k]
            if h in self._recent_sent_hashes:
                return True
            self._recent_sent_hashes[h] = now
            return False

    def send_text(self, text: str, urgent: bool = False, skip_dedup: bool = False):
        if not text: return
        if text.startswith("[[TEST]]") or text.startswith("[[SCH_TX]]"):
            now_ts = time.time()
            if now_ts - self._last_test_send_time < 60:
                self._ui_log("WARN", f"Test message suppressed (within 60s cooldown): {text[:60]}")
                return
            self._last_test_send_time = now_ts
        if not skip_dedup and self._is_recent_duplicate(text):
            self._ui_log("WARN", f"Duplicate send suppressed (<{config.DUPLICATE_SUPPRESS_SEC:.0f}s): {text[:60]}")
            return
        if urgent:
            self._send_queue.put(PrioritySendItem(1, "urgent", text))
        else:
            self._send_queue.put(PrioritySendItem(self._get_normal_priority(text), "normal", text))

    def send_dm(self, text: str, target: str):
        if text:
            self._send_queue.put(PrioritySendItem(90, "dm", text, target=target))

    def send_text_batch(self, texts: list[str]):
        if not texts:
            return
        for t in texts:
            self._send_queue.put(PrioritySendItem(80, "memorial", t))

    def _do_send(self, text: str, target: str | None = None):
        chunks = split_message(text)
        for i, chunk in enumerate(chunks):
            self._send_text_internal(chunk, target=target)
            time.sleep(config.MSG_INTERVAL_SEC if i == len(chunks)-1 else config.SPLIT_WAIT_SEC)

    def _do_send_urgent(self, text: str, target: str | None = None):
        chunks = split_message(text)
        for i, chunk in enumerate(chunks):
            self._send_text_internal(chunk, target=target)
            if i < len(chunks) - 1:
                time.sleep(config.SPLIT_WAIT_SEC)
            else:
                time.sleep(config.URGENT_MSG_INTERVAL_SEC)

    @staticmethod
    def _get_normal_priority(text: str) -> int:
        """通常キューのメッセージ優先度を返す（小さいほど高優先）"""
        if text.startswith("[[EARTHQUAKE]]") or text.startswith("[EQ]"):
            return 1
        if (text.startswith("[[WEATHER WARNING]]") or
                text.startswith("[[RIVER FLOOD]]") or
                text.startswith("[WX]")):
            return 2
        if text.startswith("[[TYPHOON INFO]]") or text.startswith("[TY No."):
            return 3
        return 4

    def _send_worker(self):
        while not self._stop_event.is_set():
            try:
                item = self._send_queue.get()
            except queue.Empty:
                time.sleep(0.1)
                continue

            try:
                if item.kind == "dm":
                    chunks = split_message(item.text)
                    for i, chunk in enumerate(chunks):
                        ok = self._send_dm_internal(chunk, item.target)
                        if not ok:
                            self._ui_log("WARN", f"DM dropped (no PKI key): target={item.target} msg={chunk}")
                            break
                        time.sleep(config.SPLIT_WAIT_SEC if i < len(chunks)-1 else config.MSG_INTERVAL_SEC)

                elif item.kind == "memorial":
                    chunks = split_message(item.text, mode="memorial")
                    for i, chunk in enumerate(chunks):
                        self._send_text_internal(chunk, target=None)
                        time.sleep(config.MSG_INTERVAL_SEC if i == len(chunks)-1 else 3.0)

                elif item.kind == "jalert":
                    self._do_send_urgent(item.text)
                    self._send_discord(item.text, urgent=True)

                elif item.kind == "urgent":
                    self._do_send_urgent(item.text)

                else:
                    self._do_send(item.text)

            except Exception as e:
                self._ui_log("ERROR", f"Send worker error for kind={item.kind}: {e}")

            time.sleep(config.MSG_INTERVAL_SEC if item.kind in ("normal", "urgent", "jalert") else 0.1)

    def _get_node_public_key(self, node_id: str) -> bytes | None:
        if not self.lora_iface or not self.lora_iface.nodesByNum: return None
        try: node_num = int(node_id.lstrip("!"), 16)
        except ValueError: return None
        node = self.lora_iface.nodesByNum.get(node_num)
        if node and 'user' in node:
            pub_key = node['user'].get('publicKey')
            if pub_key: return pub_key
        return None

    def _send_dm_internal(self, text: str, target: str) -> bool:
        public_key = self._get_node_public_key(target)
        if not public_key: self._ui_log("WARN", f"DM PKI failed: no public key for {target}"); return False
        try:
            from meshtastic.protobuf import portnums_pb2
            if isinstance(public_key, str): public_key = base64.b64decode(public_key)
            public_key_b64 = base64.b64encode(public_key).decode('utf-8')
            self._ui_log("TX", f"DM->{target} {text} [key:{public_key_b64}]")
            self.lora_iface.sendData(text.encode("utf-8"), destinationId=target,
                                    portNum=portnums_pb2.PortNum.TEXT_MESSAGE_APP,
                                    pkiEncrypted=True, publicKey=public_key,
                                    wantAck=False, channelIndex=config.CHANNEL_INDEX)
            return True
        except Exception as e: self._ui_log("ERROR", f"DM PKI send failed for {target}: {e}"); return False

    # ===================================
    # Discord Output
    # ===================================
    def _send_discord(self, text: str, skip_dedup: bool = False, urgent: bool = False):
        if not config.ENABLE_DISCORD or not config.DISCORD_WEBHOOK_URL:
            return
        if not skip_dedup and self._is_recent_duplicate(text, channel="discord"):
            self._ui_log("WARN", f"Duplicate Discord send suppressed (<{config.DUPLICATE_SUPPRESS_SEC:.0f}s): {text[:60]}")
            return
        self._discord_queue.put((text, urgent))

    def _discord_worker(self):
        while not self._stop_event.is_set():
            try:
                text, urgent = self._discord_queue.get(timeout=1)
            except queue.Empty:
                continue
            try:
                MAX_CHARS = 1950
                chunks = []
                remaining = text
                while len(remaining) > MAX_CHARS:
                    split_at = remaining.rfind('\n', 0, MAX_CHARS)
                    if split_at == -1:
                        split_at = MAX_CHARS
                    chunks.append(remaining[:split_at])
                    remaining = remaining[split_at:].lstrip()
                if remaining:
                    chunks.append(remaining)
                for i, chunk in enumerate(chunks):
                    max_attempts = 2 if urgent else 1
                    for attempt in range(max_attempts):
                        try:
                            r = requests.post(config.DISCORD_WEBHOOK_URL, json={"content": chunk}, timeout=10)
                            if r.status_code in (200, 204):
                                break
                            self._ui_log("WARN", f"Discord send failed: {r.status_code}")
                        except Exception as e:
                            self._ui_log("WARN", f"Discord send error: {e}")
                        if attempt < max_attempts - 1:
                            time.sleep(1.0)
                    if i < len(chunks) - 1:
                        time.sleep(1.0)
            finally:
                self._discord_queue.task_done()

    def _send_test_discord(self, tag: str = "[[TEST]]"):
        if not config.ENABLE_DISCORD or not config.DISCORD_WEBHOOK_URL:
            return
        lora_ok = self.lora_iface is not None
        mqtt_ok = self.mqtt is not None and self.mqtt._running
        discord_ok = True
        msg = build_test_message(lora_available=lora_ok, mqtt_available=mqtt_ok, discord_available=discord_ok, tag=tag)
        self._send_discord(msg)

    def _send_text_internal(self, text: str, target: str | None = None) -> int | None:
        prefix = "TX" if target is None else f"DM->{target}"
        self._ui_log(prefix, text)
        if config.USE_DUMMY:
            return None

        import hashlib
        chunk_hash = hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()
        now_ts = time.time()
        with self._dedup_lock:
            expired = [h for h, ts in self._final_guard_hashes.items() if now_ts - ts > 1.0]
            for h in expired:
                del self._final_guard_hashes[h]
            if chunk_hash in self._final_guard_hashes:
                self._ui_log("WARN", f"Final guard blocked duplicate (1s): {text[:60]}")
                return None
            self._final_guard_hashes[chunk_hash] = now_ts

        # Txカウンタのインクリメント（チャネル送信のみ、DMは対象外）
        if target is None:
            cat = self._categorize_text(text)
            with self._tx_counters_lock:
                self._tx_counters[cat] = self._tx_counters.get(cat, 0) + 1

        packet_id = None
        if config.USE_MQTT and self.mqtt and target is None:
            try:
                packet_id = self.mqtt.send_text(text)
            except Exception as e:
                self._ui_log("ERROR", f"MQTT send: {e}")
        if config.USE_LORA and self.lora_iface:
            try:
                if target and target.startswith("!"):
                    self.lora_iface.sendText(text, destinationId=target, channelIndex=config.CHANNEL_INDEX)
                else:
                    self.lora_iface.sendText(text, channelIndex=config.CHANNEL_INDEX)
            except Exception as e:
                self._ui_log("ERROR", f"LoRa send: {e}")
        return packet_id

    # ===================================
    # P2PQuake History Fetch
    # ===================================
    def _fetch_history(self):
        try:
            targets = [("551", config.HISTORY_FETCH_LIMIT_EQ, "eq"), ("554", config.HISTORY_FETCH_LIMIT_EQ, "eq"), ("552", config.HISTORY_FETCH_LIMIT_TS, "tsunami")]
            for code, limit, kind in targets:
                r = request_with_retry(
                    "GET",
                    "https://api.p2pquake.net/v2/history",
                    retries=2,
                    backoff=1.0,
                    timeout=(5, 15),
                    params={"codes": code, "limit": limit},
                )
                if r is None or r.status_code != 200:
                    continue
                data = r.json()
                if not isinstance(data, list): continue
                events_to_add = []
                for ev in data:
                    eid = str(ev.get("_id") or ev.get("id") or "")
                    if not eid or store.is_duplicate(eid): continue
                    if kind == "eq":
                        eq = ev.get("earthquake", {}) or {}
                        if eq.get("maxScale", 0) >= config.MIN_SENDO_SCALE: events_to_add.append(ev)
                    else: events_to_add.append(ev)
                events_to_add.sort(key=lambda x: x.get("earthquake", {}).get("time", "") if kind == "eq" else x.get("time", "") or x.get("issue", {}).get("time", ""), reverse=True)
                for ev in events_to_add:
                    eid = str(ev.get("_id") or ev.get("id") or "")
                    if kind == "eq": store.add_eq(eid, ev, save=False)
                    else: store.add_tsunami(eid, ev, save=False)
                if events_to_add:
                    if kind == "eq": store.request_save("eq")
                    else: store.request_save("tsunami")
                    self._ui_log("INFO", f"Fetched history for code {code}: {len(events_to_add)} events")
        except Exception as e: self._ui_log("WARN", f"History fetch error: {e}")

    # ===================================
    # WebSocket Ingest Loops
    # ===================================
    async def _ws_loop(self):
        if not _HAS_WEBSOCKETS:
            self._ui_log("ERROR", "websockets package not installed")
            return
        while not self._stop_event.is_set():
            try:
                await asyncio.gather(self._p2pquake_ws_loop(), self._wolfx_ws_loop())
            except Exception as e:
                self._ui_log("ERROR", f"WS loop crashed: {e}, restarting in 10s")
                await asyncio.sleep(10)

    async def _p2pquake_ws_loop(self):
        if not _HAS_WEBSOCKETS:
            return
        backoff = config.WS_BACKOFF_INIT
        while not self._stop_event.is_set():
            try:
                self._ui_log("INFO", f"P2PQuake connecting to {config.WS_URL}")
                async with websockets.connect(config.WS_URL, ping_interval=30) as ws:
                    self._ui_log("INFO", "P2PQuake WebSocket connected")
                    self.p2pquake_connected = True
                    self._state["eew_lock"] = False
                    backoff = config.WS_BACKOFF_INIT
                    # 接続成功時はカウンタをリセットしない（連続切断判定のため）
                    async for raw in ws:
                        if self._stop_event.is_set(): break
                        self._p2pquake_last_received = time.time()
                        try: payload = json.loads(raw); self._dispatch_payload(payload)
                        except json.JSONDecodeError: continue
                        except Exception as e: self._ui_log("ERROR", f"P2PQuake dispatch err: {e}")
            except Exception as e:
                self.p2pquake_connected = False
                reason = str(e)
                now_ts = time.time()
                last_disconnect = getattr(self, "_ws_last_disconnect_ts", {}).get("p2pquake", 0)
                if now_ts - last_disconnect > 60:
                    count = 0
                else:
                    count = self._ws_error_count.get("p2pquake", 0)
                count += 1
                self._ws_error_count["p2pquake"] = count
                if not hasattr(self, "_ws_last_disconnect_ts"):
                    self._ws_last_disconnect_ts = {}
                self._ws_last_disconnect_ts["p2pquake"] = now_ts

                if count == 1:
                    self._ui_log("WARN", f"P2PQuake error: {reason}; reconnect in {backoff}s")
                else:
                    self._ui_log("INFO", f"P2PQuake disconnected (count={count}): {reason}; reconnect in {backoff}s")

                for _ in range(int(backoff / 0.5)):
                    if self._stop_event.is_set():
                        return
                    await asyncio.sleep(0.5)
                backoff = min(backoff*2, config.WS_BACKOFF_MAX)

    async def _wolfx_ws_loop(self):
        if not _HAS_WEBSOCKETS:
            return
        backoff = config.WS_BACKOFF_INIT
        while not self._stop_event.is_set():
            try:
                self._ui_log("INFO", f"Wolfx connecting to {config.WOLFX_WS_URL}")
                async with websockets.connect(config.WOLFX_WS_URL, ping_interval=30) as ws:
                    self._ui_log("INFO", "Wolfx WebSocket connected"); self._state["eew_lock"] = False; backoff = config.WS_BACKOFF_INIT
                    # 接続成功時はカウンタをリセットしない（連続切断判定のため）
                    async for raw in ws:
                        if self._stop_event.is_set(): break
                        try:
                            wolfx_data = json.loads(raw)
                            if wolfx_data.get("type") == "heartbeat": continue
                            msg_type = wolfx_data.get("type")
                            if msg_type == "jma_eew":
                                payload = wolfx_to_internal_eew(wolfx_data)
                                self._dispatch_payload(payload)
                            elif msg_type == "jma_eq":
                                if self.p2pquake_connected and (time.time() - self._p2pquake_last_received) < 60:
                                    continue
                                payload = wolfx_to_internal_eq(wolfx_data)
                                if payload:
                                    self._dispatch_payload(payload)
                        except json.JSONDecodeError: continue
                        except Exception as e: self._ui_log("ERROR", f"Wolfx dispatch err: {e}")
            except Exception as e:
                reason = str(e)
                now_ts = time.time()
                last_disconnect = getattr(self, "_ws_last_disconnect_ts", {}).get("wolfx", 0)
                if now_ts - last_disconnect > 60:
                    count = 0
                else:
                    count = self._ws_error_count.get("wolfx", 0)
                count += 1
                self._ws_error_count["wolfx"] = count
                if not hasattr(self, "_ws_last_disconnect_ts"):
                    self._ws_last_disconnect_ts = {}
                self._ws_last_disconnect_ts["wolfx"] = now_ts

                if count == 1:
                    self._ui_log("WARN", f"Wolfx error: {reason}; reconnect in {backoff}s")
                else:
                    self._ui_log("INFO", f"Wolfx disconnected (count={count}): {reason}; reconnect in {backoff}s")

                for _ in range(int(backoff / 0.5)):
                    if self._stop_event.is_set():
                        return
                    await asyncio.sleep(0.5)
                backoff = min(backoff*2, config.WS_BACKOFF_MAX)

    # ===================================
    # Payload Dispatch
    # ===================================
    def _handle_eq_tsunami_payload(self, payload: dict, code: int):
        eid = str(payload.get("_id") or payload.get("id") or "")
        if code == 551 and eid and store.has_eq(eid):
            existing = store.get_event(eid)
            if not (existing and existing.get("code") == 554):
                self._ui_log("INFO", f"Skipping duplicate earthquake event {eid}")
                return
        self._state["is_preliminary"] = False
        self._discord_eq_lines = []
        def eq_discord_send(msg: str):
            # マップURLは送信オフでも監視タブ用に必ず記録
            if msg.startswith("[EQ] Epicenter Map URL "):
                url = msg.replace("[EQ] Epicenter Map URL ", "", 1).strip()
                self.earthquake_urls.append(url)
            if not config.ENABLE_EQ:
                self._ui_log("INFO", f"EQ send skipped (disabled): {msg[:60]}")
                self._discord_eq_lines.append(msg)
                return
            self.send_text(msg, urgent=True)
            self._discord_eq_lines.append(msg)
        ok = handle_event(payload, eq_discord_send, self._state, code=code)
        if ok:
            handle_eq(payload, eq_discord_send, self._state)
            self._state["last_eq_send"] = time.time()
        self._state["eew_lock"] = False
        if self._discord_eq_lines:
            if config.ENABLE_EQ:
                self._send_discord("\n".join(self._discord_eq_lines))
            self._discord_eq_lines = []

    def _dispatch_payload(self, payload: dict):
        code = payload.get("code")

        if code in (561, 562):
            if "isCancel" not in payload: payload["isCancel"] = payload.get("cancel", False)
            def eew_send_fn(msg: str):
                if not config.ENABLE_EEW:
                    self._ui_log("INFO", f"EEW send skipped (disabled): {msg[:60]}")
                    return
                self.send_text(msg, urgent=True)
                self._send_discord(msg)
            def eew_send_lora_only(msg: str):
                if not config.ENABLE_EEW:
                    self._ui_log("INFO", f"EEW send skipped (disabled): {msg[:60]}")
                    return
                self.send_text(msg, urgent=True)
            def eew_send_discord_combined(main_msg: str, map_msg: str):
                if not config.ENABLE_EEW:
                    self._ui_log("INFO", f"EEW Discord send skipped (disabled): {main_msg[:60]}")
                    return
                combined = main_msg + "\n" + map_msg
                self._send_discord(combined)
            handle_eew(payload, eew_send_fn, self._state,
                       eew_send_lora_only=eew_send_lora_only,
                       eew_send_discord_combined=eew_send_discord_combined)
        elif code == 552:
            def tsunami_send_fn(msg: str):
                if not config.ENABLE_TSUNAMI:
                    self._ui_log("INFO", f"Tsunami send skipped (disabled): {msg[:60]}")
                    return
                self.send_text(msg, urgent=True)
                self._send_discord(msg)
            handle_tsunami(payload, tsunami_send_fn, self._state)
        elif code == 554:
            self._ui_log("INFO", "Skipping preliminary EQ (code 554) for both LoRa and Discord")
            def dummy_send(msg: str):
                pass
            handle_event(payload, dummy_send, self._state, code=554)
            return
        elif code == 551:
            self._handle_eq_tsunami_payload(payload, code)

    # ===================================
    # Periodic Background Loop
    # ===================================
    def _periodic_loop(self):
        last_diff_fetch = 0
        last_tsunami_fallback = 0
        last_eew_cleanup = time.time()
        last_megaquake_check = 0
        last_river_flood_check = 0
        last_volcano_check = 0

        self._startup_complete_time = time.time()
        self._startup_in_progress = False
        with self._weather_lock:
            for code, state in self._weather_state.items():
                if state["kinds"]:
                    self._last_sent_kinds_snapshot[code] = frozenset(state["kinds"].items())
                state["unsent"] = False
            self._weather_lifted.clear()

        with self._volcano_lock:
            last_by_key = {}
            for rec in getattr(self, "_volcano_history", []):
                last_by_key[rec.get("name", "")] = rec
            for key, state in self._volcano_state.items():
                if state.get("level", 1) >= 2:
                    if key in last_by_key:
                        rec = last_by_key[key]
                        snapshot = frozenset({
                            (key, rec.get("new_level", state.get("level", 0)),
                             rec.get("condition", ""), state.get("warning_type_jp", ""))
                        })
                    else:
                        snapshot = frozenset({
                            (key, state.get("level", 0), state.get("condition", ""), state.get("warning_type_jp", ""))
                        })
                    self._volcano_last_sent_snapshot[key] = snapshot

        with self._river_flood_lock:
            last_by_key = {}
            for rec in getattr(self, "_river_flood_history", []):
                last_by_key[rec.get("name", "")] = rec
            for river_name, state in self._river_flood_state.items():
                if state.get("level", 1) >= 3:
                    if river_name in last_by_key:
                        rec = last_by_key[river_name]
                        snapshot = frozenset({(river_name, rec.get("new_level", state.get("level", 0)))})
                    else:
                        snapshot = frozenset({(river_name, state.get("level", 0))})
                    self._river_flood_last_sent_snapshot[river_name] = snapshot

        while not self._stop_event.is_set():
            time.sleep(10)
            now_ts = time.time()

            if now_ts - getattr(self, "_last_resource_log", 0) >= config.RESOURCE_MONITOR_INTERVAL_SEC:
                import threading as _threading

                thread_count = _threading.active_count()
                send_counts = self._send_queue.count_by_kind()
                urgent_q = send_counts.get("urgent", 0)
                normal_q = send_counts.get("normal", 0)
                jalert_q = send_counts.get("jalert", 0)
                memorial_q = send_counts.get("memorial", 0)

                resource_msg = (
                    f"Resource monitor: threads={thread_count} urgent={urgent_q} normal={normal_q}"
                    f" jalert={jalert_q} memorial={memorial_q}"
                )

                # 各フィードの最終取得成功時刻と連続失敗回数
                for feed_name in ("r8", "river", "volcano", "typhoon", "megaquake", "jalert"):
                    last = self._feed_last_success.get(feed_name, 0.0)
                    age = int(now_ts - last) if last > 0 else -1
                    fails = self._feed_fail_count.get(feed_name, 0)
                    resource_msg += f" {feed_name}_age={age}s {feed_name}_fail={fails}"

                # 処理済みIDの保持件数
                resource_msg += (
                    f" ids weather={len(self._processed_weather_ids)}"
                    f" river={len(self._river_flood_processed_ids)}"
                    f" volcano={len(self._volcano_processed_ids)}"
                    f" jalert={len(self._jalert_sent_hashes)}"
                )

                # Tx総数とカテゴリ別件数
                with self._tx_counters_lock:
                    tx_snapshot = dict(self._tx_counters)
                tx_total = sum(tx_snapshot.values())
                resource_msg += f" tx_total={tx_total}"
                for cat in ("eew", "eq", "tsunami", "volcano", "weather",
                            "river_flood", "typhoon", "jalert", "memorial",
                            "megaquake", "test", "other"):
                    resource_msg += f" tx_{cat}={tx_snapshot.get(cat, 0)}"

                try:
                    import psutil

                    cpu_percent = psutil.cpu_percent(interval=None)
                    mem_percent = psutil.virtual_memory().percent

                    try:
                        load_avg = psutil.getloadavg()
                        load_msg = f" load1={load_avg[0]:.2f} load5={load_avg[1]:.2f} load15={load_avg[2]:.2f}"
                    except Exception:
                        load_msg = ""

                    resource_msg += f" cpu={cpu_percent:.1f}% mem={mem_percent:.1f}%{load_msg}"
                except ImportError:
                    pass
                except Exception:
                    pass

                self._ui_log("RESOURCE", resource_msg)
                self._last_resource_log = now_ts

            try:
                self._send_lifted_messages_if_ready()
            except Exception as e:
                self._ui_log("ERROR", f"Lifted send error: {e}")

            try:
                if config.USE_SCHEDULED_TEST:
                    t = datetime.now()
                    today = t.strftime("%Y%m%d")
                    if getattr(self, "_scheduled_date", "") != today:
                        self._scheduled_sent.clear()
                        self._scheduled_date = today
                    key = (today, t.hour)
                    if t.hour in config.SCHEDULED_TEST_HOURS and t.minute == 0 and key not in self._scheduled_sent:
                        now_ts = time.time()
                        lora_ok = self.lora_iface is not None
                        mqtt_ok = self.mqtt is not None and self.mqtt._running
                        discord_ok = config.ENABLE_DISCORD and bool(config.DISCORD_WEBHOOK_URL)
                        test_msg = build_test_message(lora_available=lora_ok, mqtt_available=mqtt_ok, discord_available=discord_ok, tag="[[SCH_TX]]")
                        self.send_text(test_msg)
                        self._send_test_discord(tag="[[SCH_TX]]")
                        self._scheduled_sent.add(key)
                        self._last_test_send = now_ts

                typhoon_schedule_hours = {0, 3, 6, 9, 12, 15, 18, 21}
                t = datetime.now()
                today = t.strftime("%Y%m%d")
                if getattr(self, "_typhoon_scheduled_date", "") != today:
                    self._typhoon_scheduled_sent = set()
                    self._typhoon_scheduled_date = today
                typhoon_key = (today, t.hour)
                if t.hour in typhoon_schedule_hours and t.minute == 0 and typhoon_key not in getattr(self, "_typhoon_scheduled_sent", set()):
                    self._send_typhoon_info_if_needed(force=True)
                    self._typhoon_scheduled_sent = getattr(self, "_typhoon_scheduled_sent", set())
                    self._typhoon_scheduled_sent.add(typhoon_key)

            except Exception as e:
                self._ui_log("ERROR", f"Periodic test message error: {e}")

            if now_ts - last_diff_fetch >= 60:
                try:
                    self._update_weather_state_from_r8()
                    last_diff_fetch = now_ts
                except Exception as e:
                    self._ui_log("ERROR", f"Weather R8 update error: {e}")

            try:
                if config.ENABLE_MEGAQUAKE and (now_ts - last_megaquake_check >= 60):
                    self._check_megaquake_feed()
                    last_megaquake_check = now_ts
            except Exception as e:
                self._ui_log("ERROR", f"Megaquake check error: {e}")

            # 河川氾濫警報チェック
            try:
                if (now_ts - last_river_flood_check >= 60):
                    self._check_river_flood_feed()
                    last_river_flood_check = now_ts
            except Exception as e:
                self._ui_log("ERROR", f"River flood check error: {e}")

            # 火山警報チェック
            try:
                if (now_ts - last_volcano_check >= 60):
                    self._check_volcano_feed()
                    last_volcano_check = now_ts
            except Exception as e:
                self._ui_log("ERROR", f"Volcano check error: {e}")
            try:
                if now_ts - last_tsunami_fallback >= config.TSUNAMI_FALLBACK_INTERVAL_SEC:
                    self._check_tsunami_jma_fallback()
                    last_tsunami_fallback = now_ts
            except Exception as e:
                self._ui_log("ERROR", f"Tsunami fallback check error: {e}")

            try:
                if now_ts - last_eew_cleanup >= 3600:
                    _periodic_cleanup_eew_state(self._state)
                    last_eew_cleanup = now_ts
            except Exception as e:
                self._ui_log("ERROR", f"EEW periodic cleanup error: {e}")

            try:
                self._check_memorial()
            except Exception as e:
                self._ui_log("ERROR", f"Memorial check error: {e}")

    def _jalert_polling_loop(self):
        fail_count = 0
        backoff = config.JALERT_POLL_INTERVAL_SEC
        while not self._stop_event.is_set():
            try:
                self._check_and_enqueue_jalert()
                fail_count = 0
                backoff = config.JALERT_POLL_INTERVAL_SEC
            except Exception as e:
                fail_count += 1
                level = "WARN" if fail_count == 1 else "ERROR"
                self._ui_log(level, f"J-ALERT polling error (fail #{fail_count}): {e}")
                backoff = min(60, backoff * 2)
            time.sleep(backoff)

    def _check_tsunami_jma_fallback(self):
        active_grades = store.get_active_tsunami_grades()
        if not active_grades:
            return
        jma_status = _check_tsunami_active_jma(use_json=config.TSUNAMI_USE_JSON_FALLBACK)
        if jma_status is False:
            self.send_text("[[TSUNAMI WR]] <ALL LIFTED>")
            active_en = [_area_en(a) for a in active_grades if active_grades.get(a)]
            if active_en:
                self.send_text("[[TSUNAMI WR]] <Partially LIFTED> " + "|".join(active_en))
            store.clear_active_tsunami()
            self._ui_log("INFO", "Tsunami all lifted by JMA XML fallback check")
        elif jma_status is None:
            self._ui_log("DEBUG", "JMA XML check unavailable, skip force lift")

    def _check_memorial(self):
        try:
            now = datetime.now(); today_key = now.strftime("%Y%m%d")
            if not hasattr(self, "_memorial_date_key") or self._memorial_date_key != today_key:
                self._memorial_sent_today.clear(); self._memorial_date_key = today_key
                self._memorial_data = None

            # 防災の日（毎年9月1日 09:00）
            if now.month == 9 and now.day == 1 and now.hour == 9 and now.minute == 0:
                dp_name = "Disaster Prevention Day"
                if dp_name not in self._memorial_sent_today:
                    dp_msg = (
                        "[[JPEQ]] Today is DISASTER PREVENTION DAY. In addition, the week beginning September 1 has been designated as DISASTER PREVENTION WEEK."
                    )
                    self.send_text(dp_msg)
                    self._send_discord(dp_msg)
                    self._memorial_sent_today.add(dp_name)

            memorial_path = Path(__file__).resolve().parent / "data" / "historical_disasters.json"
            if not memorial_path.exists(): return
            if not hasattr(self, "_memorial_data") or self._memorial_data is None:
                with memorial_path.open("r", encoding="utf-8") as f:
                    self._memorial_data = json.load(f)
            for d in self._memorial_data:
                name = d.get("disaster_name", "Unknown")
                if name in self._memorial_sent_today: continue
                dt_str = d.get("date_time", "")
                m = re.search(r"\d{2,4}-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2})", dt_str)
                if not m: continue
                month, day, hour, minute = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
                if (now.month == month and now.day == day and now.hour == hour and now.minute == minute):
                    mag = d.get("earthquake_magnitude", ""); tsunami = d.get("tsunami_scale", "")
                    casualties = d.get("fatalities_and_missing", ""); characteristics = d.get("characteristics", "")
                    batch = []
                    msg_body = f"[[MEMORIAL]] {name} | {dt_str} | Mag: {mag}"
                    if tsunami and tsunami != "No tsunami": msg_body += f" | Tsunami: {tsunami}"
                    msg_body += f" | Casualties: {casualties}"
                    batch.append(msg_body)
                    if characteristics: batch.append(characteristics)
                    self.send_text_batch(batch)
                    for bm in batch:
                        self._send_discord(bm)
                    self._memorial_sent_today.add(name)
        except Exception as e: self._ui_log("WARN", f"Memorial check error: {e}")

    # ===================================
    # System Lifecycle
    # ===================================
    def force_refresh_history(self):
        with store._lock:
            store.history_cache.clear(); store.tsunami_cache.clear(); store.recent_ids.clear()
        try:
            if store._eq_file.exists(): store._eq_file.unlink()
            if store._tsunami_file.exists(): store._tsunami_file.unlink()
        except Exception as e: self._ui_log("WARN", f"Failed to delete history files: {e}")
        threading.Thread(target=self._fetch_history, daemon=True).start()

    def _recover_unsent_finals(self):
        unsent = _load_unsent_finals()
        if not unsent:
            return

        self._ui_log("INFO", f"Recovering {len(unsent)} unsent FINAL events")

        for event_id, item in unsent.items():
            payload = item.get("payload")
            if not payload:
                _remove_unsent_final(event_id)
                continue

            self._state["is_preliminary"] = False
            self._discord_eq_lines = []

            def eq_discord_send(msg: str):
                self.send_text(msg, urgent=True)
                self._discord_eq_lines.append(msg)
                if msg.startswith("[EQ] Epicenter Map URL "):
                    url = msg.replace("[EQ] Epicenter Map URL ", "", 1).strip()
                    self.earthquake_urls.append(url)

            handle_eq(payload, eq_discord_send, self._state)
            self._state["eew_lock"] = False

            if self._discord_eq_lines:
                self._send_discord("\n".join(self._discord_eq_lines))
                self._discord_eq_lines = []

            _remove_unsent_final(event_id)

    def start(self):
        if self._thread and self._thread.is_alive(): return
        self._stop_event.clear()
        log_dir = Path(config.LOG_FILE_DIR)
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self._ui_log("ERROR", f"Failed to create log directory: {e}")
            raise
        if not TxSystem._log_initialized:
            start_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._log_handlers = []
            formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
            root_logger = logging.getLogger()
            all_path = log_dir / f"jpeq_tx_{start_ts}.log"
            all_fh = SizeRotatingFileHandler(all_path, max_bytes=config.LOG_MAX_FILE_SIZE, delay=False)
            all_fh.setLevel(logging.DEBUG); all_fh.setFormatter(formatter); root_logger.addHandler(all_fh); self._log_handlers.append(all_fh)
            eq_path = log_dir / f"eq_{start_ts}.log"
            eq_fh = SizeRotatingFileHandler(eq_path, max_bytes=config.LOG_MAX_FILE_SIZE, delay=False)
            eq_fh.setLevel(logging.DEBUG); eq_fh.setFormatter(formatter); eq_fh.addFilter(EarthquakeLogFilter()); root_logger.addHandler(eq_fh); self._log_handlers.append(eq_fh)
            tsunami_path = log_dir / f"tsunami_{start_ts}.log"
            tsunami_fh = SizeRotatingFileHandler(tsunami_path, max_bytes=config.LOG_MAX_FILE_SIZE, delay=False)
            tsunami_fh.setLevel(logging.DEBUG); tsunami_fh.setFormatter(formatter); tsunami_fh.addFilter(TsunamiLogFilter()); root_logger.addHandler(tsunami_fh); self._log_handlers.append(tsunami_fh)
            eew_path = log_dir / f"eew_{start_ts}.log"
            eew_fh = SizeRotatingFileHandler(eew_path, max_bytes=config.LOG_MAX_FILE_SIZE, delay=False)
            eew_fh.setLevel(logging.DEBUG); eew_fh.setFormatter(formatter); eew_fh.addFilter(EEWLogFilter()); root_logger.addHandler(eew_fh); self._log_handlers.append(eew_fh)
            weather_path = log_dir / f"weather_{start_ts}.log"
            weather_fh = SizeRotatingFileHandler(weather_path, max_bytes=config.LOG_MAX_FILE_SIZE, delay=False)
            weather_fh.setLevel(logging.DEBUG); weather_fh.setFormatter(formatter); weather_fh.addFilter(WeatherLogFilter()); root_logger.addHandler(weather_fh); self._log_handlers.append(weather_fh)
            typhoon_path = log_dir / f"typhoon_{start_ts}.log"
            typhoon_fh = SizeRotatingFileHandler(typhoon_path, max_bytes=config.LOG_MAX_FILE_SIZE, delay=False)
            typhoon_fh.setLevel(logging.DEBUG); typhoon_fh.setFormatter(formatter); typhoon_fh.addFilter(TyphoonLogFilter()); root_logger.addHandler(typhoon_fh); self._log_handlers.append(typhoon_fh)
            jalert_path = log_dir / f"jalert_{start_ts}.log"
            jalert_fh = SizeRotatingFileHandler(jalert_path, max_bytes=config.LOG_MAX_FILE_SIZE, delay=False)
            jalert_fh.setLevel(logging.DEBUG); jalert_fh.setFormatter(formatter); jalert_fh.addFilter(JAlertLogFilter()); root_logger.addHandler(jalert_fh); self._log_handlers.append(jalert_fh)
            volcano_path = log_dir / f"volcano_{start_ts}.log"
            volcano_fh = SizeRotatingFileHandler(volcano_path, max_bytes=config.LOG_MAX_FILE_SIZE, delay=False)
            volcano_fh.setLevel(logging.DEBUG); volcano_fh.setFormatter(formatter); volcano_fh.addFilter(VolcanoLogFilter()); root_logger.addHandler(volcano_fh); self._log_handlers.append(volcano_fh)
            TxSystem._log_initialized = True
        else:
            self._log_handlers = []
        threading.Thread(target=self._fetch_history, daemon=True).start()
        if self.mqtt: self.mqtt.start()
        threading.Thread(target=self._jalert_polling_loop, daemon=True, name="jalert").start()
        threading.Thread(target=self._periodic_loop, daemon=True, name="periodic").start()
        threading.Thread(target=self._send_worker, daemon=True, name="send_worker").start()
        def _ws_thread_runner():
            self._loop = asyncio.new_event_loop(); asyncio.set_event_loop(self._loop)
            try: self._loop.run_until_complete(self._ws_loop())
            finally: self._loop.close()
        self._thread = threading.Thread(target=_ws_thread_runner, daemon=True, name="ws"); self._thread.start()
        self._ui_log("INFO", "TX system started")
        threading.Thread(target=self._recover_unsent_finals, daemon=True, name="recover_finals").start()
        import atexit
        def _flush_logs():
            for h in logging.getLogger().handlers:
                try:
                    h.flush()
                except Exception:
                    pass
        atexit.register(_flush_logs)

    def stop(self):
        self._stop_event.set()
        timers = self._state.get("eew_throttle_timers", {})
        for t in list(timers.values()):
            try: t.cancel()
            except Exception: pass
        timers.clear()
        root_logger = logging.getLogger()
        for h in self._log_handlers:
            h.close(); root_logger.removeHandler(h)
        self._log_handlers.clear()
        if self.mqtt: self.mqtt.stop()
        if self.lora_iface:
            try: self.lora_iface.close()
            except Exception: pass
        self._ui_log("INFO", "TX system stopped")

# ================================================================
# Tkinter UI (complete, no omissions)
# ================================================================
COLORS = {
    "bg": "#eef1f5", "bg_panel": "#ffffff", "bg_input": "#ffffff",
    "bg_stripe": "#f4f6fa", "bg_hover": "#eef2ff",
    "fg": "#1b1f27", "fg_dim": "#6b7280", "accent": "#3b5bdb",
    "accent_alt": "#e03131", "ok": "#2f9e44", "warn": "#f08c00",
    "err": "#e03131", "rx": "#3b5bdb", "tx": "#2f9e44",
    "border": "#d8dce3",
}
LEVEL_COLOR = {"INFO": COLORS["fg"], "WARN": COLORS["warn"], "ERROR": COLORS["err"], "TX": COLORS["tx"], "RX": COLORS["rx"]}
FONT_BASE = ("Segoe UI", 10)
FONT_SECTION = ("Segoe UI", 10, "bold")
FONT_CARD_TITLE = ("Segoe UI", 11, "bold")
FONT_HEADER = ("Segoe UI", 20, "bold")

class JPEQApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("JPEQ TX v3.1 - Control Panel")
        self.geometry("1024x768")
        try:
            self.state('zoomed')
        except tk.TclError:
            try:
                self.attributes('-zoomed', True)
            except tk.TclError:
                pass
        self.configure(bg=COLORS["bg"])
        self._setup_style()
        self.tx: TxSystem | None = None
        self._log_queue: queue.Queue = queue.Queue()
        self._tx_running = False
        self._tree_sort_state: dict = {}
        self._build_statusbar()
        self._build_layout()
        self._build_tabs()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(150, self._poll_log_queue)
        self.after(2000, self._refresh_history_panel)

    def _setup_style(self):
        s = ttk.Style(self)
        try: s.theme_use("clam")
        except Exception: pass
        s.configure("TFrame", background=COLORS["bg"])
        s.configure("Panel.TFrame", background=COLORS["bg_panel"])
        s.configure("TLabel", background=COLORS["bg"], foreground=COLORS["fg"], font=FONT_BASE)
        s.configure("Panel.TLabel", background=COLORS["bg_panel"], foreground=COLORS["fg"], font=FONT_BASE)
        s.configure("Dim.TLabel", background=COLORS["bg_panel"], foreground=COLORS["fg_dim"], font=FONT_BASE)
        s.configure("Header.TLabel", background=COLORS["bg"], foreground=COLORS["accent"], font=("Segoe UI", 14, "bold"))
        s.configure("TButton", background=COLORS["bg_input"], foreground=COLORS["fg"],
                    borderwidth=1, relief="flat", padding=[10, 5], font=FONT_BASE)
        s.map("TButton",
              background=[("active", COLORS["bg_hover"]), ("disabled", COLORS["bg"])],
              relief=[("pressed", "flat"), ("!pressed", "flat")])
        s.configure("Accent.TButton", background=COLORS["accent"], foreground="white",
                    borderwidth=0, relief="flat", padding=[12, 6], font=("Segoe UI", 10, "bold"))
        s.map("Accent.TButton",
              background=[("active", "#2f4bc7"), ("disabled", "#a9b6ea")])
        s.configure("Danger.TButton", background=COLORS["accent_alt"], foreground="white",
                    borderwidth=0, relief="flat", padding=[12, 6], font=("Segoe UI", 10, "bold"))
        s.map("Danger.TButton",
              background=[("active", "#c92a2a"), ("disabled", "#f0a8a8")])
        s.configure("TNotebook", background=COLORS["bg"], borderwidth=0, tabmargins=[4, 6, 4, 0])
        s.configure("TNotebook.Tab", background=COLORS["bg"], foreground=COLORS["fg_dim"],
                    padding=[16, 9], font=FONT_BASE, borderwidth=0)
        s.map("TNotebook.Tab",
              background=[("selected", COLORS["bg_panel"])],
              foreground=[("selected", COLORS["accent"])],
              font=[("selected", ("Segoe UI", 10, "bold"))])
        s.configure("TEntry", fieldbackground=COLORS["bg_input"], foreground=COLORS["fg"],
                    insertcolor=COLORS["fg"], padding=[6, 5], bordercolor=COLORS["border"],
                    lightcolor=COLORS["border"], darkcolor=COLORS["border"])
        s.map("TEntry", bordercolor=[("focus", COLORS["accent"])])
        s.configure("TCombobox", fieldbackground=COLORS["bg_input"], background=COLORS["bg_input"],
                    foreground="black", padding=[6, 5], arrowsize=14)
        s.map("TCombobox", fieldbackground=[("readonly", COLORS["bg_input"])], foreground=[("readonly", "black")])
        # チェックボックスは主に白いカード内 (_add_checkbutton) で使うため、
        # 既定は bg_panel に合わせる。灰色地に置くもの (Monitor タブのフィルタ行) は
        # OnBg.TCheckbutton を使う。
        s.configure("TCheckbutton", background=COLORS["bg_panel"], foreground=COLORS["fg"], font=FONT_BASE)
        s.map("TCheckbutton", background=[("active", COLORS["bg_panel"])])
        s.configure("OnBg.TCheckbutton", background=COLORS["bg"], foreground=COLORS["fg"], font=FONT_BASE)
        s.map("OnBg.TCheckbutton", background=[("active", COLORS["bg"])])
        s.configure("TPanedwindow", background=COLORS["bg"])
        s.configure("Treeview", background=COLORS["bg_panel"], fieldbackground=COLORS["bg_panel"],
                    foreground=COLORS["fg"], borderwidth=0, rowheight=26, font=FONT_BASE)
        s.configure("Treeview.Heading", background=COLORS["bg_input"], foreground=COLORS["accent"],
                    font=("Segoe UI", 9, "bold"), padding=[6, 6], relief="flat")
        s.map("Treeview.Heading", background=[("active", COLORS["bg_hover"])])
        s.map("Treeview", background=[("selected", COLORS["accent"])], foreground=[("selected", "white")])

    # ------------------------------------------------------------
    # Generic Treeview helpers: click-to-sort headers + row striping
    # ------------------------------------------------------------
    def _bind_sortable(self, tree):
        """Adds click-to-sort behavior to every column heading of a Treeview,
        without disturbing the header text already set by the caller."""
        self._tree_sort_state.setdefault(tree, {})
        for col in tree["columns"]:
            tree.heading(col, command=lambda c=col, t=tree: self._sort_tree(t, c))
        tree.tag_configure("even", background=COLORS["bg_panel"])
        tree.tag_configure("odd", background=COLORS["bg_stripe"])

    def _sort_tree(self, tree, col):
        state = self._tree_sort_state.setdefault(tree, {})
        reverse = not state.get(col, False)
        items = list(tree.get_children(""))

        def _key(iid):
            v = tree.set(iid, col)
            try:
                return (0, float(str(v).replace(",", "").replace("%", "")))
            except (ValueError, TypeError):
                return (1, str(v).lower())

        items.sort(key=_key, reverse=reverse)
        for index, iid in enumerate(items):
            tree.move(iid, "", index)
        state[col] = reverse
        for c in tree["columns"]:
            base = str(tree.heading(c, "text")).rstrip(" \u25b2\u25bc").rstrip()
            suffix = "  \u25bc" if reverse else "  \u25b2"
            tree.heading(c, text=base + (suffix if c == col else ""))
        self._stripe_tree(tree)

    def _stripe_tree(self, tree):
        for i, iid in enumerate(tree.get_children("")):
            existing = tuple(t for t in (tree.item(iid, "tags") or ()) if t not in ("odd", "even"))
            tree.item(iid, tags=existing + (("odd",) if i % 2 else ("even",)))

    def _build_layout(self):
        self.minsize(1100, 720)
        header = tk.Frame(self, bg=COLORS["bg_panel"], height=56)
        header.pack(side=tk.TOP, fill=tk.X)
        header.pack_propagate(False)
        btn_frame = tk.Frame(header, bg=COLORS["bg_panel"])
        btn_frame.pack(side=tk.RIGHT, padx=12, pady=8)
        self.btn_start = ttk.Button(btn_frame, text="\u25b6  START", style="Accent.TButton", command=self._start_tx)
        self.btn_start.pack(side=tk.LEFT, padx=3)
        self.btn_stop = ttk.Button(btn_frame, text="\u25a0  STOP", style="Danger.TButton", command=self._stop_tx)
        self.btn_stop.pack(side=tk.LEFT, padx=3)
        self.btn_stop.state(["disabled"])

        title_box = tk.Frame(header, bg=COLORS["bg_panel"])
        title_box.pack(side=tk.LEFT, padx=5, pady=0)

        # タイトルを左に配置
        tk.Label(title_box, text="JPEQ TX", bg=COLORS["bg_panel"], fg=COLORS["accent"],
                 font=("Segoe UI Black", 32, "bold"),
                 anchor="w", padx=0, pady=0).pack(side=tk.LEFT, anchor="w")

        # サブテキストを右に配置
        subtext_frame = tk.Frame(title_box, bg=COLORS["bg_panel"])
        subtext_frame.pack(side=tk.LEFT, padx=(8, 0), pady=0)

        line1 = "MESHTASTIC ALERTS RELAY SYSTEM"
        tk.Label(subtext_frame, text=line1,
                 bg=COLORS["bg_panel"], fg=COLORS["fg_dim"], font=("Segoe UI", 8),
                 justify=tk.LEFT, anchor="w", padx=0, pady=0).pack(anchor="w")

        line2 = "EEW/Earthquake/Tsunami/Volcano/Weather/Typhoon/Flood/Megaquake/J-Alert"
        tk.Label(subtext_frame, text=line2,
                 bg=COLORS["bg_panel"], fg=COLORS["fg_dim"], font=("Segoe UI", 8),
                 justify=tk.LEFT, anchor="w", padx=0, pady=0).pack(anchor="w")

        line3 = "YAKITAMA Logic Expanded - YNGMT"
        tk.Label(subtext_frame, text=line3,
                 bg=COLORS["bg_panel"], fg=COLORS["fg_dim"], font=("Segoe UI", 8),
                 justify=tk.LEFT, anchor="w", padx=0, pady=0).pack(anchor="w")
        tk.Frame(self, bg=COLORS["border"], height=1).pack(fill=tk.X)
        main = tk.Frame(self, bg=COLORS["bg"])
        main.pack(fill=tk.BOTH, expand=True, padx=10, pady=8)
        self.notebook = ttk.Notebook(main)
        self.notebook.pack(fill=tk.BOTH, expand=True)

    def _build_tabs(self):
        self._build_tab_config(); self._build_tab_send(); self._build_tab_dict(); self._build_tab_monitor()

    # ===================================
    # Tab: Log
    # ===================================
    # このメソッドは削除する。
    # ログ表示機能は _build_tab_monitor 内のログ系子タブへ統合する。

    # ===================================
    # Tab: Config
    # ===================================
    def _build_tab_config(self):
        tab = tk.Frame(self.notebook, bg=COLORS["bg"]); self.notebook.add(tab, text="Config")

        # 3カラムは PanedWindow にして、ユーザーがドラッグで幅を自由に変えられるようにする
        # (固定幅だった旧レイアウトを解消 = 動的レイアウト化)
        columns_frame = ttk.PanedWindow(tab, orient=tk.HORIZONTAL)
        columns_frame.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)

        left_wrapper = tk.Frame(columns_frame, bg=COLORS["bg"])
        columns_frame.add(left_wrapper, weight=1)
        left_canvas = tk.Canvas(left_wrapper, bg=COLORS["bg"], highlightthickness=0)
        left_scrollbar = ttk.Scrollbar(left_wrapper, orient="vertical", command=left_canvas.yview)
        left_frame = tk.Frame(left_canvas, bg=COLORS["bg"])
        left_frame.bind("<Configure>", lambda e: left_canvas.configure(scrollregion=left_canvas.bbox("all")))
        left_canvas.create_window((0, 0), window=left_frame, anchor="nw")
        left_canvas.configure(yscrollcommand=left_scrollbar.set)
        left_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        left_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self._build_card(left_frame, "Communication", self._build_communication_card)

        def _on_mousewheel_left(event): left_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        left_canvas.bind("<Enter>", lambda e: left_canvas.bind_all("<MouseWheel>", _on_mousewheel_left))
        left_canvas.bind("<Leave>", lambda e: left_canvas.unbind_all("<MouseWheel>"))

        center_wrapper = tk.Frame(columns_frame, bg=COLORS["bg"])
        columns_frame.add(center_wrapper, weight=1)
        center_canvas = tk.Canvas(center_wrapper, bg=COLORS["bg"], highlightthickness=0)
        center_scrollbar = ttk.Scrollbar(center_wrapper, orient="vertical", command=center_canvas.yview)
        center_frame = tk.Frame(center_canvas, bg=COLORS["bg"])
        center_frame.bind("<Configure>", lambda e: center_canvas.configure(scrollregion=center_canvas.bbox("all")))
        center_canvas.create_window((0, 0), window=center_frame, anchor="nw")
        center_canvas.configure(yscrollcommand=center_scrollbar.set)
        center_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        center_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self._build_card(center_frame, "Earthquake / Tsunami", self._build_eq_tsunami_card)

        def _on_mousewheel_center(event): center_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        center_canvas.bind("<Enter>", lambda e: center_canvas.bind_all("<MouseWheel>", _on_mousewheel_center))
        center_canvas.bind("<Leave>", lambda e: center_canvas.unbind_all("<MouseWheel>"))

        right_wrapper = tk.Frame(columns_frame, bg=COLORS["bg"])
        columns_frame.add(right_wrapper, weight=1)
        right_canvas = tk.Canvas(right_wrapper, bg=COLORS["bg"], highlightthickness=0)
        right_scrollbar = ttk.Scrollbar(right_wrapper, orient="vertical", command=right_canvas.yview)
        right_frame = tk.Frame(right_canvas, bg=COLORS["bg"])
        right_frame.bind("<Configure>", lambda e: right_canvas.configure(scrollregion=right_canvas.bbox("all")))
        right_canvas.create_window((0, 0), window=right_frame, anchor="nw")
        right_canvas.configure(yscrollcommand=right_scrollbar.set)
        right_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        right_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self._build_card(right_frame, "Weather", self._build_weather_card)

        def _on_mousewheel_right(event): right_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        right_canvas.bind("<Enter>", lambda e: right_canvas.bind_all("<MouseWheel>", _on_mousewheel_right))
        right_canvas.bind("<Leave>", lambda e: right_canvas.unbind_all("<MouseWheel>"))

        apply_frame = tk.Frame(tab, bg=COLORS["bg"])
        apply_frame.pack(fill=tk.X, pady=(8,0))
        ttk.Button(apply_frame, text="Apply Settings", style="Accent.TButton", command=self._apply_config).pack(side=tk.LEFT, padx=4)
        ttk.Button(apply_frame, text="Save Defaults", command=self._save_defaults).pack(side=tk.LEFT, padx=4)
        ttk.Button(apply_frame, text="Reload from defaults", command=self._reload_config).pack(side=tk.LEFT, padx=4)

    def _build_card(self, parent, title, body_builder):
        # 白背景 + 薄い枠線 + 余白で「カード」らしく見せる
        outer = tk.Frame(parent, bg=COLORS["bg"])
        outer.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
        card = tk.Frame(outer, bg=COLORS["bg_panel"], highlightbackground=COLORS["border"],
                         highlightthickness=1, bd=0)
        card.pack(fill=tk.BOTH, expand=True)
        tk.Label(card, text=title, bg=COLORS["bg_panel"], fg=COLORS["accent"], font=FONT_CARD_TITLE,
                 anchor="w").pack(fill=tk.X, padx=14, pady=(12, 6))
        ttk.Separator(card, orient="horizontal").pack(fill=tk.X, padx=14)
        body = tk.Frame(card, bg=COLORS["bg_panel"])
        body.pack(fill=tk.BOTH, expand=True, padx=14, pady=(8, 14))
        body_builder(body)

    def _add_checkbutton(self, parent, text, var, pady=3):
        cb = ttk.Checkbutton(parent, text=text, variable=var)
        cb.pack(anchor="w", pady=pady, fill=tk.X)
        return cb

    def _add_labeled_entry(self, parent, label, var, width=20, pady=3):
        bg = parent.cget("bg")
        row = tk.Frame(parent, bg=bg)
        row.pack(fill=tk.X, pady=pady)
        row.columnconfigure(1, weight=1)
        tk.Label(row, text=label, bg=bg, fg=COLORS["fg"]).grid(row=0, column=0, sticky="w")
        entry = ttk.Entry(row, textvariable=var, width=width)
        # sticky="ew" + column(1) の weight により、親フレームが広がると
        # テキストボックスの幅も追従して伸縮する（従来は固定文字数幅だった）
        entry.grid(row=0, column=1, sticky="ew", padx=(8, 0))
        return entry

    def _add_labeled_combobox(self, parent, label, var, values, width=10, pady=3):
        bg = parent.cget("bg")
        row = tk.Frame(parent, bg=bg)
        row.pack(fill=tk.X, pady=pady)
        row.columnconfigure(1, weight=1)
        tk.Label(row, text=label, bg=bg, fg=COLORS["fg"]).grid(row=0, column=0, sticky="w")
        combo = ttk.Combobox(row, textvariable=var, values=values, state="readonly", width=width)
        combo.grid(row=0, column=1, sticky="ew", padx=(8, 0))
        return combo

    def _build_communication_card(self, body):
        self.var_use_mqtt = tk.BooleanVar(value=config.USE_MQTT)
        self.var_use_lora = tk.BooleanVar(value=config.USE_LORA)
        self.var_use_dummy = tk.BooleanVar(value=config.USE_DUMMY)
        self.var_enable_discord = tk.BooleanVar(value=config.ENABLE_DISCORD)

        # 再起動が必要な設定をまとめる枠
        restart_frame = tk.LabelFrame(
            body,
            text=" Restart Required Settings ",
            bg=COLORS["bg"],
            fg=COLORS["err"],
            font=("Segoe UI", 9, "bold"),
            borderwidth=2,
            relief="groove",
            padx=6,
            pady=6,
        )
        restart_frame.pack(fill=tk.X, pady=(0, 8))

        self.cb_use_mqtt = self._add_checkbutton(restart_frame, "Use MQTT", self.var_use_mqtt)
        self.cb_use_lora = self._add_checkbutton(restart_frame, "Use LoRa", self.var_use_lora)
        self.cb_use_dummy = self._add_checkbutton(restart_frame, "Use Dummy", self.var_use_dummy)
        self.cb_enable_discord = self._add_checkbutton(restart_frame, "Enable Discord", self.var_enable_discord)

        self._updating_send_modes = False

        def _update_send_mode_states(*_args):
            if self._updating_send_modes:
                return
            self._updating_send_modes = True
            try:
                dummy_on = self.var_use_dummy.get()

                if dummy_on:
                    # ダミー有効時は MQTT/LoRa/Discord を強制OFF、グレーアウト
                    if self.var_use_mqtt.get():
                        self.var_use_mqtt.set(False)
                    if self.var_use_lora.get():
                        self.var_use_lora.set(False)
                    if self.var_enable_discord.get():
                        self.var_enable_discord.set(False)

                    self.cb_use_mqtt.state(["disabled"])
                    self.cb_use_lora.state(["disabled"])
                    self.cb_enable_discord.state(["disabled"])
                else:
                    # ダミー無効時は MQTT/LoRa/Discord を再び有効化
                    self.cb_use_mqtt.state(["!disabled"])
                    self.cb_use_lora.state(["!disabled"])
                    self.cb_enable_discord.state(["!disabled"])

                # ダミーモード自体は常に操作可能
                self.cb_use_dummy.state(["!disabled"])
            finally:
                self._updating_send_modes = False

        self.var_use_mqtt.trace_add("write", _update_send_mode_states)
        self.var_use_lora.trace_add("write", _update_send_mode_states)
        self.var_use_dummy.trace_add("write", _update_send_mode_states)
        self.var_enable_discord.trace_add("write", _update_send_mode_states)

        # 初期状態を反映
        _update_send_mode_states()

        self.var_serial_port = tk.StringVar(value=config.SERIAL_PORT)
        self.var_channel_index = tk.StringVar(value=str(config.CHANNEL_INDEX))
        self.var_channel_name = tk.StringVar(value=config.LORA_CHANNEL)
        self.var_channel_psk = tk.StringVar(value=config.LORA_CHANNEL_KEY)
        self.var_node_id = tk.StringVar(value=config.LORA_NODE_ID)
        self.var_mqtt_broker = tk.StringVar(value=config.LORA_MQTT_BROKER)
        self.var_mqtt_port = tk.StringVar(value=str(config.LORA_MQTT_PORT))
        self.var_mqtt_user = tk.StringVar(value=config.LORA_MQTT_USER)
        self.var_discord_webhook = tk.StringVar(value=config.DISCORD_WEBHOOK_URL)
        self.var_split_wait = tk.StringVar(value=str(config.SPLIT_WAIT_SEC))
        self.var_msg_interval = tk.StringVar(value=str(config.MSG_INTERVAL_SEC))
        self.var_loopback_timeout = tk.StringVar(value=str(config.LOOPBACK_TIMEOUT))
        self.var_test_hours = tk.StringVar(value=",".join(str(h) for h in config.SCHEDULED_TEST_HOURS))

        self._add_labeled_entry(restart_frame, "Serial Port:", self.var_serial_port, width=18)
        self._add_labeled_entry(restart_frame, "Channel Index:", self.var_channel_index, width=6)
        self._add_labeled_entry(restart_frame, "Channel Name:", self.var_channel_name, width=18)
        self._add_labeled_entry(restart_frame, "Channel PSK:", self.var_channel_psk, width=18)
        self._add_labeled_entry(restart_frame, "Node ID:", self.var_node_id, width=18)
        self._add_labeled_entry(restart_frame, "MQTT Broker:", self.var_mqtt_broker, width=22)
        self._add_labeled_entry(restart_frame, "MQTT Port:", self.var_mqtt_port, width=6)
        self._add_labeled_entry(restart_frame, "MQTT User:", self.var_mqtt_user, width=18)
        self._add_labeled_entry(restart_frame, "Discord Webhook URL:", self.var_discord_webhook, width=26)

        # 注釈をrestart_frame内のDiscord Webhook URLの下に配置
        tk.Frame(restart_frame, bg=COLORS["bg"], height=8).pack(fill=tk.X)
        note_label = tk.Message(
            restart_frame,
            text="When you change communication settings, you must restart the program (exit completely with the X button and run again).\nUse 'Save Defaults' to keep these settings after restarting.",
            bg=COLORS["bg"],
            fg=COLORS["err"],
            font=("Segoe UI", 8, "bold"),
            justify=tk.LEFT,
            anchor="w",
            aspect=400,
        )
        note_label.pack(fill=tk.X, pady=(0, 4))

        self._add_labeled_entry(body, "Split Wait (s):", self.var_split_wait, width=6)
        self._add_labeled_entry(body, "Msg Interval (s):", self.var_msg_interval, width=6)
        self._add_labeled_entry(body, "Loopback Timeout (s):", self.var_loopback_timeout, width=6)
        self._add_labeled_entry(body, "Scheduled Test Hours:", self.var_test_hours, width=18)

    def _build_eq_tsunami_card(self, body):
        tk.Label(body, text="EEW", bg=COLORS["bg_panel"], fg=COLORS["accent"], font=("Segoe UI",8,"bold")).pack(anchor="w", pady=(2,0))
        self.var_enable_eew = tk.BooleanVar(value=config.ENABLE_EEW)
        self._add_checkbutton(body, "Enable EEW", self.var_enable_eew)
        self.var_eew_min = tk.StringVar(value=config.scale_to_label(config.EEW_MIN_SCALE))
        self._add_labeled_combobox(body, "Min Intensity:", self.var_eew_min, ["1","2","3","4","5-","5+","6-","6+","7"])
        self.var_eew_send_mode = tk.StringVar(value=config.EEW_SEND_MODE)
        self._add_labeled_combobox(body, "Send Mode:", self.var_eew_send_mode, ["all","first_cancel"])
        self.var_eew_map_url_mode = tk.StringVar(value=config.EEW_MAP_URL_MODE)
        self._add_labeled_combobox(body, "Map URL Mode:", self.var_eew_map_url_mode, ["all","first","none"])

        tk.Label(body, text="Earthquake", bg=COLORS["bg_panel"], fg=COLORS["accent"], font=("Segoe UI",8,"bold")).pack(anchor="w", pady=(4,0))
        self.var_enable_eq = tk.BooleanVar(value=config.ENABLE_EQ)
        self._add_checkbutton(body, "Enable Earthquake", self.var_enable_eq)
        self.var_eq_min = tk.StringVar(value=config.scale_to_label(config.MIN_SENDO_SCALE))
        self._add_labeled_combobox(body, "Min Intensity:", self.var_eq_min, ["1","2","3","4","5-","5+","6-","6+","7"])
        self.var_enable_map = tk.BooleanVar(value=config.ENABLE_MAP_URL)
        self._add_checkbutton(body, "Enable Epicenter Map URL", self.var_enable_map)
        self.var_enable_megaquake = tk.BooleanVar(value=config.ENABLE_MEGAQUAKE)
        self._add_checkbutton(body, "Enable Megaquake Info", self.var_enable_megaquake)

        tk.Label(body, text="Tsunami", bg=COLORS["bg_panel"], fg=COLORS["accent"], font=("Segoe UI",8,"bold")).pack(anchor="w", pady=(4,0))
        self.var_enable_tsunami = tk.BooleanVar(value=config.ENABLE_TSUNAMI)
        self._add_checkbutton(body, "Enable Tsunami", self.var_enable_tsunami)
        self.var_tsunami_fallback = tk.StringVar(value=str(config.TSUNAMI_FALLBACK_INTERVAL_SEC))
        self._add_labeled_entry(body, "XML Fallback Interval (sec):", self.var_tsunami_fallback, width=6)
        self.var_tsunami_json = tk.BooleanVar(value=config.TSUNAMI_USE_JSON_FALLBACK)
        self._add_checkbutton(body, "Use VTSE41 JSON as fallback", self.var_tsunami_json)

        tk.Label(body, text="Local Filter", bg=COLORS["bg_panel"], fg=COLORS["accent"], font=("Segoe UI",8,"bold")).pack(anchor="w", pady=(4,0))
        self.var_use_local = tk.BooleanVar(value=config.USE_LOCAL_FILTER)
        self._add_checkbutton(body, "Enable Local Filter", self.var_use_local)
        tk.Label(
            body,
            text="Applies to: Earthquake / EEW / Tsunami / Weather Warnings / River Flood\n* Not applied to Typhoon, J-ALERT, or Megaquake.",
            bg=COLORS["bg_panel"],
            fg=COLORS["fg_dim"],
            font=("Segoe UI", 7),
            justify=tk.LEFT,
            anchor="w",
        ).pack(anchor="w", pady=(2, 0))

        self._region_map = {
            "HOKKAIDO":           ["HOKKAIDO"],
            "TOHOKU":             ["AOMORI","IWATE","MIYAGI","AKITA","YAMAGATA","FUKUSHIMA"],
            "KANTO-KOSHINETSU":   ["IBARAKI","TOCHIGI","GUNMA","SAITAMA","CHIBA","TOKYO","KANAGAWA","YAMANASHI","NAGANO","NIIGATA"],
            "TOKAI":              ["GIFU","SHIZUOKA","AICHI","MIE"],
            "HOKURIKU":           ["TOYAMA","ISHIKAWA","FUKUI"],
            "KINKI":              ["SHIGA","KYOTO","OSAKA","HYOGO","NARA","WAKAYAMA"],
            "CHUGOKU":            ["TOTTORI","SHIMANE","OKAYAMA","HIROSHIMA","YAMAGUCHI"],
            "SHIKOKU":            ["TOKUSHIMA","KAGAWA","EHIME","KOCHI"],
            "KYUSHU":             ["FUKUOKA","SAGA","NAGASAKI","KUMAMOTO","OITA","MIYAZAKI","KAGOSHIMA"],
            "OKINAWA":            ["OKINAWA"],
        }
        self.var_local_region = tk.StringVar(value=config.LOCAL_REGION)
        region_row = tk.Frame(body, bg=COLORS["bg_panel"])
        region_row.pack(fill=tk.X, pady=1)
        tk.Label(region_row, text="Region:", bg=COLORS["bg_panel"], fg=COLORS["fg"]).pack(side=tk.LEFT)
        region_cb = ttk.Combobox(region_row, textvariable=self.var_local_region,
                                 values=list(self._region_map.keys()), state="readonly", width=18)
        region_cb.pack(side=tk.LEFT, padx=2)

        self.var_local_pref = tk.StringVar(value=config.LOCAL_PREF)
        pref_row = tk.Frame(body, bg=COLORS["bg_panel"])
        pref_row.pack(fill=tk.X, pady=1)
        tk.Label(pref_row, text="Prefecture:", bg=COLORS["bg_panel"], fg=COLORS["fg"]).pack(side=tk.LEFT)
        self.pref_cb = ttk.Combobox(pref_row, textvariable=self.var_local_pref,
                                    values=[], state="readonly", width=18)
        self.pref_cb.pack(side=tk.LEFT, padx=2)

        def _on_region_changed(*args):
            region = self.var_local_region.get()
            prefs = self._region_map.get(region, [])
            self.pref_cb["values"] = [""] + prefs
        self.var_local_region.trace_add("write", _on_region_changed)
        if config.LOCAL_REGION:
            _on_region_changed()

        self.var_local_eew_min = tk.StringVar(value=config.scale_to_label(config.LOCAL_EEW_MIN_SCALE))
        self._add_labeled_combobox(body, "Local EEW Min:", self.var_local_eew_min, ["1","2","3","4","5-","5+","6-","6+","7"])
        self.var_local_eq_min = tk.StringVar(value=config.scale_to_label(config.LOCAL_EQ_MIN_SCALE))
        self._add_labeled_combobox(body, "Local EQ Min:", self.var_local_eq_min, ["1","2","3","4","5-","5+","6-","6+","7"])

    def _build_weather_card(self, body):
        self.var_enable_warning = tk.BooleanVar(value=config.ENABLE_WARNING)
        self._add_checkbutton(body, "Enable Weather Warnings", self.var_enable_warning)
        self.var_warning_level = tk.StringVar(value="LEVEL >= 3")
        self._add_labeled_combobox(body, "Warning Min Level:", self.var_warning_level, ["LEVEL >= 3"], width=12)

        tk.Label(body, text="Typhoon", bg=COLORS["bg_panel"], fg=COLORS["accent"], font=("Segoe UI",8,"bold")).pack(anchor="w", pady=(4,0))
        self.var_enable_typhoon = tk.BooleanVar(value=config.ENABLE_TYPHOON_INFO)
        self._add_checkbutton(body, "Enable Typhoon Info", self.var_enable_typhoon)
        self.var_enable_typhoon_map = tk.BooleanVar(value=config.ENABLE_TYPHOON_MAP_URL)
        self._add_checkbutton(body, "Enable Typhoon Map URL", self.var_enable_typhoon_map)

        tk.Label(body, text="Volcano", bg=COLORS["bg_panel"], fg=COLORS["accent"], font=("Segoe UI",8,"bold")).pack(anchor="w", pady=(4,0))
        self.var_enable_volcano = tk.BooleanVar(value=config.ENABLE_VOLCANO)
        self._add_checkbutton(body, "Enable Volcano Info", self.var_enable_volcano)

        tk.Label(body, text="River Flood", bg=COLORS["bg_panel"], fg=COLORS["accent"], font=("Segoe UI",8,"bold")).pack(anchor="w", pady=(4,0))
        self.var_enable_river_flood = tk.BooleanVar(value=config.ENABLE_RIVER_FLOOD)
        self._add_checkbutton(body, "Enable River Flood Info", self.var_enable_river_flood)

        ttk.Separator(body, orient="horizontal").pack(fill=tk.X, pady=(10,5))

        tk.Label(body, text="J-ALERT", bg=COLORS["bg_panel"], fg=COLORS["accent"], font=("Segoe UI",10,"bold")).pack(anchor="w", pady=(6,0))
        self.var_enable_jalert = tk.BooleanVar(value=config.ENABLE_JALERT)
        self._add_checkbutton(body, "Enable J-ALERT (from YAHOO!JAPAN)", self.var_enable_jalert)

        jalert_url_row = tk.Frame(body, bg=COLORS["bg_panel"])
        jalert_url_row.pack(fill=tk.X, pady=1)
        tk.Label(jalert_url_row, text="J-ALERT Test URL:", bg=COLORS["bg_panel"], fg=COLORS["fg"]).pack(side=tk.LEFT)
        self.jalert_test_url_var = tk.StringVar(value="")
        ttk.Entry(jalert_url_row, textvariable=self.jalert_test_url_var, width=34).pack(side=tk.LEFT, padx=2)

    # ===================================
    # Tab: Send
    # ===================================
    def _build_tab_send(self):
        tab = tk.Frame(self.notebook, bg=COLORS["bg"]); self.notebook.add(tab, text="Send")
        quick_card = tk.Frame(tab, bg=COLORS["bg_panel"], highlightbackground=COLORS["border"], highlightthickness=1)
        quick_card.pack(fill=tk.X, padx=8, pady=8)
        tk.Label(quick_card, text="Quick Actions", bg=COLORS["bg_panel"], fg=COLORS["accent"], font=("Segoe UI",10,"bold"), anchor="w").pack(fill=tk.X, padx=10, pady=(8,2))
        btns = tk.Frame(quick_card, bg=COLORS["bg_panel"]); btns.pack(fill=tk.X, padx=10, pady=(0,8))
        ttk.Button(btns, text="Send Test Message", style="Accent.TButton", command=self._send_test).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="Send Earthquake History", style="Accent.TButton", command=self._send_eq_history).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="Send Weather Summary", style="Accent.TButton", command=self._send_weather_summary).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="Send Typhoon Info", style="Accent.TButton", command=self._send_typhoon_info).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="Send J-ALERT Test", style="Accent.TButton", command=self._send_jalert_test).pack(side=tk.LEFT, padx=4)
        custom = tk.Frame(tab, bg=COLORS["bg_panel"], highlightbackground=COLORS["border"], highlightthickness=1)
        custom.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0,8))
        tk.Label(custom, text="Custom Message", bg=COLORS["bg_panel"], fg=COLORS["accent"], font=("Segoe UI",10,"bold"), anchor="w").pack(fill=tk.X, padx=10, pady=(8,2))
        text_frame = tk.Frame(custom, bg=COLORS["bg_panel"])
        text_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=6)
        self.txt_custom = scrolledtext.ScrolledText(text_frame, bg=COLORS["bg_input"], fg=COLORS["fg"], insertbackground=COLORS["fg"],
                                                    font=("Consolas",10), height=6, borderwidth=0, highlightthickness=0)
        self.txt_custom.pack(fill=tk.BOTH, expand=True)
        self.txt_custom.bind("<KeyRelease>", self._update_char_count)
        send_row = tk.Frame(custom, bg=COLORS["bg_panel"]); send_row.pack(fill=tk.X, padx=10, pady=(0,8))
        self.lbl_char_count = tk.Label(send_row, text="Bytes: 0 / 200", bg=COLORS["bg_panel"], fg=COLORS["fg_dim"])
        self.lbl_char_count.pack(side=tk.LEFT)
        ttk.Button(send_row, text="Send", style="Accent.TButton", command=self._send_custom).pack(side=tk.RIGHT)

    def _update_char_count(self, event=None):
        text = self.txt_custom.get("1.0", tk.END).rstrip("\n")
        byte_count = len(text.encode("utf-8"))
        self.lbl_char_count.config(text=f"Bytes: {byte_count} / 200")

    def _force_refresh_history(self):
        if not self.tx: messagebox.showwarning("JPEQ TX", "Start the system first."); return
        self.tx.force_refresh_history()
        self.after(3000, self._refresh_history_panel)

    # ===================================
    # Tab: Dictionary
    # ===================================
    def _build_tab_dict(self):
        tab = tk.Frame(self.notebook, bg=COLORS["bg"]); self.notebook.add(tab, text="Dictionary")
        bar = tk.Frame(tab, bg=COLORS["bg"]); bar.pack(fill=tk.X, padx=8, pady=(6,2))
        tk.Label(bar, text="Dictionary (data/jpeq_dict.json)", bg=COLORS["bg"], fg=COLORS["accent"], font=("Segoe UI",10,"bold")).pack(side=tk.LEFT)
        ttk.Button(bar, text="Reload", command=self._dict_reload).pack(side=tk.RIGHT, padx=2)
        ttk.Button(bar, text="Save to File", style="Accent.TButton", command=self._dict_save).pack(side=tk.RIGHT, padx=2)
        ttk.Button(bar, text="Delete", style="Danger.TButton", command=self._dict_delete).pack(side=tk.RIGHT, padx=2)
        ttk.Button(bar, text="Edit", command=self._dict_edit).pack(side=tk.RIGHT, padx=2)
        ttk.Button(bar, text="Add", command=self._dict_add).pack(side=tk.RIGHT, padx=2)
        ctrl = tk.Frame(tab, bg=COLORS["bg"]); ctrl.pack(fill=tk.X, padx=8, pady=(0,2))
        tk.Label(ctrl, text="Section:", bg=COLORS["bg"], fg=COLORS["fg_dim"]).pack(side=tk.LEFT)
        self._dict_section_var = tk.StringVar(value="local")
        sec_cb = ttk.Combobox(ctrl, textvariable=self._dict_section_var, values=["ALL","local","obs_point","epicenter","sea_area","pref","normalization","river_names","volcano_names","warning_types"], state="readonly", width=14)
        sec_cb.pack(side=tk.LEFT, padx=(4,10)); sec_cb.bind("<<ComboboxSelected>>", lambda _: self._dict_refresh())
        tk.Label(ctrl, text="Filter:", bg=COLORS["bg"], fg=COLORS["fg_dim"]).pack(side=tk.LEFT)
        self._dict_filter_var = tk.StringVar(); self._dict_filter_var.trace_add("write", lambda *_: self._dict_refresh())
        ttk.Entry(ctrl, textvariable=self._dict_filter_var, width=20).pack(side=tk.LEFT, padx=4)
        tk.Label(ctrl, text="(JP or EN)", bg=COLORS["bg"], fg=COLORS["fg_dim"]).pack(side=tk.LEFT)
        wrap = tk.Frame(tab, bg=COLORS["bg_panel"], highlightbackground=COLORS["border"], highlightthickness=1)
        wrap.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0,6))
        cols = ("section","jp","en")
        self._dict_tree = ttk.Treeview(wrap, columns=cols, show="headings")
        self._dict_tree.heading("section", text="Section")
        self._dict_tree.heading("jp", text="Japanese (JP)")
        self._dict_tree.heading("en", text="English / Value (EN)")
        self._dict_tree.column("section", width=120, anchor="w")
        self._dict_tree.column("jp", width=240, anchor="w")
        self._dict_tree.column("en", width=380, anchor="w")
        vsb = ttk.Scrollbar(wrap, orient="vertical", command=self._dict_tree.yview)
        self._dict_tree.configure(yscrollcommand=vsb.set)
        self._dict_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4, pady=4); vsb.pack(side=tk.RIGHT, fill=tk.Y, pady=4)
        self._bind_sortable(self._dict_tree)
        self._dict_tree.bind("<Double-1>", lambda _: self._dict_edit())
        foot = tk.Frame(tab, bg=COLORS["bg"]); foot.pack(fill=tk.X, padx=8, pady=(0,2))
        self._dict_count_lbl = tk.Label(foot, text="", bg=COLORS["bg"], fg=COLORS["fg_dim"]); self._dict_count_lbl.pack(side=tk.LEFT)
        tk.Label(foot, text="* local section only editable", bg=COLORS["bg"], fg=COLORS["fg_dim"]).pack(side=tk.RIGHT)
        self._dict_refresh()

    # ------------------------------------------------------------
    # Monitor Tab (final with blink fixes)
    # ------------------------------------------------------------
    def _add_hist_tab_header(self, parent, key):
        if not hasattr(self, '_last_updated_labels'):
            self._last_updated_labels = {}
        lbl = tk.Label(parent, text="Last updated: -", bg=COLORS["bg_panel"], fg=COLORS["fg_dim"], font=("Segoe UI", 8), anchor="w")
        lbl.pack(fill=tk.X, padx=8, pady=(4, 0))
        self._last_updated_labels[key] = lbl

    def _update_last_updated_label(self, key):
        if hasattr(self, '_last_updated_labels') and key in self._last_updated_labels:
            self._last_updated_labels[key].config(text=f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def _build_tab_monitor(self):
        tab = tk.Frame(self.notebook, bg=COLORS["bg"])
        self.notebook.add(tab, text="Monitor")

        self.monitor_sub_notebook = ttk.Notebook(tab)
        self.monitor_sub_notebook.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        # ===================================
        # Earthquake History
        # ===================================
        eq_hist_tab = tk.Frame(self.monitor_sub_notebook, bg=COLORS["bg_panel"])
        self.monitor_sub_notebook.add(eq_hist_tab, text="Earthquake History")
        self._add_hist_tab_header(eq_hist_tab, "eq")

        eq_cols = ("time", "epicenter", "mag", "intensity", "max_point", "map")
        self.eq_tree = ttk.Treeview(eq_hist_tab, columns=eq_cols, show="headings", height=15)
        for col, w in zip(eq_cols, (140, 260, 60, 70, 220, 240)):
            self.eq_tree.heading(col, text=col.upper())
            self.eq_tree.column(col, width=w, anchor="w")
        self.eq_tree.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
        self._bind_sortable(self.eq_tree)  # 列見出しクリックでソート可能に

        # ===================================
        # Tsunami History
        # ===================================
        ts_hist_tab = tk.Frame(self.monitor_sub_notebook, bg=COLORS["bg_panel"])
        self.monitor_sub_notebook.add(ts_hist_tab, text="Tsunami History")
        self._add_hist_tab_header(ts_hist_tab, "ts")

        ts_cols = ("time", "status", "areas")
        self.ts_tree = ttk.Treeview(ts_hist_tab, columns=ts_cols, show="headings", height=15)
        for col, w in zip(ts_cols, (140, 120, 560)):
            self.ts_tree.heading(col, text=col.upper())
            self.ts_tree.column(col, width=w, anchor="w")
        self.ts_tree.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
        self._bind_sortable(self.ts_tree)

        # ===================================
        # Volcano History
        # ===================================
        volcano_hist_tab = tk.Frame(self.monitor_sub_notebook, bg=COLORS["bg_panel"])
        self.monitor_sub_notebook.add(volcano_hist_tab, text="Volcano History")
        self._add_hist_tab_header(volcano_hist_tab, "volcano")

        volcano_cols = ("volcano", "prefecture", "level", "status", "since")
        self.volcano_tree = ttk.Treeview(volcano_hist_tab, columns=volcano_cols, show="headings", height=15)
        for col, width in zip(volcano_cols, (220, 180, 80, 140, 120)):
            self.volcano_tree.heading(col, text=col.upper())
            self.volcano_tree.column(col, width=width, anchor="w")
        self.volcano_tree.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
        self._bind_sortable(self.volcano_tree)

        # ===================================
        # WX Warning Status
        # ===================================
        warnings_tab = tk.Frame(self.monitor_sub_notebook, bg=COLORS["bg"])
        self.monitor_sub_notebook.add(warnings_tab, text="WX Warning Status")

        filter_bar = tk.Frame(warnings_tab, bg=COLORS["bg"])
        filter_bar.pack(fill=tk.X, padx=4, pady=2)

        self.var_monitor_all = tk.BooleanVar(value=False)
        cb_all = ttk.Checkbutton(filter_bar, text="All Japan", variable=self.var_monitor_all,
                                 command=self._on_monitor_filter_change)
        cb_all.pack(side=tk.LEFT, padx=2)

        self._monitor_region_vars = {}
        self._monitor_region_cbs = {}
        self._monitor_region_map = self._build_region_map_for_monitor()
        for region in self._monitor_region_map:
            var = tk.BooleanVar(value=False)
            cb = ttk.Checkbutton(filter_bar, text=region, variable=var,
                                 command=self._on_monitor_filter_change)
            cb.pack(side=tk.LEFT, padx=2)
            self._monitor_region_vars[region] = var
            self._monitor_region_cbs[region] = cb
        self._update_region_checkboxes_state()

        table_frame = tk.Frame(warnings_tab, bg=COLORS["bg"])
        table_frame.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        cols = ("area", "level", "details")
        self.monitor_tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=25)
        self.monitor_tree.heading("area", text="Area")
        self.monitor_tree.heading("level", text="Level")
        self.monitor_tree.heading("details", text="Details")
        self.monitor_tree.column("area", width=240, anchor="w")
        self.monitor_tree.column("level", width=140, anchor="w")
        self.monitor_tree.column("details", width=480, anchor="w")

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.monitor_tree.yview)
        self.monitor_tree.configure(yscrollcommand=vsb.set)

        self.monitor_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        # Tkinterのタグ優先順位は item の tags タプルの並びではなく、
        # tag_configure を呼んだ順序（先に設定した方が優先）で決まる。
        # レベル色を縞模様(odd/even)より必ず先に設定し、レベル色が縞で
        # 上書きされないようにする。
        self.monitor_tree.tag_configure("level1", background="#FFFFFF", foreground="#808080")
        self.monitor_tree.tag_configure("level2", background="#FFFF00", foreground="#000000")
        self.monitor_tree.tag_configure("level3", background="#FF0000", foreground="#FFFFFF")
        self.monitor_tree.tag_configure("level4", background="#4B0082", foreground="#FFFFFF")
        self.monitor_tree.tag_configure("level5", background="#000000", foreground="#FFFFFF")
        self._bind_sortable(self.monitor_tree)

        # ===================================
        # Typhoon History
        # ===================================
        ty_hist_tab = tk.Frame(self.monitor_sub_notebook, bg=COLORS["bg_panel"])
        self.monitor_sub_notebook.add(ty_hist_tab, text="Typhoon History")
        self._add_hist_tab_header(ty_hist_tab, "ty")

        ty_cols = ("time", "name", "class", "summary", "map")
        self.ty_tree = ttk.Treeview(ty_hist_tab, columns=ty_cols, show="headings", height=15)
        for col, w in zip(ty_cols, (140, 140, 140, 320, 240)):
            self.ty_tree.heading(col, text=col.upper())
            self.ty_tree.column(col, width=w, anchor="w")
        self.ty_tree.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
        self._bind_sortable(self.ty_tree)

        # ===================================
        # Flood History
        # ===================================
        flood_hist_tab = tk.Frame(self.monitor_sub_notebook, bg=COLORS["bg_panel"])
        self.monitor_sub_notebook.add(flood_hist_tab, text="Flood History")
        self._add_hist_tab_header(flood_hist_tab, "river")

        river_cols = ("river", "prefecture", "level", "status", "since")
        self.river_tree = ttk.Treeview(flood_hist_tab, columns=river_cols, show="headings", height=15)
        for col, width in zip(river_cols, (220, 180, 80, 120, 120)):
            self.river_tree.heading(col, text=col.upper())
            self.river_tree.column(col, width=width, anchor="w")
        self.river_tree.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
        self._bind_sortable(self.river_tree)

        # ===================================
        # System Log
        # ===================================
        system_tab = tk.Frame(self.monitor_sub_notebook, bg=COLORS["bg"])
        self.monitor_sub_notebook.add(system_tab, text="System Log")

        self.system_sub_notebook = ttk.Notebook(system_tab)
        self.system_sub_notebook.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)

        self.log_texts = {}
        log_tab_defs = [
            ("Tx Log", "TX", COLORS["tx"]),
            ("Info Log", "INFO", COLORS["fg"]),
            ("Warn Log", "WARN", COLORS["warn"]),
            ("Error Log", "ERROR", COLORS["err"]),
        ]
        for tab_title, level, color in log_tab_defs:
            log_tab = tk.Frame(self.system_sub_notebook, bg=COLORS["bg"])
            self.system_sub_notebook.add(log_tab, text=tab_title)

            bar = tk.Frame(log_tab, bg=COLORS["bg"])
            bar.pack(fill=tk.X, pady=(4, 2))
            ttk.Button(bar, text="Clear", command=lambda w=log_tab: self._clear_log_text(w)).pack(side=tk.RIGHT, padx=4)

            wrap = tk.Frame(log_tab, bg=COLORS["bg_panel"])
            wrap.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

            txt = scrolledtext.ScrolledText(
                wrap,
                bg=COLORS["bg_panel"],
                fg=COLORS["fg"],
                insertbackground=COLORS["fg"],
                font=("Consolas", 9),
                wrap=tk.WORD,
                borderwidth=0,
                highlightthickness=0,
            )
            txt.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
            txt.config(state=tk.DISABLED)
            txt.tag_config(level, foreground=color)
            txt.tag_config("DM", foreground=COLORS["accent"])

            self.log_texts[level] = txt

        resource_tab = tk.Frame(self.system_sub_notebook, bg=COLORS["bg"])
        self.system_sub_notebook.add(resource_tab, text="Resource")

        resource_bar = tk.Frame(resource_tab, bg=COLORS["bg"])
        resource_bar.pack(fill=tk.X, pady=(4, 2))
        tk.Label(resource_bar, text=f"fetch every {config.RESOURCE_MONITOR_INTERVAL_SEC}s",
                 bg=COLORS["bg"], fg=COLORS["fg_dim"], font=("Segoe UI", 8)).pack(side=tk.LEFT, padx=4)
        ttk.Button(resource_bar, text="Clear", command=lambda: self._clear_resource_text()).pack(side=tk.RIGHT, padx=4)

        resource_wrap = tk.Frame(resource_tab, bg=COLORS["bg_panel"])
        resource_wrap.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        self.resource_tree = ttk.Treeview(resource_wrap, columns=("category", "item", "value"), show="headings", height=25)
        self.resource_tree.heading("category", text="Category")
        self.resource_tree.heading("item", text="Item")
        self.resource_tree.heading("value", text="Value")
        self.resource_tree.column("category", width=140, anchor="w")
        self.resource_tree.column("item", width=240, anchor="w")
        self.resource_tree.column("value", width=220, anchor="w")

        resource_vsb = ttk.Scrollbar(resource_wrap, orient="vertical", command=self.resource_tree.yview)
        self.resource_tree.configure(yscrollcommand=resource_vsb.set)

        self.resource_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4, pady=4)
        resource_vsb.pack(side=tk.RIGHT, fill=tk.Y, pady=4)

        # ストライプ用のタグ設定
        self.resource_tree.tag_configure("even", background=COLORS["bg_panel"])
        self.resource_tree.tag_configure("odd", background=COLORS["bg_stripe"])

        # タブ切り替え時に各表示を更新
        self.monitor_sub_notebook.bind("<<NotebookTabChanged>>", self._on_monitor_sub_tab_changed)

    def _build_region_map_for_monitor(self):
        regions = {
            "HOKKAIDO": [], "TOHOKU": [], "KANTO-KOSHIN": [],
            "TOKAI": [], "HOKURIKU": [], "KINKI": [],
            "CHUGOKU": [], "SHIKOKU": [], "KYUSHU": [], "OKINAWA": []
        }
        for code in MONITORED_AREAS:
            if code.startswith("01"):
                regions["HOKKAIDO"].append(code)
            elif code.startswith(("02","03","04","05","06","07")):
                regions["TOHOKU"].append(code)
            elif code.startswith(("08","09","10","11","12","13","14","19","20")):
                regions["KANTO-KOSHIN"].append(code)
            elif code.startswith(("15","16","17","18")):
                regions["HOKURIKU"].append(code)
            elif code.startswith(("21","22","23","24")):
                regions["TOKAI"].append(code)
            elif code.startswith(("25","26","27","28","29","30")):
                regions["KINKI"].append(code)
            elif code.startswith(("31","32","33","34")):
                regions["CHUGOKU"].append(code)
            elif code.startswith(("36","37","38","39")):
                regions["SHIKOKU"].append(code)
            elif code.startswith(("40","41","42","43","44","45","46")):
                regions["KYUSHU"].append(code)
            elif code.startswith("47"):
                regions["OKINAWA"].append(code)
            else:
                regions["KYUSHU"].append(code)
        return regions

    def _on_monitor_filter_change(self):
        self._update_region_checkboxes_state()
        self._refresh_monitor_panel()

    def _update_region_checkboxes_state(self):
        all_checked = self.var_monitor_all.get()
        for cb in self._monitor_region_cbs.values():
            cb.state(["disabled"] if all_checked else ["!disabled"])

    def _get_active_filter_codes(self):
        if self.var_monitor_all.get():
            all_codes = set(MONITORED_AREAS.keys())
        else:
            all_codes = set()
            for region, var in self._monitor_region_vars.items():
                if var.get():
                    all_codes.update(self._monitor_region_map.get(region, []))
        return all_codes

    def _refresh_monitor_panel(self):
        current_tab = self.monitor_sub_notebook.tab(self.monitor_sub_notebook.select(), "text")
        if current_tab != "WX Warning Status":
            self.after(30000, self._refresh_monitor_panel)
            return

        active_codes = self._get_active_filter_codes()
        for iid in self.monitor_tree.get_children():
            self.monitor_tree.delete(iid)

        if not active_codes:
            self.after(30000, self._refresh_monitor_panel)
            return

        if not self.tx or not hasattr(self.tx, '_weather_state'):
            self.after(30000, self._refresh_monitor_panel)
            return

        ws = self.tx._weather_state
        rows = []
        for code in sorted(active_codes):
            state = ws.get(code, {})
            kinds = state.get("kinds", {})
            issued = state.get("issued_times", {})
            level = calc_area_level(kinds)

            area_info = MONITORED_AREAS.get(code, {})
            area_name = area_info.get("en", code)

            max_level = level
            detail_lines = []
            sorted_kinds = sorted(
                kinds.items(),
                key=lambda kv: (-_warning_level(kv[1]), KIND_SORT_PRIORITY.get(kv[0], 99), kv[0])
            )
            for kind_code, jp_name in sorted_kinds:
                lv = _warning_level(jp_name)
                if max_level >= 3 and lv < 3:
                    continue
                en_name = translate_warning_kind_en(jp_name) or jp_name
                since = issued.get(kind_code, "")
                if since:
                    detail_lines.append(f"Lv.{lv} {en_name} since {since}")
                else:
                    detail_lines.append(f"Lv.{lv} {en_name}")
            details = " | ".join(detail_lines) if detail_lines else ""

            level_text = {
                5: "Level 5 EMERGENCY",
                4: "Level 4 URGENT",
                3: "Level 3 WARNING",
                2: "Level 2 ADVISORY",
                1: "Level 1 NO WARNING",
            }.get(level, f"Level {level}")
            rows.append((level, area_name, level_text, details))

        for level, area_name, level_text, details in rows:
            tag = f"level{level}"
            self.monitor_tree.insert("", tk.END, values=(area_name, level_text, details), tags=(tag,))
        self._stripe_tree(self.monitor_tree)

        self.after(30000, self._refresh_monitor_panel)

    def _on_monitor_sub_tab_changed(self, event):
        current_tab = self.monitor_sub_notebook.tab(self.monitor_sub_notebook.select(), "text")
        if current_tab == "WX Warning Status":
            self._refresh_monitor_panel()
        elif current_tab == "Flood History":
            self._refresh_river_flood_panel()
        elif current_tab == "Volcano History":
            self._refresh_volcano_panel()
        elif current_tab == "Earthquake History" or current_tab == "Typhoon History" or current_tab == "Tsunami History":
            self._refresh_history_panel()

    # このメソッドは削除する。
    # リソースログの文字色は常時黒に固定する。

    def _refresh_river_flood_panel(self):
        for iid in self.river_tree.get_children():
            self.river_tree.delete(iid)
        if not self.tx or not hasattr(self.tx, '_river_flood_state'):
            return
        state = self.tx._river_flood_state
        for river_name, info in state.items():
            river_en = info.get("river_en", river_name)
            # 英語名が未設定または日本語のままの場合は辞書から再検索
            if not river_en or river_en == river_name:
                matched_key = _find_river_entry(river_name)
                if matched_key:
                    river_entry = _dict.get("river_names", {}).get(matched_key)
                    if isinstance(river_entry, dict) and river_entry.get("en"):
                        river_en = river_entry["en"]
            pref = info.get("pref", "")
            level = info.get("level", 1)
            level_text = _RIVER_FLOOD_LEVEL_LABEL.get(level, "")
            since = info.get("issued_time", "")
            self.river_tree.insert("", tk.END, values=(river_en or river_name, pref, level, level_text, since))
        self._stripe_tree(self.river_tree)
        self._update_last_updated_label("river")

    def _refresh_volcano_panel(self):
        for iid in self.volcano_tree.get_children():
            self.volcano_tree.delete(iid)
        if not self.tx or not hasattr(self.tx, '_volcano_state'):
            return
        state = self.tx._volcano_state
        for volcano_name, info in state.items():
            volcano_en = info.get("volcano_en", volcano_name)
            pref = info.get("pref", "")
            level = info.get("level", 1)
            level_text = self.tx._volcano_level_label(level) if hasattr(self.tx, "_volcano_level_label") else str(level)
            since = info.get("issued_time", "")
            self.volcano_tree.insert("", tk.END, values=(volcano_en or volcano_name, pref, level, level_text, since))
        self._stripe_tree(self.volcano_tree)
        self._update_last_updated_label("volcano")

    # このメソッドは削除する。
    # URL専用タブを廃止したため、地震・台風履歴内のMap列へ統合する。

    # ===================================
    # Status Bar / Clock
    # ===================================
    def _build_statusbar(self):
        bar = tk.Frame(self, bg=COLORS["bg_panel"], height=24); bar.pack(side=tk.BOTTOM, fill=tk.X); bar.pack_propagate(False)
        self.lbl_status = tk.Label(bar, text="STOPPED", bg=COLORS["bg_panel"], fg=COLORS["err"], font=("Segoe UI",9,"bold"))
        self.lbl_status.pack(side=tk.LEFT, padx=10)
        self.lbl_clock = tk.Label(bar, text="", bg=COLORS["bg_panel"], fg=COLORS["fg_dim"]); self.lbl_clock.pack(side=tk.RIGHT, padx=10)
        self._tick_clock()

    # ===================================
    # Dictionary Tab: CRUD Handlers
    # ===================================
    def _dict_refresh(self):
        sec = self._dict_section_var.get() if hasattr(self, "_dict_section_var") else "local"
        flt = self._dict_filter_var.get().strip().lower() if hasattr(self, "_dict_filter_var") else ""
        for iid in self._dict_tree.get_children(): self._dict_tree.delete(iid)

        if sec == "ALL":
            target_secs = list(_dict.keys())
        else:
            target_secs = [sec]

        count = 0
        total_all = 0
        for target_sec in target_secs:
            data = _dict.get(target_sec, {})
            total_all += len(data)
            for jp, en in sorted(data.items()):
                if isinstance(en, (dict, list)):
                    en_str = json.dumps(en, ensure_ascii=False)
                else:
                    en_str = en if en is not None else "(unregistered)"
                if flt and flt not in jp.lower() and flt not in en_str.lower(): continue
                self._dict_tree.insert("", tk.END, iid=f"{target_sec}:{jp}", values=(target_sec, jp, en_str)); count += 1
        self._stripe_tree(self._dict_tree)

        if sec == "ALL":
            self._dict_count_lbl.config(text=f"Section [ALL]  shown: {count} / total: {total_all}  (overall: {get_dict_size()} entries)")
        else:
            data = _dict.get(sec, {})
            self._dict_count_lbl.config(text=f"Section [{sec}]  shown: {count} / total: {len(data)}  (overall: {get_dict_size()} entries)")

    def _dict_reload(self): _load_dict(); self._dict_refresh()
    def _dict_save(self):
        try: _save_dict(); messagebox.showinfo("Dictionary", f"Saved to:\n{_DICT_PATH}\n\nTotal: {get_dict_size()} entries")
        except Exception as e: messagebox.showerror("Dictionary", f"Save failed:\n{e}")

    def _dict_add(self):
        sec = self._dict_section_var.get()
        if sec == "ALL" or sec not in ("local", "warning_types", "river_names", "volcano_names"):
            messagebox.showinfo("Dictionary", f"Only local / warning_types / river_names / volcano_names section can be added.\nCurrent: {sec}")
            return
        self._dict_entry_dialog(title=f"Add Entry ({sec})", jp_init="", en_init="")

    def _dict_edit(self):
        sel = self._dict_tree.selection()
        if not sel: messagebox.showinfo("Dictionary", "Select an entry to edit."); return
        sec = self._dict_section_var.get()
        if sec == "ALL" or sec not in ("local", "warning_types", "river_names", "volcano_names"):
            messagebox.showinfo("Dictionary", f"Only local / warning_types / river_names / volcano_names section can be edited.\nCurrent: {sec}")
            return
        iid = sel[0]; jp = iid.split(":",1)[1] if ":" in iid else iid
        en = _dict[sec].get(jp, "")
        self._dict_entry_dialog(title=f"Edit Entry ({sec})", jp_init=jp, en_init=en if en is not None else "", old_jp=jp)

    def _dict_delete(self):
        sel = self._dict_tree.selection()
        if not sel: messagebox.showinfo("Dictionary", "Select an entry to delete."); return
        sec = self._dict_section_var.get()
        if sec == "ALL" or sec not in ("local", "warning_types", "river_names", "volcano_names"):
            messagebox.showinfo("Dictionary", f"Only local / warning_types / river_names / volcano_names section can be deleted.\nCurrent: {sec}")
            return
        jps = [iid.split(":",1)[1] if ":" in iid else iid for iid in sel]
        if not messagebox.askyesno("Dictionary", f"Delete {len(jps)} entries?\n\n" + "\n".join(jps[:10]) + ("..." if len(jps)>10 else "")): return
        for jp in jps:
            _dict[sec].pop(jp, None)
            if sec == "warning_types":
                _WARNING_TYPE_EN.pop(jp, None)
        with _dict_lock:
            epicenter_en.cache_clear()
            town_to_romaji.cache_clear()
        self._dict_refresh()

    def _dict_entry_dialog(self, title: str, jp_init: str, en_init: str, old_jp: str | None = None):
        dlg = tk.Toplevel(self); dlg.title(title); dlg.resizable(False, False); dlg.configure(bg=COLORS["bg"]); dlg.grab_set()
        frame = tk.Frame(dlg, bg=COLORS["bg"], padx=16, pady=12); frame.pack()
        sec = self._dict_section_var.get()

        tk.Label(frame, text="Japanese:", bg=COLORS["bg"], fg=COLORS["fg"]).grid(row=0, column=0, sticky="w", pady=4)
        var_jp = tk.StringVar(value=jp_init); e_jp = ttk.Entry(frame, textvariable=var_jp, width=26); e_jp.grid(row=0, column=1, padx=6, pady=4)

        if sec == "river_names":
            en_value = ""
            pref_value = ""
            if isinstance(en_init, dict):
                en_value = en_init.get("en", "")
                pref_value = en_init.get("pref", "")

            tk.Label(frame, text="English (en):", bg=COLORS["bg"], fg=COLORS["fg"]).grid(row=1, column=0, sticky="w", pady=4)
            var_en = tk.StringVar(value=en_value); e_en = ttk.Entry(frame, textvariable=var_en, width=26); e_en.grid(row=1, column=1, padx=6, pady=4)

            tk.Label(frame, text="Prefecture (pref):", bg=COLORS["bg"], fg=COLORS["fg"]).grid(row=2, column=0, sticky="w", pady=4)
            var_pref = tk.StringVar(value=pref_value); e_pref = ttk.Entry(frame, textvariable=var_pref, width=26); e_pref.grid(row=2, column=1, padx=6, pady=4)
        elif sec == "volcano_names":
            en_value = ""
            pref_value = ""
            code_value = ""
            if isinstance(en_init, dict):
                en_value = en_init.get("en", "")
                pref_value = en_init.get("pref", "")
                code_value = str(en_init.get("code", ""))

            tk.Label(frame, text="English (en):", bg=COLORS["bg"], fg=COLORS["fg"]).grid(row=1, column=0, sticky="w", pady=4)
            var_en = tk.StringVar(value=en_value); e_en = ttk.Entry(frame, textvariable=var_en, width=26); e_en.grid(row=1, column=1, padx=6, pady=4)

            tk.Label(frame, text="Prefecture (pref):", bg=COLORS["bg"], fg=COLORS["fg"]).grid(row=2, column=0, sticky="w", pady=4)
            var_pref = tk.StringVar(value=pref_value); e_pref = ttk.Entry(frame, textvariable=var_pref, width=26); e_pref.grid(row=2, column=1, padx=6, pady=4)

            tk.Label(frame, text="Code:", bg=COLORS["bg"], fg=COLORS["fg"]).grid(row=3, column=0, sticky="w", pady=4)
            var_code = tk.StringVar(value=code_value); e_code = ttk.Entry(frame, textvariable=var_code, width=26); e_code.grid(row=3, column=1, padx=6, pady=4)
        else:
            tk.Label(frame, text="English:", bg=COLORS["bg"], fg=COLORS["fg"]).grid(row=1, column=0, sticky="w", pady=4)
            var_en = tk.StringVar(value=en_init if en_init is not None else ""); e_en = ttk.Entry(frame, textvariable=var_en, width=26); e_en.grid(row=1, column=1, padx=6, pady=4)
            tk.Label(frame, text="(empty = null, pending manual entry)", bg=COLORS["bg"], fg=COLORS["fg_dim"]).grid(row=2, column=1, sticky="w")

        def _ok():
            jp = var_jp.get().strip()
            if not jp: messagebox.showwarning("Dictionary", "Japanese field is required.", parent=dlg); return
            if sec == "river_names":
                en = var_en.get().strip()
                pref = var_pref.get().strip()
                value = {"en": en, "pref": pref}
            elif sec == "volcano_names":
                en = var_en.get().strip()
                pref = var_pref.get().strip()
                code_raw = var_code.get().strip()
                try:
                    code = int(code_raw) if code_raw else 0
                except ValueError:
                    messagebox.showwarning("Dictionary", "Code must be an integer.", parent=dlg)
                    return
                value = {"en": en, "pref": pref, "code": code}
            else:
                value = var_en.get().strip() or None

            if old_jp and old_jp != jp:
                _dict[sec].pop(old_jp, None)
                if sec == "warning_types":
                    _WARNING_TYPE_EN.pop(old_jp, None)
            _dict[sec][jp] = value
            if sec == "warning_types" and value is not None:
                _WARNING_TYPE_EN[jp] = value
            with _dict_lock:
                epicenter_en.cache_clear()
                town_to_romaji.cache_clear()
            self._dict_refresh(); dlg.destroy()

        btn_row = tk.Frame(frame, bg=COLORS["bg"]); btn_row.grid(row=4, column=0, columnspan=2, pady=(10,0))
        ttk.Button(btn_row, text="OK", style="Accent.TButton", command=_ok).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_row, text="Cancel", command=dlg.destroy).pack(side=tk.LEFT, padx=4)
        e_jp.focus_set(); dlg.bind("<Return>", lambda _: _ok())

    def _tick_clock(self):
        self.lbl_clock.config(text=datetime.now().strftime("%Y-%m-%d %H:%M:%S")); self.after(1000, self._tick_clock)

    # ===================================
    # TX System Start/Stop Handlers
    # ===================================
    def _start_tx(self):
        if self._tx_running: return
        self._apply_config(silent=True)
        self.tx = TxSystem(log_callback=self._enqueue_log); self.tx.start()
        self._tx_running = True; self.btn_start.state(["disabled"]); self.btn_stop.state(["!disabled"])
        self.lbl_status.config(text="RUNNING", fg=COLORS["ok"])

    def _stop_tx(self):
        if not self._tx_running or not self.tx: return
        self.tx.stop(); self._tx_running = False; self.tx = None
        self.btn_start.state(["!disabled"]); self.btn_stop.state(["disabled"])
        self.lbl_status.config(text="STOPPED", fg=COLORS["err"])

    def _enqueue_log(self, level: str, message: str): self._log_queue.put((level, message))

    def _poll_log_queue(self):
        try:
            while True: level, message = self._log_queue.get_nowait(); self._append_log(level, message)
        except queue.Empty: pass
        self.after(150, self._poll_log_queue)

    def _append_log(self, level: str, message: str):
        if level == "RESOURCE":
            self._append_resource_log(message)
            return

        normalized = "INFO"
        if level == "TX" or level.startswith("DM"):
            normalized = "TX"
        elif level == "WARN":
            normalized = "WARN"
        elif level == "ERROR":
            normalized = "ERROR"
        elif level == "INFO":
            normalized = "INFO"

        txt = self.log_texts.get(normalized)
        if txt is None:
            return

        ts = datetime.now().strftime("%H:%M:%S")
        line = f"{ts}  [{level:<5}]  {message}\n"
        tag = level if level in LEVEL_COLOR else "DM"
        txt.config(state=tk.NORMAL)
        txt.insert(tk.END, line, tag)
        if int(txt.index("end-1c").split(".")[0]) > 1500:
            txt.delete("1.0", "300.0")
        txt.see(tk.END)
        txt.config(state=tk.DISABLED)

    # リソースモニター項目のカテゴリ別定義
    # (カテゴリ名, ((内部キー, 表示名), ...)) の順で並べる
    _RESOURCE_CATEGORIES = (
        ("Hardware", (
            ("cpu", "CPU Usage"),
            ("mem", "Memory Usage"),
            ("load1", "Load Avg (1m)"),
            ("load5", "Load Avg (5m)"),
            ("load15", "Load Avg (15m)"),
        )),
        ("Threads", (
            ("threads", "Active Threads"),
        )),
        ("Send Queue", (
            ("urgent", "Urgent Queue"),
            ("normal", "Normal Queue"),
            ("jalert", "J-ALERT Queue"),
            ("memorial", "Memorial Queue"),
        )),
        ("Tx Statistics", (
            ("tx_total", "Total"),
            ("tx_eew", "EEW"),
            ("tx_eq", "Earthquake"),
            ("tx_tsunami", "Tsunami"),
            ("tx_volcano", "Volcano"),
            ("tx_weather", "Weather Warning"),
            ("tx_river_flood", "River Flood"),
            ("tx_typhoon", "Typhoon"),
            ("tx_jalert", "J-ALERT"),
            ("tx_memorial", "Memorial"),
            ("tx_megaquake", "Megaquake"),
            ("tx_test", "Test"),
            ("tx_other", "Other"),
        )),
        ("Feed Health", (
            ("r8_age", "R8 Feed Age"),
            ("r8_fail", "R8 Feed Fails"),
            ("river_age", "River Feed Age"),
            ("river_fail", "River Feed Fails"),
            ("volcano_age", "Volcano Feed Age"),
            ("volcano_fail", "Volcano Feed Fails"),
            ("typhoon_age", "Typhoon Feed Age"),
            ("typhoon_fail", "Typhoon Feed Fails"),
            ("megaquake_age", "Megaquake Feed Age"),
            ("megaquake_fail", "Megaquake Feed Fails"),
            ("jalert_age", "J-ALERT Feed Age"),
            ("jalert_fail", "J-ALERT Feed Fails"),
        )),
        ("Processed IDs", (
            ("weather", "Weather"),
            ("river", "River"),
            ("volcano", "Volcano"),
            ("jalert", "J-ALERT"),
        )),
    )

    def _append_resource_log(self, message: str):
        if not hasattr(self, "resource_tree"):
            return
        # "Resource monitor: " プレフィックスを除去
        if message.startswith("Resource monitor: "):
            payload = message[len("Resource monitor: "):]
        else:
            payload = message
        # "key=value" を空白区切りで抽出して辞書化
        kv: dict[str, str] = {}
        for token in payload.split():
            if "=" in token:
                k, v = token.split("=", 1)
                kv[k] = v
        # 既存項目を全削除して再挿入（順序を安定させる）
        for iid in self.resource_tree.get_children():
            self.resource_tree.delete(iid)
        row_index = 0
        for cat_idx, (cat_name, items) in enumerate(self._RESOURCE_CATEGORIES):
            if cat_idx > 0:
                self.resource_tree.insert("", tk.END, values=("", "", ""), tags=("separator",))
            for k, label in items:
                v = kv.get(k, "-")
                tag = "odd" if row_index % 2 else "even"
                self.resource_tree.insert("", tk.END, values=(cat_name, label, v), tags=(tag,))
                row_index += 1

    def _clear_resource_text(self):
        if not hasattr(self, "resource_tree"):
            return
        for iid in self.resource_tree.get_children():
            self.resource_tree.delete(iid)

    def _clear_log_text(self, tab):
        for child in tab.winfo_children():
            if isinstance(child, tk.Frame):
                for sub in child.winfo_children():
                    if isinstance(sub, scrolledtext.ScrolledText):
                        sub.config(state=tk.NORMAL)
                        sub.delete("1.0", tk.END)
                        sub.config(state=tk.DISABLED)
                        return

    # ===================================
    # Send Tab: Quick Action Handlers
    # ===================================
    def _send_test(self):
        if not self.tx: messagebox.showwarning("JPEQ TX", "Start the system first."); return
        def _do_test():
            lora_ok = self.tx.lora_iface is not None
            mqtt_ok = self.tx.mqtt is not None and self.tx.mqtt._running
            discord_ok = config.ENABLE_DISCORD and bool(config.DISCORD_WEBHOOK_URL)
            msg = build_test_message(lora_available=lora_ok, mqtt_available=mqtt_ok, discord_available=discord_ok)
            self.tx.send_text(msg)
            self.tx._send_test_discord()
        threading.Thread(target=_do_test, daemon=True).start()

    def _send_eq_history(self):
        if not self.tx: messagebox.showwarning("JPEQ TX", "Start the system first."); return
        def _do_send():
            eq_list = store.list_eq()
            sent = 0
            sent_keys = set()
            for ev in eq_list:
                if sent >= 5:
                    break
                if ev.get("code") == 554:
                    continue
                eq = ev.get("earthquake", {}) or {}
                max_scale = eq.get("maxScale", 0) or 0
                if max_scale < 30:
                    continue
                hypo = eq.get("hypocenter", {}) or {}
                t = eq.get("time", "")
                epi = epicenter_en(hypo.get("name", "")) or hypo.get("name", "Unknown")
                mag_raw = hypo.get("magnitude", "-")
                if not hypo.get("name") or mag_raw in ("", "-", None):
                    continue
                mag = "-"
                try:
                    f = float(mag_raw)
                    if f >= 0:
                        mag = str(int(f)) if f == int(f) else f"{f:.1f}"
                except Exception:
                    pass
                key = f"{t}|{epi}"
                if key in sent_keys:
                    continue
                sent_keys.add(key)
                i = config.scale_to_label(max_scale)
                msg = f"[[EQ HISTORY]] {t} {epi} | M:{mag} | I:{i}"
                self.tx.send_text(msg)
                sent += 1
            if sent == 0:
                self.tx.send_text("[[EQ HISTORY]] No recent earthquakes with intensity >= 3")
        threading.Thread(target=_do_send, daemon=True).start()

    def _send_jalert_test(self):
        if not self.tx:
            messagebox.showwarning("JPEQ TX", "Start the system first.")
            return
        if not config.ENABLE_JALERT:
            messagebox.showwarning("JPEQ TX", "J-ALERT is disabled in Config.")
            return
        def _do_send():
            test_url = self.jalert_test_url_var.get().strip()
            if test_url:
                raw = self.tx._fetch_jalert_test_from_url(test_url)
            else:
                raw = self.tx._fetch_jalert_raw()
            if not raw:
                self.tx._ui_log("WARN", "J-ALERT test: failed to fetch page")
                return
            if "72時間以内に発表されている情報はありません" in raw:
                msg = "<<J-ALERT>> NO INFORMATION HAS BEEN ANNOUNCED WITHIN 72 HOURS."
                with self.tx._jalert_send_lock:
                    self.tx._do_send(msg)
                    self.tx._send_discord(msg)
                self.tx._ui_log("INFO", "J-ALERT test (no info) sent")
                return
            parsed = self.tx._parse_jalert_text(raw)
            if not parsed:
                self.tx._ui_log("WARN", "J-ALERT test: parse failed")
                return
            messages = self.tx._build_jalert_messages(parsed)
            with self.tx._jalert_send_lock:
                for msg in messages:
                    self.tx._do_send(msg)
                    self.tx._send_discord(msg)
                    time.sleep(config.JALERT_MSG_INTERVAL_SEC)
            self.tx._ui_log("INFO", "J-ALERT test package sent")
        threading.Thread(target=_do_send, daemon=True).start()

    def _send_weather_summary(self):
        if not self.tx: messagebox.showwarning("JPEQ TX", "Start the system first."); return
        def _do_send():
            self.tx._send_all_current_warnings_as_summary()
        threading.Thread(target=_do_send, daemon=True).start()

    def _send_typhoon_info(self):
        if not self.tx:
            messagebox.showwarning("JPEQ TX", "Start the system first.")
            return
        threading.Thread(target=self.tx._send_typhoon_info_manual, daemon=True).start()

    def _send_custom(self):
        if not self.tx: messagebox.showwarning("JPEQ TX", "Start the system first."); return
        text = self.txt_custom.get("1.0", tk.END).strip()
        if not text: return
        def _send_all_media():
            self.tx.send_text(text)          # LoRa + MQTT
            self.tx._send_discord(text)      # Discord
        threading.Thread(target=_send_all_media, daemon=True).start()

    # ===================================
    # Config Tab: Apply / Reload / Save Defaults
    # ===================================
    def _save_defaults(self):
        try:
            self._apply_config(silent=True)
            Config.save_defaults()
            messagebox.showinfo("JPEQ TX", "Default settings saved to data/config.json")
        except Exception as e:
            messagebox.showerror("JPEQ TX", f"Failed to save default settings:\n{e}")

    def _apply_config(self, silent: bool = False):
        scale_map = {"1":10,"2":20,"3":30,"4":40,"5-":45,"5+":50,"6-":55,"6+":60,"7":70}
        config.USE_MQTT = self.var_use_mqtt.get(); config.USE_LORA = self.var_use_lora.get(); config.USE_DUMMY = self.var_use_dummy.get()
        config.ENABLE_DISCORD = self.var_enable_discord.get()
        config.ENABLE_EEW = self.var_enable_eew.get(); config.ENABLE_EQ = self.var_enable_eq.get(); config.ENABLE_TSUNAMI = self.var_enable_tsunami.get()
        config.EEW_MIN_SCALE = scale_map.get(self.var_eew_min.get(),30)
        config.EEW_SEND_MODE = self.var_eew_send_mode.get(); config.EEW_MAP_URL_MODE = self.var_eew_map_url_mode.get()
        config.MIN_SENDO_SCALE = scale_map.get(self.var_eq_min.get(),10)
        config.ENABLE_MAP_URL = self.var_enable_map.get()
        config.ENABLE_MEGAQUAKE = self.var_enable_megaquake.get()
        try: config.TSUNAMI_FALLBACK_INTERVAL_SEC = int(self.var_tsunami_fallback.get())
        except Exception: pass
        config.TSUNAMI_USE_JSON_FALLBACK = self.var_tsunami_json.get()
        config.ENABLE_WARNING = self.var_enable_warning.get()
        config.ENABLE_TYPHOON_INFO = self.var_enable_typhoon.get()
        config.ENABLE_TYPHOON_MAP_URL = self.var_enable_typhoon_map.get()
        config.ENABLE_JALERT = self.var_enable_jalert.get()
        config.ENABLE_VOLCANO = self.var_enable_volcano.get()
        config.ENABLE_RIVER_FLOOD = self.var_enable_river_flood.get()
        config.USE_LOCAL_FILTER = self.var_use_local.get(); config.LOCAL_REGION = self.var_local_region.get(); config.LOCAL_PREF = self.var_local_pref.get()
        config.LOCAL_EEW_MIN_SCALE = scale_map.get(self.var_local_eew_min.get(),10)
        config.LOCAL_EQ_MIN_SCALE = scale_map.get(self.var_local_eq_min.get(),10)
        try:
            hours_str = self.var_test_hours.get()
            hours_list = [int(x.strip()) for x in hours_str.split(",") if x.strip().isdigit()]
            if hours_list: config.SCHEDULED_TEST_HOURS = sorted(list(set(hours_list)))
            else: raise ValueError("No valid hours")
        except Exception: pass
        try:
            config.SERIAL_PORT = self.var_serial_port.get()
            config.CHANNEL_INDEX = int(self.var_channel_index.get())
            config.LORA_CHANNEL = self.var_channel_name.get()
            config.LORA_CHANNEL_KEY = self.var_channel_psk.get()
            config.LORA_NODE_ID = self.var_node_id.get()
            config.LORA_MQTT_BROKER = self.var_mqtt_broker.get()
            config.LORA_MQTT_PORT = int(self.var_mqtt_port.get())
            config.LORA_MQTT_USER = self.var_mqtt_user.get()
            config.DISCORD_WEBHOOK_URL = self.var_discord_webhook.get()
            config.SPLIT_WAIT_SEC = float(self.var_split_wait.get())
            config.MSG_INTERVAL_SEC = float(self.var_msg_interval.get())
            config.LOOPBACK_TIMEOUT = float(self.var_loopback_timeout.get())
        except ValueError as e:
            messagebox.showerror("Invalid value", str(e))
            return
        if not silent:
            messagebox.showinfo("JPEQ TX", "Settings applied.\nNote: some changes require restart.")

    def _reload_config(self):
        Config.load_defaults()
        self.var_use_mqtt.set(config.USE_MQTT); self.var_use_lora.set(config.USE_LORA); self.var_use_dummy.set(config.USE_DUMMY)
        self.var_enable_discord.set(config.ENABLE_DISCORD)
        self.var_enable_eew.set(config.ENABLE_EEW); self.var_enable_eq.set(config.ENABLE_EQ); self.var_enable_tsunami.set(config.ENABLE_TSUNAMI)
        self.var_eew_min.set(config.scale_to_label(config.EEW_MIN_SCALE))
        self.var_eew_send_mode.set(config.EEW_SEND_MODE); self.var_eew_map_url_mode.set(config.EEW_MAP_URL_MODE)
        self.var_eq_min.set(config.scale_to_label(config.MIN_SENDO_SCALE))
        self.var_enable_map.set(config.ENABLE_MAP_URL)
        self.var_enable_megaquake.set(config.ENABLE_MEGAQUAKE)
        self.var_tsunami_fallback.set(str(config.TSUNAMI_FALLBACK_INTERVAL_SEC))
        self.var_tsunami_json.set(config.TSUNAMI_USE_JSON_FALLBACK)
        self.var_enable_warning.set(config.ENABLE_WARNING)
        self.var_enable_typhoon.set(config.ENABLE_TYPHOON_INFO)
        self.var_enable_typhoon_map.set(config.ENABLE_TYPHOON_MAP_URL)
        self.var_enable_jalert.set(config.ENABLE_JALERT)
        self.var_enable_volcano.set(config.ENABLE_VOLCANO)
        self.var_enable_river_flood.set(config.ENABLE_RIVER_FLOOD)
        self.var_use_local.set(config.USE_LOCAL_FILTER); self.var_local_region.set(config.LOCAL_REGION); self.var_local_pref.set(config.LOCAL_PREF)
        self.var_local_eew_min.set(config.scale_to_label(config.LOCAL_EEW_MIN_SCALE))
        self.var_local_eq_min.set(config.scale_to_label(config.LOCAL_EQ_MIN_SCALE))
        self.var_test_hours.set(",".join(str(h) for h in config.SCHEDULED_TEST_HOURS))
        self.var_serial_port.set(config.SERIAL_PORT); self.var_channel_index.set(str(config.CHANNEL_INDEX))
        self.var_channel_name.set(config.LORA_CHANNEL); self.var_channel_psk.set(config.LORA_CHANNEL_KEY)
        self.var_node_id.set(config.LORA_NODE_ID); self.var_mqtt_broker.set(config.LORA_MQTT_BROKER)
        self.var_mqtt_port.set(str(config.LORA_MQTT_PORT)); self.var_mqtt_user.set(config.LORA_MQTT_USER)
        self.var_discord_webhook.set(config.DISCORD_WEBHOOK_URL)
        self.var_split_wait.set(str(config.SPLIT_WAIT_SEC)); self.var_msg_interval.set(str(config.MSG_INTERVAL_SEC))
        self.var_loopback_timeout.set(str(config.LOOPBACK_TIMEOUT))

    # ===================================
    # History Panel Refresh & Window Close
    # ===================================
    def _refresh_history_panel(self):
        for iid in self.eq_tree.get_children(): self.eq_tree.delete(iid)
        eq_list = store.list_eq()
        for ev in eq_list[:50]:
            eq = ev.get("earthquake", {}) or {}; hypo = eq.get("hypocenter", {}) or {}
            t = eq.get("time", ""); epi = epicenter_en(hypo.get("name", "")) or "Unknown"
            mag_raw = hypo.get("magnitude", "-"); mag = "-"
            if mag_raw is not None and mag_raw not in ("", "-"):
                try:
                    f = float(mag_raw)
                    if f >= 0:
                        mag = str(int(f)) if f == int(f) else f"{f:.1f}"
                except Exception: pass
            i = config.scale_to_label(eq.get("maxScale",0) or 0)
            max_pt = ev.get("max_point")
            if max_pt and max_pt.get("addr"):
                addr = max_pt.get("addr",""); pref = max_pt.get("pref","")
                town = town_to_romaji(addr, pref) if addr else pref_en(pref)
                pt_scale_label = config.scale_to_label(max_pt.get("scale",0) or 0)
                max_pt_str = f"{town} I:{pt_scale_label}"
            else: max_pt_str = "-"
            map_url = ev.get("map_url", "")
            self.eq_tree.insert("", tk.END, values=(t, epi, mag, i, max_pt_str, map_url))
        self._stripe_tree(self.eq_tree)
        self._update_last_updated_label("eq")

        for iid in self.ty_tree.get_children(): self.ty_tree.delete(iid)
        if self.tx and hasattr(self.tx, "_typhoon_state"):
            ty_list = list(self.tx._typhoon_state.items())
            for row, (num, info) in enumerate(ty_list[:50]):
                t = info.get("last_seen", "")
                name = info.get("name", "")
                clazz = info.get("class", "")
                summary = info.get("summary", "-")
                url = info.get("url", "")
                self.ty_tree.insert("", tk.END, values=(t, name, clazz, summary, url))
        self._stripe_tree(self.ty_tree)
        self._update_last_updated_label("ty")

        for iid in self.ts_tree.get_children(): self.ts_tree.delete(iid)
        ts_list = store.list_tsunami()
        for ev in ts_list[:50]:
            if ev.get("code") != 552: continue
            t = ev.get("time","") or ev.get("issue",{}).get("time","")
            cancelled = ev.get("cancelled", False); areas = ev.get("areas",[]) or []
            grades = {ar.get("grade","") for ar in areas}
            if cancelled: status = "Lifted"
            elif "MajorWarning" in grades: status = "MajorWarning"
            elif "Warning" in grades: status = "Warning"
            else: status = "Advisory"
            area_str = " | ".join(_area_en(ar.get("name","")) for ar in areas[:8])
            self.ts_tree.insert("", tk.END, values=(t, status, area_str))
        self._stripe_tree(self.ts_tree)
        self._update_last_updated_label("ts")
        self.after(30000, self._refresh_history_panel)

    # ブラウザ連携廃止に伴い、_open_eq_map_from_table と _open_ty_map_from_table は削除する。
    # もし _get_eq_url_by_values や _get_ty_url_by_values が残っていれば、それらも不要。

    def _on_close(self):
        if self._tx_running and self.tx: self.tx.stop()
        self.destroy()

def run_ui():
    app = JPEQApp(); app.mainloop()

# ================================================================
# Entry Point
# ================================================================
def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")

def main():
    parser = argparse.ArgumentParser(description="JPEQ TX System (single-file)")
    parser.add_argument("--update-dict", action="store_true")
    parser.add_argument("--no-ui", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    setup_logging(args.verbose)
    Config.load_defaults()
    if args.update_dict:
        update_dict_offline(); return 0
    if not acquire_single_instance_lock():
        msg = ("JPEQ TX is already running (instance lock held).\n"
               "Duplication detected. Terminate existing process first.")
        print(f"[ERROR] {msg}")
        try:
            _root = tk.Tk(); _root.withdraw()
            messagebox.showerror("JPEQ TX", msg)
            _root.destroy()
        except Exception:
            pass
        return 1
    if args.no_ui:
        def _log(level, msg): print(f"[{level}] {msg}")
        tx = TxSystem(log_callback=_log); tx.start()
        try:
            while True: time.sleep(1)
        except KeyboardInterrupt: tx.stop()
        return 0
    run_ui(); return 0

if __name__ == "__main__":
    sys.exit(main())