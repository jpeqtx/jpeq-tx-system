#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JPEQ TX WX Database Module (Standalone)
気象庁の全XML電文をJSONに変換・蓄積するデータベース
単独実行: python jpeq_tx_wx_db.py

保存先: data/jpeq_tx_wx_db.json
"""

import json
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import threading

import requests
from bs4 import BeautifulSoup

# xmltodict が未インストールの場合はインストールが必要
try:
    import xmltodict
except ImportError:
    xmltodict = None


class TyphoonScraper:
    """気象庁台風情報ページから発表時刻（Birth）を取得するスクレイパー"""

    TYPHOON_INFO_URL = "https://www.jma.go.jp/bosai/information/typhoon.html"

    @staticmethod
    def fetch_birth_time(typhoon_number: str) -> Optional[str]:
        """指定された台風番号の発表時刻を気象庁ページから取得する。"""
        try:
            response = requests.get(TyphoonScraper.TYPHOON_INFO_URL, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            target_link = None
            for a_tag in soup.find_all("a", href=True):
                if f"typhoon_{typhoon_number}" in a_tag.get("href", ""):
                    target_link = a_tag
                    break

            if target_link is None:
                return None

            detail_href = target_link.get("href")
            if not detail_href.startswith("http"):
                detail_href = "https://www.jma.go.jp" + detail_href

            detail_response = requests.get(detail_href, timeout=10)
            detail_response.raise_for_status()
            detail_soup = BeautifulSoup(detail_response.content, "html.parser")

            return TyphoonScraper._extract_birth_time(detail_soup)

        except Exception:
            return None

    @staticmethod
    def _extract_birth_time(soup: BeautifulSoup) -> Optional[str]:
        for elem in soup.find_all(["th", "td", "div", "span"]):
            text = elem.get_text(strip=True)
            if "発表時刻" in text or "発生日時" in text or "発生" in text:
                parent = elem.find_parent("tr")
                if parent:
                    all_text = parent.get_text(" ", strip=True)
                    match = re.search(r"(\d{4})年(\d{2})月(\d{2})日(\d{2})時(\d{2})分?", all_text)
                    if match:
                        return f"{match.group(1)}/{match.group(2)}/{match.group(3)} {match.group(4)}:{match.group(5) if match.group(5) else '00'}"
                    match = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})\s+(\d{2}):(\d{2})", all_text)
                    if match:
                        return f"{match.group(1)}/{match.group(2)}/{match.group(3)} {match.group(4)}:{match.group(5)}"
        return None


class JMAXMLArchiver:
    """
    気象庁の全XML電文をJSONに変換して蓄積するアーカイバ
    高頻度フィード(extra.xml)を定期的にポーリングし、
    未処理の全XML電文を取得・JSON変換・保存する。
    """

    EXTRA_FEED_URL = "https://www.data.jma.go.jp/developer/xml/feed/extra.xml"
    REGULAR_FEED_URL = "https://www.data.jma.go.jp/developer/xml/feed/regular.xml"
    EXTRA_L_FEED_URL = "https://www.data.jma.go.jp/developer/xml/feed/extra_l.xml"

    def __init__(self, db_path: Optional[Path] = None):
        if db_path is None:
            # プロジェクトルート直下の data フォルダに保存
            project_root = Path(__file__).resolve().parent
            db_path = project_root / "data" / "jpeq_tx_wx_db.json"
        self._db_path = db_path
        self._lock = threading.Lock()
        self._processed_ids: Set[str] = set()
        self._last_fetch_time: Optional[datetime] = None
        self._stop_event = threading.Event()

        # データベースファイルの初期化
        if not self._db_path.exists():
            self._init_db()
        else:
            # 既存の処理済みIDを読み込む
            self._load_processed_ids()

    def _init_db(self):
        """データベースファイルを新規作成する"""
        initial_data = {
            "metadata": {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "version": "2.0",
                "description": "JMA XML Archive - All XML telegrams converted to JSON"
            },
            "entries": []
        }
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._db_path.open("w", encoding="utf-8") as f:
            json.dump(initial_data, f, ensure_ascii=False, indent=2)

    def _load_processed_ids(self):
        """既存のデータベースから処理済みIDを読み込む"""
        try:
            db = self._read_db()
            for entry in db.get("entries", []):
                entry_id = entry.get("entry_id", "")
                if entry_id:
                    self._processed_ids.add(entry_id)
            # 最新の10000件に制限
            if len(self._processed_ids) > 10000:
                self._processed_ids = set(list(self._processed_ids)[-5000:])
        except Exception:
            pass

    def _read_db(self) -> Dict[str, Any]:
        """データベースファイルを読み込む"""
        if not self._db_path.exists():
            self._init_db()
        with self._db_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _write_db(self, data: Dict[str, Any]):
        """データベースファイルに書き込む"""
        temp_path = Path(str(self._db_path) + ".tmp")
        with temp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(temp_path, self._db_path)

    def _get_feed_entries(self, feed_url: str) -> List[Dict[str, Any]]:
        """フィードから全エントリを取得する"""
        try:
            response = requests.get(feed_url, timeout=15, headers={"Cache-Control": "no-cache"})
            if response.status_code != 200:
                print(f"  Failed to fetch feed: {response.status_code}")
                return []
            if xmltodict is not None:
                feed_dict = xmltodict.parse(response.content)
                entries = feed_dict.get("feed", {}).get("entry", [])
                if isinstance(entries, dict):
                    entries = [entries]
                return entries
            else:
                return self._parse_feed_entries_raw(response.content)
        except Exception as e:
            print(f"  Error fetching feed: {e}")
            return []

    def _parse_feed_entries_raw(self, content: bytes) -> List[Dict[str, Any]]:
        """xmltodictがない場合のフィード解析"""
        import xml.etree.ElementTree as ET
        root = ET.fromstring(content)
        ATOM_NS = "http://www.w3.org/2005/Atom"
        entries = []
        for entry in root.findall(f"{{{ATOM_NS}}}entry"):
            entry_dict = {}
            for child in entry:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if tag == "link":
                    entry_dict["link"] = child.get("href", "")
                else:
                    entry_dict[tag] = child.text or ""
            entries.append(entry_dict)
        return entries

    def _fetch_xml_content(self, url: str) -> Optional[Dict[str, Any]]:
        """指定されたURLからXMLを取得し、JSONに変換する"""
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                return None
            if xmltodict is not None:
                return xmltodict.parse(response.content)
            else:
                return {"raw_xml": response.text}
        except Exception:
            return None

    def archive_new_entries(self) -> int:
        """
        フィードから未処理の全エントリを取得し、JSONに変換して保存する。
        戻り値: 新規にアーカイブしたエントリ数
        """
        with self._lock:
            db = self._read_db()
            new_count = 0
            now = datetime.now(timezone.utc)

            # 高頻度フィードと長期フィードの両方をチェック
            for feed_url in [self.EXTRA_FEED_URL, self.EXTRA_L_FEED_URL]:
                print(f"  Checking feed: {feed_url}")
                entries = self._get_feed_entries(feed_url)
                print(f"  Found {len(entries)} entries in feed")

                for entry in entries:
                    entry_id = entry.get("id", "")
                    if not entry_id:
                        continue

                    if entry_id in self._processed_ids:
                        continue

                    link = entry.get("link", "")
                    if isinstance(link, dict):
                        link = link.get("@href", "")
                    if not link:
                        continue

                    print(f"  Fetching: {link}")
                    xml_json = self._fetch_xml_content(link)
                    if xml_json is None:
                        continue

                    record = {
                        "entry_id": entry_id,
                        "title": entry.get("title", ""),
                        "updated": entry.get("updated", ""),
                        "published": entry.get("published", ""),
                        "link": link,
                        "content": xml_json,
                        "archived_at": now.isoformat()
                    }

                    db["entries"].append(record)
                    self._processed_ids.add(entry_id)
                    new_count += 1
                    print(f"  Archived: {entry.get('title', 'Unknown')[:60]}")

                if len(self._processed_ids) > 10000:
                    self._processed_ids = set(list(self._processed_ids)[-5000:])

            if new_count > 0:
                self._write_db(db)
                self._last_fetch_time = now
                print(f"  Total new entries archived: {new_count}")

            return new_count

    def get_stats(self) -> Dict[str, Any]:
        """データベースの統計情報を取得する"""
        db = self._read_db()
        entries = db.get("entries", [])
        total_entries = len(entries)
        total_size = self._db_path.stat().st_size if self._db_path.exists() else 0

        type_counts = {}
        for entry in entries:
            title = entry.get("title", "")
            match = re.search(r"^(.*?)(\d{4})", title)
            if match:
                etype = match.group(1).strip()
                type_counts[etype] = type_counts.get(etype, 0) + 1

        return {
            "total_entries": total_entries,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "type_counts": type_counts,
            "last_fetch_time": self._last_fetch_time.isoformat() if self._last_fetch_time else None
        }

    def stop(self):
        """アーカイバを停止する"""
        self._stop_event.set()

    def run_forever(self, interval_sec: int = 60):
        """定期的にアーカイブを実行し続ける"""
        print(f"JMA XML Archiver started. Polling every {interval_sec} seconds.")
        print(f"Database: {self._db_path}")
        print(f"Press Ctrl+C to stop.\n")

        while not self._stop_event.is_set():
            try:
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{now_str}] Fetching new entries...")
                new_count = self.archive_new_entries()
                stats = self.get_stats()
                print(f"[{now_str}] Done. Total entries: {stats['total_entries']}, "
                      f"DB size: {stats['total_size_mb']}MB")
                print()
            except Exception as e:
                print(f"Error during archive cycle: {e}")

            # インターバル待機（停止イベントをチェックしながら）
            for _ in range(interval_sec):
                if self._stop_event.is_set():
                    break
                time.sleep(1)

        print("Archiver stopped.")


class WXDatabase(JMAXMLArchiver):
    """
    気象・台風情報の履歴データベース
    JMAXMLArchiver を継承し、全XMLのアーカイブ機能に加えて
    気象警報・台風情報の専用レコード追加機能を提供する。
    """

    def __init__(self, db_path: Optional[Path] = None):
        super().__init__(db_path)
        self._weather_count = 0
        self._typhoon_count = 0

    def add_weather_record(self, area_code: str, area_name: str, warnings: Dict[str, str],
                           levels: Dict[str, int], status: str) -> None:
        """気象警報レコードを追加する"""
        record = {
            "type": "weather",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "area_code": area_code,
            "area_name": area_name,
            "warnings": warnings,
            "levels": levels,
            "status": status
        }
        with self._lock:
            db = self._read_db()
            if "weather_records" not in db:
                db["weather_records"] = []
            db["weather_records"].append(record)
            self._weather_count += 1
            self._write_db(db)

    def add_typhoon_record(self, number: str, name: str, typhoon_class: str,
                           position: Dict[str, float], pressure: int,
                           max_wind: int, storm_radius: Optional[int],
                           gale_radius: Optional[int],
                           movement: Dict[str, Any],
                           birth: Optional[str] = None) -> None:
        """台風情報レコードを追加する"""
        record = {
            "type": "typhoon",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "number": number,
            "name": name,
            "class": typhoon_class,
            "position": position,
            "pressure": pressure,
            "max_wind": max_wind,
            "storm_radius": storm_radius,
            "gale_radius": gale_radius,
            "movement": movement,
            "birth": birth
        }
        with self._lock:
            db = self._read_db()
            if "typhoon_records" not in db:
                db["typhoon_records"] = []
            db["typhoon_records"].append(record)
            self._typhoon_count += 1
            self._write_db(db)

    def get_typhoon_birth_time(self, typhoon_number: str) -> Optional[str]:
        """台風の発生日時をスクレイピングで取得する"""
        return TyphoonScraper.fetch_birth_time(typhoon_number)

    def get_weather_history(self, area_code: Optional[str] = None,
                            limit: int = 100) -> List[Dict[str, Any]]:
        """気象警報の履歴を取得する"""
        db = self._read_db()
        records = db.get("weather_records", [])
        if area_code:
            records = [r for r in records if r.get("area_code") == area_code]
        return records[-limit:]

    def get_typhoon_history(self, number: Optional[str] = None,
                            limit: int = 100) -> List[Dict[str, Any]]:
        """台風情報の履歴を取得する"""
        db = self._read_db()
        records = db.get("typhoon_records", [])
        if number:
            records = [r for r in records if r.get("number") == number]
        return records[-limit:]

    def get_latest_weather_state(self) -> Dict[str, Dict[str, Any]]:
        """最新の気象警報状態を取得する（システム復旧用）"""
        db = self._read_db()
        records = db.get("weather_records", [])
        latest_state = {}
        for record in records:
            area_code = record.get("area_code")
            if area_code:
                latest_state[area_code] = record
        return latest_state

    def get_latest_typhoon_state(self) -> Dict[str, Dict[str, Any]]:
        """最新の台風状態を取得する（システム復旧用）"""
        db = self._read_db()
        records = db.get("typhoon_records", [])
        latest_state = {}
        for record in records:
            number = record.get("number")
            if number:
                latest_state[number] = record
        return latest_state

    def get_full_stats(self) -> Dict[str, Any]:
        """全統計情報を取得する"""
        stats = self.get_stats()
        db = self._read_db()
        stats["weather_records"] = len(db.get("weather_records", []))
        stats["typhoon_records"] = len(db.get("typhoon_records", []))
        stats["total_records"] = stats["weather_records"] + stats["typhoon_records"]
        return stats


# ================================================================
# 単独実行用エントリポイント
# ================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("JPEQ TX WX Database - JMA XML Archiver")
    print("=" * 60)
    print()

    # アーカイバの初期化（デフォルトパスを使用：data/jpeq_tx_wx_db.json）
    archiver = JMAXMLArchiver()

    # 統計情報の表示
    stats = archiver.get_stats()
    print(f"Database: {archiver._db_path}")
    print(f"Existing entries: {stats['total_entries']}")
    print(f"Database size: {stats['total_size_mb']}MB")
    print()

    # シグナルハンドラ（Ctrl+C で安全に停止）
    def signal_handler(signum, frame):
        print("\nStopping archiver...")
        archiver.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 永続実行（60秒間隔でポーリング）
    archiver.run_forever(interval_sec=60)