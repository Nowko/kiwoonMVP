# -*- coding: utf-8 -*-
import datetime
import io
import json
import os
import zipfile
from xml.etree import ElementTree

import requests


class DartApiService(object):
    CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
    DISCLOSURE_LIST_URL = "https://opendart.fss.or.kr/api/list.json"

    def __init__(self, paths, credential_manager=None, timeout=12):
        self.paths = paths
        self.credential_manager = credential_manager
        self.timeout = int(timeout or 12)
        self._corp_code_cache = None

    @property
    def cache_dir(self):
        path = os.path.join(self.paths.data_dir, "dart")
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    @property
    def corp_code_cache_path(self):
        return os.path.join(self.cache_dir, "corp_codes.json")

    @property
    def enabled(self):
        cfg = self.get_api_config(include_key=True)
        return bool(cfg.get("enabled")) and bool(cfg.get("api_key"))

    def get_api_config(self, include_key=False):
        if self.credential_manager is None:
            return {"api_key": "", "enabled": False}
        return dict(self.credential_manager.get_dart_api(include_key=include_key) or {})

    def get_api_key(self):
        return str(self.get_api_config(include_key=True).get("api_key", "") or "").strip()

    def build_disclosure_url(self, rcp_no):
        rcp_no = str(rcp_no or "").strip()
        if not rcp_no:
            return ""
        return "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={0}".format(rcp_no)

    def refresh_corp_codes(self, force=False, max_age_hours=24):
        max_age_hours = float(max_age_hours or 24.0)
        if (not force) and self._corp_code_cache:
            return dict(self._corp_code_cache)
        if (not force) and os.path.exists(self.corp_code_cache_path):
            try:
                with open(self.corp_code_cache_path, "r", encoding="utf-8") as fp:
                    data = json.load(fp)
                fetched_at = self._parse_ts(data.get("fetched_at"))
                if fetched_at is not None:
                    elapsed = (datetime.datetime.now() - fetched_at).total_seconds()
                    if elapsed <= max_age_hours * 3600:
                        self._corp_code_cache = data
                        return dict(data)
            except Exception:
                pass

        api_key = self.get_api_key()
        if not api_key:
            return {"fetched_at": "", "stocks": {}}
        response = requests.get(
            self.CORP_CODE_URL,
            params={"crtfc_key": api_key},
            timeout=self.timeout,
        )
        response.raise_for_status()
        zf = zipfile.ZipFile(io.BytesIO(response.content))
        xml_names = [name for name in zf.namelist() if name.lower().endswith(".xml")]
        if not xml_names:
            raise RuntimeError("DART corp code XML not found")
        root = ElementTree.fromstring(zf.read(xml_names[0]))
        stocks = {}
        for item in root.findall(".//list"):
            stock_code = str(item.findtext("stock_code", "") or "").strip()
            corp_code = str(item.findtext("corp_code", "") or "").strip()
            corp_name = str(item.findtext("corp_name", "") or "").strip()
            modify_date = str(item.findtext("modify_date", "") or "").strip()
            if not stock_code or not corp_code:
                continue
            stocks[stock_code] = {
                "stock_code": stock_code,
                "corp_code": corp_code,
                "corp_name": corp_name,
                "modify_date": modify_date,
            }
        data = {
            "fetched_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "stocks": stocks,
        }
        with open(self.corp_code_cache_path, "w", encoding="utf-8") as fp:
            json.dump(data, fp, ensure_ascii=False, indent=2)
        self._corp_code_cache = data
        return dict(data)

    def get_corp_info_by_stock_code(self, code, refresh_if_missing=True):
        code = str(code or "").strip()
        if not code:
            return {}
        data = self.refresh_corp_codes(force=False)
        stocks = dict(data.get("stocks") or {})
        info = dict(stocks.get(code) or {})
        if info or (not refresh_if_missing):
            return info
        data = self.refresh_corp_codes(force=True)
        return dict((data.get("stocks") or {}).get(code) or {})

    def fetch_recent_disclosures(self, code, days=180, page_count=100):
        code = str(code or "").strip()
        if not code:
            return []
        api_key = self.get_api_key()
        corp_info = self.get_corp_info_by_stock_code(code, refresh_if_missing=True)
        corp_code = str(corp_info.get("corp_code", "") or "").strip()
        corp_name = str(corp_info.get("corp_name", "") or "").strip()
        if (not api_key) or (not corp_code):
            return []
        end_date = datetime.date.today()
        begin_date = end_date - datetime.timedelta(days=max(1, int(days or 180)))
        page_no = 1
        page_count = max(1, min(100, int(page_count or 100)))
        items = []
        while True:
            response = requests.get(
                self.DISCLOSURE_LIST_URL,
                params={
                    "crtfc_key": api_key,
                    "corp_code": corp_code,
                    "bgn_de": begin_date.strftime("%Y%m%d"),
                    "end_de": end_date.strftime("%Y%m%d"),
                    "page_no": page_no,
                    "page_count": page_count,
                    "last_reprt_at": "Y",
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()
            if str(data.get("status", "") or "") not in ["000", "013"]:
                raise RuntimeError("DART list error: {0} {1}".format(data.get("status", ""), data.get("message", "")))
            page_items = data.get("list") or []
            for row in page_items:
                receipt_no = str(row.get("rcept_no", "") or "").strip()
                report_name = str(row.get("report_nm", "") or "").strip()
                disclosure_date = self._normalize_date(row.get("rcept_dt", ""))
                items.append(
                    {
                        "event_id": "{0}:{1}".format(code, receipt_no or report_name or disclosure_date),
                        "code": code,
                        "corp_code": corp_code,
                        "corp_name": corp_name or str(row.get("corp_name", "") or "").strip(),
                        "disclosure_date": disclosure_date,
                        "receipt_no": receipt_no,
                        "report_name": report_name,
                        "flr_name": str(row.get("flr_nm", "") or "").strip(),
                        "rm": str(row.get("rm", "") or "").strip(),
                        "stock_code": str(row.get("stock_code", code) or code).strip(),
                        "source_url": self.build_disclosure_url(receipt_no),
                        "raw_json": dict(row or {}),
                    }
                )
            total_page = int(data.get("total_page", 1) or 1)
            if page_no >= total_page or not page_items:
                break
            page_no += 1
        items.sort(key=lambda x: (str(x.get("disclosure_date", "") or ""), str(x.get("receipt_no", "") or "")), reverse=True)
        return items

    def _normalize_date(self, value):
        text = str(value or "").strip()
        if len(text) == 8 and text.isdigit():
            return "{0}-{1}-{2}".format(text[:4], text[4:6], text[6:8])
        return text

    def _parse_ts(self, value):
        text = str(value or "").strip()
        if not text:
            return None
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
            try:
                return datetime.datetime.strptime(text, fmt)
            except Exception:
                continue
        return None
