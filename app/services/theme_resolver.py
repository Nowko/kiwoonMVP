# -*- coding: utf-8 -*-
import json
import os
import re


class ThemeResolver(object):
    DEFAULT_THEME_KEYWORDS = {
        "알루미늄": ["알루미늄", "비철금속", "알루미나"],
        "2차전지": ["2차전지", "이차전지", "배터리", "양극재", "음극재", "전해질", "분리막"],
        "전기차": ["전기차", "ev", "테슬라", "완성차"],
        "반도체": ["반도체", "파운드리", "비메모리", "메모리", "웨이퍼", "후공정"],
        "HBM": ["hbm", "고대역폭메모리"],
        "AI": ["ai", "인공지능", "생성형 ai", "llm", "온디바이스 ai"],
        "로봇": ["로봇", "협동로봇", "자동화", "스마트팩토리"],
        "유리기판": ["유리기판", "glass substrate"],
        "바이오": ["바이오", "신약", "항체", "임상", "치료제"],
        "제약": ["제약", "의약품", "개량신약"],
        "원전": ["원전", "원자력", "smr", "소형모듈원전", "원전해체"],
        "전력설비": ["전력설비", "전력기기", "변압기", "배전", "초고압"],
        "전선": ["전선", "케이블", "해저케이블"],
        "조선": ["조선", "선박", "lng선", "조선기자재"],
        "방산": ["방산", "방위산업", "유도탄", "탄약", "장갑차"],
        "우주항공": ["우주", "항공", "위성", "발사체", "누리호"],
        "드론": ["드론", "무인기", "uam"],
        "자율주행": ["자율주행", "adas", "라이다", "차량용 반도체"],
        "양자": ["양자", "양자암호", "양자컴퓨터"],
        "가상자산": ["비트코인", "가상자산", "암호화폐", "코인", "블록체인"],
        "STO": ["sto", "토큰증권", "증권형 토큰"],
        "리튬": ["리튬", "탄산리튬", "수산화리튬"],
        "폐배터리": ["폐배터리", "배터리 재활용", "리사이클링"],
        "희토류": ["희토류", "니켈", "코발트", "망간"],
        "수소": ["수소", "연료전지", "수전해", "암모니아"],
        "철강": ["철강", "열연", "후판", "스테인리스"],
        "구리": ["구리", "동가격", "전기동"],
        "해운": ["해운", "운임", "벌크선", "컨테이너선"],
        "엔터": ["엔터", "음반", "콘서트", "아티스트", "팬덤"],
        "화장품": ["화장품", "k뷰티", "뷰티"],
        "음식료": ["음식료", "라면", "식품", "k푸드", "음료"],
    }

    DEFAULT_CODE_THEME_MAP = {
        "008350": ["알루미늄"],
        "018470": ["알루미늄"],
    }

    DEFAULT_NAME_THEME_MAP = {
        "남선알미늄": ["알루미늄"],
        "조일알미늄": ["알루미늄"],
    }

    DEFAULT_EVENT_THEME_RULES = [
        {"theme": "원자재 가격 상승", "keywords": ["가격 상승", "원자재", "국제 가격", "급등"]},
        {"theme": "관세 수혜", "keywords": ["관세", "보호무역", "반덤핑", "관세 부과"]},
        {"theme": "정책 수혜", "keywords": ["정책", "지원책", "정부", "예산", "육성"]},
        {"theme": "실적 서프라이즈", "keywords": ["어닝 서프라이즈", "실적 개선", "깜짝 실적", "호실적"]},
        {"theme": "대형 수주", "keywords": ["수주", "공급계약", "계약 체결", "수주 공시"]},
        {"theme": "M&A 모멘텀", "keywords": ["인수", "합병", "지분 취득", "경영권"]},
        {"theme": "승인 모멘텀", "keywords": ["승인", "허가", "인허가", "품목허가"]},
        {"theme": "임상 모멘텀", "keywords": ["임상", "임상 1상", "임상 2상", "임상 3상", "임상시험"]},
        {"theme": "중국 경기부양", "keywords": ["중국", "부양책", "지준율", "금리 인하"]},
        {"theme": "미국 정책 모멘텀", "keywords": ["미국", "백악관", "행정명령", "미 의회", "미국 정부"]},
        {"theme": "AI 투자 확대", "keywords": ["ai 투자", "ai 데이터센터", "생성형 ai", "llm"]},
        {"theme": "전력 인프라 투자", "keywords": ["전력망", "송전", "배전", "변압기", "전력 인프라"]},
        {"theme": "비트코인 강세", "keywords": ["비트코인", "신고가", "현물 etf", "가상자산 강세"]},
        {"theme": "유가 상승", "keywords": ["유가", "wti", "브렌트유", "국제유가"]},
        {"theme": "엔비디아 이벤트", "keywords": ["엔비디아", "nvidia", "gtc"]},
    ]

    def __init__(self, catalog_path=""):
        self.catalog_path = str(catalog_path or "").strip()
        self.theme_keywords = dict(self.DEFAULT_THEME_KEYWORDS)
        self.code_theme_map = dict(self.DEFAULT_CODE_THEME_MAP)
        self.name_theme_map = dict(self.DEFAULT_NAME_THEME_MAP)
        self.event_theme_rules = list(self.DEFAULT_EVENT_THEME_RULES)
        self._load_catalog()

    def _load_catalog(self):
        if (not self.catalog_path) or (not os.path.exists(self.catalog_path)):
            return
        try:
            with open(self.catalog_path, "r", encoding="utf-8") as fp:
                data = json.load(fp)
        except Exception:
            return
        self._merge_mapping(self.theme_keywords, dict(data.get("theme_keywords") or {}))
        self._merge_mapping(self.code_theme_map, dict(data.get("code_theme_map") or {}))
        self._merge_mapping(self.name_theme_map, dict(data.get("name_theme_map") or {}))
        event_rules = list(data.get("event_theme_rules") or [])
        if event_rules:
            self.event_theme_rules = event_rules

    def _merge_mapping(self, target, source):
        for key, values in source.items():
            merged = []
            for value in list(target.get(key, [])) + list(values or []):
                text = str(value or "").strip()
                if text and text not in merged:
                    merged.append(text)
            if merged:
                target[str(key)] = merged

    def _clean_text(self, *parts):
        text = " ".join([str(part or "") for part in parts if part is not None])
        text = re.sub(r"<[^>]+>", " ", text)
        text = text.replace("&nbsp;", " ").replace("&amp;", "&")
        return re.sub(r"\s+", " ", text).strip()

    def _normalize_theme_list(self, values):
        items = []
        for value in list(values or []):
            text = str(value or "").strip()
            if text and text not in items:
                items.append(text)
        return items

    def resolve(self, code="", name="", title="", description="", symbol_meta=None):
        code = str(code or "").strip()
        name = str(name or "").strip()
        symbol_meta = dict(symbol_meta or {})
        text = self._clean_text(name, title, description)
        lowered_text = text.lower()

        static_themes = []
        static_themes.extend(self.code_theme_map.get(code, []))
        static_themes.extend(self.name_theme_map.get(name, []))
        static_themes.extend(symbol_meta.get("themes", []))
        static_themes.extend(symbol_meta.get("theme_tags", []))
        static_themes = self._normalize_theme_list(static_themes)

        dynamic_hits = []
        for theme, keywords in self.theme_keywords.items():
            score = 0
            matched_keywords = []
            for keyword in list(keywords or []):
                token = str(keyword or "").strip()
                if (not token) or (token.lower() not in lowered_text):
                    continue
                matched_keywords.append(token)
                score += 2 if len(token) >= 4 else 1
            if matched_keywords:
                dynamic_hits.append({
                    "theme": theme,
                    "score": score,
                    "keywords": matched_keywords,
                })
        dynamic_hits = sorted(dynamic_hits, key=lambda row: (-int(row.get("score", 0)), str(row.get("theme", ""))))
        dynamic_themes = [row.get("theme", "") for row in dynamic_hits[:3]]
        event_hits = self._resolve_event_themes(lowered_text)
        event_themes = [row.get("theme", "") for row in event_hits[:3]]

        combined = self._normalize_theme_list(static_themes + dynamic_themes)
        primary_theme = combined[0] if combined else ""
        summary = primary_theme
        if len(combined) > 1:
            summary = "{0} 외 {1}".format(primary_theme, len(combined) - 1)
        event_theme = event_themes[0] if event_themes else ""
        event_summary = event_theme
        if len(event_themes) > 1:
            event_summary = "{0} 외 {1}".format(event_theme, len(event_themes) - 1)

        source = []
        if static_themes:
            source.append("dictionary")
        if dynamic_themes:
            source.append("news")
        return {
            "themes": combined[:3],
            "primary_theme": primary_theme,
            "theme_summary": summary,
            "event_themes": event_themes[:3],
            "event_theme": event_theme,
            "event_theme_summary": event_summary,
            "theme_source": "+".join(source),
            "dynamic_hits": dynamic_hits[:3],
            "event_hits": event_hits[:3],
        }

    def _resolve_event_themes(self, lowered_text):
        hits = []
        for row in list(self.event_theme_rules or []):
            theme = str(row.get("theme", "") or "").strip()
            keywords = [str(item or "").strip() for item in list(row.get("keywords") or []) if str(item or "").strip()]
            if (not theme) or (not keywords):
                continue
            matched = []
            score = 0
            for keyword in keywords:
                if keyword.lower() in lowered_text:
                    matched.append(keyword)
                    score += 2 if len(keyword) >= 4 else 1
            if matched:
                hits.append({
                    "theme": theme,
                    "score": score,
                    "keywords": matched,
                })
        return sorted(hits, key=lambda item: (-int(item.get("score", 0)), str(item.get("theme", ""))))
