# -*- coding: utf-8 -*-
import os

from PyQt5.QtCore import QObject, pyqtSignal

from app.services.news_analysis_service import RuleNewsAnalyzer, GPTNewsAnalyzer


class NewsAnalysisManager(QObject):
    log_emitted = pyqtSignal(str)

    def __init__(self, credential_manager=None, parent=None):
        super(NewsAnalysisManager, self).__init__(parent)
        self.credential_manager = credential_manager
        self.rule_analyzer = RuleNewsAnalyzer()
        self.gpt_analyzer = GPTNewsAnalyzer(api_key="", model="gpt-5-mini", model_label="GPT-5-mini 분석")
        if self.credential_manager is not None:
            self.credential_manager.credentials_changed.connect(self.refresh_from_credentials)
        self.refresh_from_credentials()

    def refresh_from_credentials(self):
        cfg = None
        if self.credential_manager is not None:
            for row in self.credential_manager.get_active_ai_apis(include_key=True):
                provider = str(row.get("provider", "") or "").strip().lower()
                if provider == "openai" and str(row.get("api_key", "") or "").strip():
                    cfg = row
                    break
        if cfg is None:
            api_key = os.environ.get("OPENAI_API_KEY", "")
            model = os.environ.get("OPENAI_NEWS_MODEL", "gpt-5-mini")
            model_label = os.environ.get("OPENAI_NEWS_MODEL_LABEL", "")
            base_url = os.environ.get("OPENAI_CHAT_COMPLETIONS_URL", "https://api.openai.com/v1/chat/completions")
            self.gpt_analyzer = GPTNewsAnalyzer(api_key=api_key, model=model, base_url=base_url, model_label=model_label)
            return
        self.gpt_analyzer = GPTNewsAnalyzer(
            api_key=str(cfg.get("api_key", "") or "").strip(),
            model=str(cfg.get("model_name", "gpt-5-mini") or "gpt-5-mini").strip(),
            base_url=str(cfg.get("base_url", "https://api.openai.com/v1/chat/completions") or "https://api.openai.com/v1/chat/completions").strip(),
            model_label=str(cfg.get("analysis_label", "") or "").strip(),
        )

    def analyze_article(self, context, base_result=None, allow_ai=True):
        base_result = dict(base_result or self.rule_analyzer.analyze(context))
        if (not allow_ai) or (not self.gpt_analyzer.enabled):
            return base_result
        if not self.should_use_ai(context, base_result):
            return base_result
        try:
            result = self.gpt_analyzer.analyze(context, base_result)
            self.log_emitted.emit(u"🤖 뉴스 GPT 분석 적용: {0} / {1}".format(context.get("code", ""), self.gpt_analyzer.model))
            return result
        except Exception as exc:
            code = str(context.get("code", "") or "").strip()
            title = str(context.get("title", "") or "").strip().replace("\r", " ").replace("\n", " ")
            if len(title) > 80:
                title = title[:80] + "..."
            self.log_emitted.emit(
                u"⚠️ GPT 뉴스 분석 실패, 프로그램 분석으로 대체: [{0}] {1} / {2}".format(code, title or "-", exc)
            )
            return base_result

    def should_use_ai(self, context, base_result):
        if not self.gpt_analyzer.enabled:
            return False
        duplicate_count = int(context.get("duplicate_count", 1) or 1)
        final_score = float(base_result.get("final_news_score", 0) or 0)
        importance_score = int(base_result.get("importance_score", 0) or 0)
        direction = str(base_result.get("direction", "neutral") or "neutral").strip().lower()
        if duplicate_count >= 3:
            return False
        if direction == "bearish":
            return True
        if importance_score >= 65:
            return True
        if final_score >= 50.0:
            return True
        return False
