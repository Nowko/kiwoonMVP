# -*- coding: utf-8 -*-
import datetime
import json
import uuid

from PyQt5.QtCore import QObject, pyqtSignal


BUY_STRATEGY_TYPES = [
    ("institution_trend_a", "기관 수급 1"),
    ("institution_trend_b", "기관 수급 2"),
    ("institution_trend_c", "기관 수급 3"),
    ("foreign_trend_a", "외국인 수급 1"),
    ("foreign_trend_b", "외국인 수급 2"),
    ("foreign_trend_c", "외국인 수급 3"),
    ("vwap", "VWAP"),
    ("vwap_b", "VWAP 2"),
    ("sell_pressure_a", "매도우위 1"),
    ("sell_pressure_b", "매도우위 2"),
    ("buy_pressure_a", "매수우위 1"),
    ("buy_pressure_b", "매수우위 2"),
    ("news_filter", "뉴스 필터"),
    ("news_trade", "뉴스 매매"),
]

SELL_STRATEGY_TYPES = [
    ("stop_loss", "손절"),
    ("take_profit", "익절"),
    ("trailing_stop", "트레일링 스탑"),
    ("time_exit", "보유시간 청산"),
    ("market_close_exit", "장마감 강제청산"),
]


class StrategyManager(QObject):
    strategies_changed = pyqtSignal()
    log_emitted = pyqtSignal(str)

    def __init__(self, persistence, realtime_market_state_manager=None, parent=None):
        super(StrategyManager, self).__init__(parent)
        self.persistence = persistence
        self.realtime_market_state_manager = realtime_market_state_manager
        self._seed_default_strategies()

    def _default_params_for_type(self, strategy_type):
        if strategy_type == "news_filter":
            return {"min_score": 60}
        if strategy_type == "news_trade":
            return {"min_score": 80}
        if strategy_type == "institution_trend_a":
            return {"min_net_buy_amount": 0.0}
        if strategy_type == "institution_trend_b":
            return {"min_net_buy_ratio_pct": 1.0}
        if strategy_type == "institution_trend_c":
            return {"streak_count": 2, "interval_type": "day"}
        if strategy_type == "foreign_trend_a":
            return {"min_net_buy_amount": 0.0}
        if strategy_type == "foreign_trend_b":
            return {"min_net_buy_ratio_pct": 0.5}
        if strategy_type == "foreign_trend_c":
            return {"streak_count": 2, "interval_type": "60m"}
        if strategy_type == "vwap":
            return {"comparison": "above_or_equal"}
        if strategy_type == "vwap_b":
            return {"comparison": "below"}
        if strategy_type == "sell_pressure_a":
            return {"max_ratio": 2.0}
        if strategy_type == "sell_pressure_b":
            return {"min_ratio": 1.5}
        if strategy_type == "sell_pressure_c":
            return {"max_ratio": 1.2}
        if strategy_type == "sell_pressure_d":
            return {"max_ratio": 1.0}
        if strategy_type == "buy_pressure_a":
            return {"min_buy_ratio": 1.25}
        if strategy_type == "buy_pressure_b":
            return {"max_buy_ratio": 1.5}
        if strategy_type == "buy_pressure_c":
            return {"min_buy_ratio": 2.0}
        if strategy_type == "stop_loss":
            return {"stop_loss_pct": -3.0}
        if strategy_type == "take_profit":
            return {"take_profit_pct": 5.0}
        if strategy_type == "trailing_stop":
            return {"trail_start_pct": 3.0, "trail_gap_pct": 1.5}
        if strategy_type == "time_exit":
            return {"hold_minutes": 30}
        if strategy_type == "market_close_exit":
            return {"exit_hhmm": "1520"}
        return {}

    def _seed_default_strategies(self):
        existing = self.persistence.fetchall(
            "SELECT strategy_kind, strategy_type FROM strategy_definitions"
        )
        existing_pairs = set((str(row["strategy_kind"] or ""), str(row["strategy_type"] or "")) for row in existing)
        changed = False
        for kind, pairs in [("buy", BUY_STRATEGY_TYPES), ("sell", SELL_STRATEGY_TYPES)]:
            for strategy_type, strategy_name in pairs:
                if (kind, strategy_type) in existing_pairs:
                    continue
                self.add_strategy(kind, strategy_type, strategy_name, params=self._default_params_for_type(strategy_type), emit_signal=False)
                existing_pairs.add((kind, strategy_type))
                changed = True
        if changed:
            self.strategies_changed.emit()

    def get_strategy_type_pairs(self, kind):
        return BUY_STRATEGY_TYPES if kind == "buy" else SELL_STRATEGY_TYPES


    def get_realtime_market_snapshot(self, code):
        if self.realtime_market_state_manager is None:
            return {}
        return self.realtime_market_state_manager.get_snapshot(code)

    def add_strategy(self, kind, strategy_type, strategy_name, params=None, scope_type="global", emit_signal=True):
        params = params or {}
        strategy_id = "{0}_{1}".format(kind, uuid.uuid4().hex[:10])
        now = self.persistence.now_ts()
        strategy_no = self._allocate_strategy_no(kind)
        is_news_filter = 1 if strategy_type == "news_filter" else 0
        is_news_trade = 1 if strategy_type == "news_trade" else 0
        is_assignable_to_slot = 0 if strategy_type == "news_trade" else 1
        self.persistence.execute(
            """
            INSERT INTO strategy_definitions (
                strategy_id, strategy_kind, strategy_type, strategy_name, strategy_no, enabled,
                scope_type, scope_targets_json, params_json, extra_json,
                is_assignable_to_slot, is_news_filter, is_news_trade, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 1, ?, '[]', ?, '{}', ?, ?, ?, ?, ?)
            """,
            (
                strategy_id, kind, strategy_type, strategy_name, strategy_no,
                scope_type, json.dumps(params, ensure_ascii=False),
                is_assignable_to_slot, is_news_filter, is_news_trade, now, now,
            ),
        )
        order_row = self.persistence.fetchone(
            "SELECT COALESCE(MAX(chain_order), 0) AS max_order FROM strategy_chain_items WHERE chain_name=?",
            (kind,),
        )
        order_no = int(order_row["max_order"] or 0) + 1
        self.persistence.execute(
            """
            INSERT INTO strategy_chain_items (
                chain_item_id, strategy_id, chain_name, chain_order, enabled, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 1, ?, ?)
            """,
            ("chain_" + uuid.uuid4().hex[:10], strategy_id, kind, order_no, now, now),
        )
        self.log_emitted.emit("➕ 전략 추가: {0} / {1}".format(kind, strategy_name))
        if emit_signal:
            self.strategies_changed.emit()
        return strategy_id

    def get_chain_items(self, kind):
        sql = """
        SELECT c.chain_item_id, c.chain_order, c.enabled AS chain_enabled,
               s.strategy_id, s.strategy_kind, s.strategy_type, s.strategy_name, s.strategy_no,
               s.enabled AS strategy_enabled, s.scope_type, s.scope_targets_json, s.params_json,
               s.is_assignable_to_slot, s.is_news_filter, s.is_news_trade
        FROM strategy_chain_items c
        INNER JOIN strategy_definitions s ON c.strategy_id = s.strategy_id
        WHERE c.chain_name=?
        ORDER BY c.chain_order
        """
        return self.persistence.fetchall(sql, (kind,))

    def update_strategy_params(self, strategy_id, params):
        now = self.persistence.now_ts()
        self.persistence.execute(
            "UPDATE strategy_definitions SET params_json=?, updated_at=? WHERE strategy_id=?",
            (json.dumps(params, ensure_ascii=False), now, strategy_id),
        )
        self.strategies_changed.emit()

    def set_strategy_enabled(self, strategy_id, enabled):
        self.persistence.execute(
            "UPDATE strategy_definitions SET enabled=?, updated_at=? WHERE strategy_id=?",
            (1 if enabled else 0, self.persistence.now_ts(), strategy_id),
        )
        self.strategies_changed.emit()

    def move_chain_item(self, chain_item_id, delta):
        row = self.persistence.fetchone(
            "SELECT chain_name, chain_order FROM strategy_chain_items WHERE chain_item_id=?",
            (chain_item_id,),
        )
        if not row:
            return
        target_order = int(row["chain_order"]) + int(delta)
        swap = self.persistence.fetchone(
            "SELECT chain_item_id, chain_order FROM strategy_chain_items WHERE chain_name=? AND chain_order=?",
            (row["chain_name"], target_order),
        )
        if not swap:
            return
        now = self.persistence.now_ts()
        self.persistence.execute(
            "UPDATE strategy_chain_items SET chain_order=?, updated_at=? WHERE chain_item_id=?",
            (target_order, now, chain_item_id),
        )
        self.persistence.execute(
            "UPDATE strategy_chain_items SET chain_order=?, updated_at=? WHERE chain_item_id=?",
            (row["chain_order"], now, swap["chain_item_id"]),
        )
        self.strategies_changed.emit()

    def delete_strategy(self, chain_item_id):
        row = self.persistence.fetchone("SELECT strategy_id FROM strategy_chain_items WHERE chain_item_id=?", (chain_item_id,))
        if not row:
            return
        strategy_id = row["strategy_id"]
        self.persistence.execute("DELETE FROM strategy_chain_items WHERE chain_item_id=?", (chain_item_id,))
        self.persistence.execute("DELETE FROM strategy_definitions WHERE strategy_id=?", (strategy_id,))
        self._reorder_chain("buy")
        self._reorder_chain("sell")
        self.strategies_changed.emit()

    def _reorder_chain(self, kind):
        rows = self.persistence.fetchall(
            "SELECT chain_item_id FROM strategy_chain_items WHERE chain_name=? ORDER BY chain_order, created_at",
            (kind,),
        )
        now = self.persistence.now_ts()
        for idx, row in enumerate(rows, 1):
            self.persistence.execute(
                "UPDATE strategy_chain_items SET chain_order=?, updated_at=? WHERE chain_item_id=?",
                (idx, now, row["chain_item_id"]),
            )

    def export_chain_profile(self, kind):
        rows = self.get_chain_items(kind)
        data = []
        for row in rows:
            data.append({
                "chain_order": int(row["chain_order"] or 0),
                "enabled": int(row["strategy_enabled"] or 0),
                "strategy_no": int(row["strategy_no"] or 0),
                "strategy_type": row["strategy_type"],
                "strategy_name": row["strategy_name"],
                "scope_type": row["scope_type"],
                "scope_targets_json": row["scope_targets_json"] or "[]",
                "params_json": row["params_json"] or "{}",
            })
        return data

    def reset_to_defaults(self):
        self.persistence.execute("DELETE FROM strategy_chain_items")
        self.persistence.execute("DELETE FROM strategy_definitions")
        self._seed_default_strategies()
        self.strategies_changed.emit()

    def import_chain_profile(self, kind, items):
        self.persistence.execute("DELETE FROM strategy_chain_items WHERE chain_name=?", (kind,))
        self.persistence.execute("DELETE FROM strategy_definitions WHERE strategy_kind=?", (kind,))
        now = self.persistence.now_ts()
        if not items:
            self._seed_default_strategies()
            self.strategies_changed.emit()
            return
        for idx, item in enumerate(sorted(items, key=lambda x: int(x.get("chain_order", 0) or 0)), 1):
            strategy_id = "{0}_{1}".format(kind, uuid.uuid4().hex[:10])
            chain_item_id = "chain_" + uuid.uuid4().hex[:10]
            self.persistence.execute(
                """
                INSERT INTO strategy_definitions (
                    strategy_id, strategy_kind, strategy_type, strategy_name, strategy_no, enabled,
                    scope_type, scope_targets_json, params_json, extra_json,
                    is_assignable_to_slot, is_news_filter, is_news_trade, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?, ?, ?, ?)
                """,
                (
                    strategy_id,
                    kind,
                    item.get("strategy_type", ""),
                    item.get("strategy_name", item.get("strategy_type", "")),
                    int(item.get("strategy_no", 0) or 0) or self._allocate_strategy_no(kind),
                    1 if int(item.get("enabled", 1) or 0) else 0,
                    item.get("scope_type", "global"),
                    item.get("scope_targets_json", "[]") or "[]",
                    item.get("params_json", "{}") or "{}",
                    0 if item.get("strategy_type", "") == "news_trade" else 1,
                    1 if item.get("strategy_type", "") == "news_filter" else 0,
                    1 if item.get("strategy_type", "") == "news_trade" else 0,
                    now,
                    now,
                ),
            )
            self.persistence.execute(
                """
                INSERT INTO strategy_chain_items (
                    chain_item_id, strategy_id, chain_name, chain_order, enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chain_item_id,
                    strategy_id,
                    kind,
                    idx,
                    1 if int(item.get("enabled", 1) or 0) else 0,
                    now,
                    now,
                ),
            )
        self._seed_default_strategies()
        self.strategies_changed.emit()

    def _allocate_strategy_no(self, kind):
        row = self.persistence.fetchone(
            "SELECT COALESCE(MAX(strategy_no), 0) AS max_no FROM strategy_definitions WHERE strategy_kind=?",
            (kind,),
        )
        return int(row["max_no"] or 0) + 1

    def get_strategy_catalog(self, kind, include_unassignable=True):
        self._seed_default_strategies()
        sql = """
        SELECT s.strategy_id, s.strategy_kind, s.strategy_type, s.strategy_name, s.strategy_no,
               s.enabled AS strategy_enabled, s.scope_type, s.scope_targets_json, s.params_json,
               s.is_assignable_to_slot, s.is_news_filter, s.is_news_trade,
               c.chain_item_id, c.chain_order, c.enabled AS chain_enabled
        FROM strategy_definitions s
        LEFT JOIN strategy_chain_items c ON c.strategy_id = s.strategy_id AND c.chain_name = s.strategy_kind
        WHERE s.strategy_kind=?
        """
        params = [kind]
        if not include_unassignable:
            sql += " AND COALESCE(s.is_assignable_to_slot, 1)=1"
        sql += " ORDER BY COALESCE(c.chain_order, 9999), s.strategy_no, s.created_at, s.strategy_id"
        return self.persistence.fetchall(sql, tuple(params))

    def get_strategy_by_no(self, kind, strategy_no):
        return self.persistence.fetchone(
            """
            SELECT strategy_id, strategy_kind, strategy_type, strategy_name, strategy_no, enabled,
                   scope_type, scope_targets_json, params_json, is_assignable_to_slot, is_news_filter, is_news_trade
            FROM strategy_definitions
            WHERE strategy_kind=? AND strategy_no=?
            """,
            (kind, int(strategy_no)),
        )

    def get_assignable_strategy_nos(self, kind):
        rows = self.persistence.fetchall(
            "SELECT strategy_no FROM strategy_definitions WHERE strategy_kind=? AND COALESCE(is_assignable_to_slot, 1)=1 ORDER BY strategy_no",
            (kind,),
        )
        return [int(row["strategy_no"] or 0) for row in rows if int(row["strategy_no"] or 0) > 0]

    def validate_buy_expression(self, expression_items):
        items = list(expression_items or [])
        if not items:
            return {"ok": True, "message": ""}
        expected_kind = "strategy"
        valid_nos = set(self.get_assignable_strategy_nos("buy"))
        for item in items:
            kind = str(item.get("kind") or "").strip().lower()
            if kind != expected_kind:
                if expected_kind == "strategy":
                    return {"ok": False, "message": "매수 전략식은 전략으로 시작하고 전략으로 끝나야 합니다."}
                return {"ok": False, "message": "연산자 다음에는 전략이 필요합니다."}
            if kind == "strategy":
                try:
                    strategy_no = int(item.get("no") or 0)
                except Exception:
                    strategy_no = 0
                if strategy_no <= 0 or strategy_no not in valid_nos:
                    return {"ok": False, "message": "유효하지 않은 매수 전략 번호가 포함되어 있습니다."}
                expected_kind = "op"
            else:
                value = str(item.get("value") or "").strip().upper()
                if value not in ["AND", "OR"]:
                    return {"ok": False, "message": "매수 전략식의 연산자는 AND 또는 OR만 사용할 수 있습니다."}
                expected_kind = "strategy"
        if expected_kind != "op":
            return {"ok": False, "message": "매수 전략식은 전략으로 끝나야 합니다."}
        return {"ok": True, "message": ""}

    def normalize_strategy_nos(self, kind, strategy_nos):
        valid_nos = set(self.get_assignable_strategy_nos(kind))
        seen = set()
        result = []
        for value in list(strategy_nos or []):
            try:
                number = int(value or 0)
            except Exception:
                continue
            if number <= 0 or number not in valid_nos or number in seen:
                continue
            seen.add(number)
            result.append(number)
        return result

    def get_slot_strategy_policy(self, slot_no):
        row = self.persistence.fetchone(
            "SELECT slot_no, buy_expression_json, sell_strategy_nos_json, news_min_score, updated_at FROM slot_strategy_policy WHERE slot_no=?",
            (int(slot_no),),
        )
        return self._row_to_dict(row)

    def save_slot_strategy_policy(self, slot_no, buy_expression_items, sell_strategy_nos, news_min_score=0):
        check = self.validate_buy_expression(buy_expression_items)
        if not check.get("ok"):
            raise ValueError(check.get("message") or "매수 전략식이 올바르지 않습니다.")
        normalized_sell = self.normalize_strategy_nos("sell", sell_strategy_nos)
        now = self.persistence.now_ts()
        self.persistence.execute(
            """
            INSERT OR REPLACE INTO slot_strategy_policy (
                slot_no, buy_expression_json, sell_strategy_nos_json, news_min_score, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(slot_no),
                json.dumps(list(buy_expression_items or []), ensure_ascii=False),
                json.dumps(normalized_sell, ensure_ascii=False),
                int(news_min_score or 0),
                now,
            ),
        )
        return self.get_slot_strategy_policy(slot_no)

    def delete_slot_strategy_policy(self, slot_no):
        self.persistence.execute(
            "DELETE FROM slot_strategy_policy WHERE slot_no=?",
            (int(slot_no),),
        )

    def get_default_strategy_policy(self):
        row = self.persistence.fetchone(
            "SELECT id, buy_expression_json, sell_strategy_nos_json, news_min_score, updated_at FROM default_strategy_policy WHERE id=1"
        )
        if row:
            return self._row_to_dict(row)
        now = self.persistence.now_ts()
        self.persistence.execute(
            "INSERT OR IGNORE INTO default_strategy_policy (id, buy_expression_json, sell_strategy_nos_json, news_min_score, updated_at) VALUES (1, '[]', '[]', 0, ?)",
            (now,),
        )
        return self._row_to_dict(self.persistence.fetchone(
            "SELECT id, buy_expression_json, sell_strategy_nos_json, news_min_score, updated_at FROM default_strategy_policy WHERE id=1"
        ))

    def save_default_strategy_policy(self, buy_expression_items, sell_strategy_nos, news_min_score=0):
        check = self.validate_buy_expression(buy_expression_items)
        if not check.get("ok"):
            raise ValueError(check.get("message") or "디폴트 매수 전략식이 올바르지 않습니다.")
        normalized_sell = self.normalize_strategy_nos("sell", sell_strategy_nos)
        now = self.persistence.now_ts()
        self.persistence.execute(
            """
            INSERT OR REPLACE INTO default_strategy_policy (
                id, buy_expression_json, sell_strategy_nos_json, news_min_score, updated_at
            ) VALUES (1, ?, ?, ?, ?)
            """,
            (
                json.dumps(list(buy_expression_items or []), ensure_ascii=False),
                json.dumps(normalized_sell, ensure_ascii=False),
                int(news_min_score or 0),
                now,
            ),
        )
        return self.get_default_strategy_policy()

    def resolve_slot_strategy_policy(self, slot_no):
        slot_row = self.get_slot_strategy_policy(slot_no)
        if slot_row:
            return {
                "source": "slot",
                "slot_no": int(slot_no),
                "buy_expression_json": slot_row["buy_expression_json"] or "[]",
                "sell_strategy_nos_json": slot_row["sell_strategy_nos_json"] or "[]",
                "news_min_score": int(slot_row["news_min_score"] or 0),
                "updated_at": slot_row["updated_at"],
            }
        default_row = self.get_default_strategy_policy()
        return {
            "source": "default",
            "slot_no": int(slot_no),
            "buy_expression_json": default_row["buy_expression_json"] or "[]",
            "sell_strategy_nos_json": default_row["sell_strategy_nos_json"] or "[]",
            "news_min_score": int(default_row["news_min_score"] or 0),
            "updated_at": default_row["updated_at"],
        }

    def get_news_trade_policy(self):
        row = self.persistence.fetchone(
            "SELECT id, enabled, min_score, sell_strategy_nos_json, updated_at FROM news_trade_policy WHERE id=1"
        )
        if row:
            return self._row_to_dict(row)
        now = self.persistence.now_ts()
        self.persistence.execute(
            "INSERT OR IGNORE INTO news_trade_policy (id, enabled, min_score, sell_strategy_nos_json, updated_at) VALUES (1, 0, 80, '[]', ?)",
            (now,),
        )
        return self._row_to_dict(self.persistence.fetchone(
            "SELECT id, enabled, min_score, sell_strategy_nos_json, updated_at FROM news_trade_policy WHERE id=1"
        ))

    def save_news_trade_policy(self, enabled, min_score, sell_strategy_nos=None):
        normalized_sell = self.normalize_strategy_nos("sell", sell_strategy_nos)
        now = self.persistence.now_ts()
        self.persistence.execute(
            """
            INSERT OR REPLACE INTO news_trade_policy (
                id, enabled, min_score, sell_strategy_nos_json, updated_at
            ) VALUES (1, ?, ?, ?, ?)
            """,
            (
                1 if enabled else 0,
                int(min_score or 0),
                json.dumps(normalized_sell, ensure_ascii=False),
                now,
            ),
        )
        return self.get_news_trade_policy()

    def expression_contains_strategy_type(self, expression_items, strategy_type):
        for item in list(expression_items or []):
            if str(item.get("kind") or "").strip().lower() != "strategy":
                continue
            row = self.get_strategy_by_no("buy", item.get("no"))
            if row and str(row["strategy_type"] or "") == str(strategy_type or ""):
                return True
        return False

    def collect_active_news_filter_scores(self):
        scores = []
        default_row = self.get_default_strategy_policy()
        default_expr = json.loads(default_row["buy_expression_json"] or "[]")
        if self.expression_contains_strategy_type(default_expr, "news_filter"):
            score = int(default_row["news_min_score"] or 0)
            if score > 0:
                scores.append({"scope": "default", "slot_no": None, "score": score})
        slot_rows = self.persistence.fetchall(
            "SELECT slot_no, buy_expression_json, news_min_score FROM slot_strategy_policy ORDER BY slot_no"
        )
        for row in slot_rows:
            expression = json.loads(row["buy_expression_json"] or "[]")
            if not self.expression_contains_strategy_type(expression, "news_filter"):
                continue
            score = int(row["news_min_score"] or 0)
            if score > 0:
                scores.append({"scope": "slot", "slot_no": int(row["slot_no"] or 0), "score": score})
        return scores

    def _safe_json_loads(self, value, default):
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value or "")
        except Exception:
            return default

    def _row_to_dict(self, row):
        if row is None:
            return {}
        if isinstance(row, dict):
            return dict(row)
        try:
            return dict(row)
        except Exception:
            return {}

    def _extract_symbol_meta(self, symbol_row):
        symbol_row = self._row_to_dict(symbol_row)
        extra_json = symbol_row.get("extra_json")
        if isinstance(extra_json, dict):
            return dict(extra_json)
        try:
            return json.loads(extra_json or "{}")
        except Exception:
            return {}

    def _resolve_numeric_metric(self, snapshot, symbol_row, *keys):
        symbol_row = self._row_to_dict(symbol_row)
        symbol_meta = self._extract_symbol_meta(symbol_row)
        for key in list(keys or []):
            for source in [snapshot or {}, symbol_row or {}, symbol_meta or {}]:
                try:
                    value = float(source.get(key) or 0)
                except Exception:
                    value = 0.0
                if value != 0:
                    return value
        return 0.0

    def _summarize_buy_result(self, outcome):
        outcome = self._row_to_dict(outcome)
        return {
            "strategy_no": int(outcome.get("strategy_no") or 0),
            "strategy_id": str(outcome.get("strategy_id") or ""),
            "strategy_name": str(outcome.get("strategy_name") or ""),
            "strategy_type": str(outcome.get("strategy_type") or ""),
            "passed": bool(outcome.get("passed")),
            "reason": str(outcome.get("reason") or ""),
        }

    def _build_buy_strategy_metadata(self, results):
        summaries = []
        passed_items = []
        trigger_item = {}
        for outcome in list(results or []):
            item = self._summarize_buy_result(outcome)
            if not item["strategy_no"] and not item["strategy_id"] and not item["strategy_name"] and not item["strategy_type"]:
                continue
            summaries.append(item)
            if item["passed"]:
                compact = {
                    "strategy_no": item["strategy_no"],
                    "strategy_id": item["strategy_id"],
                    "strategy_name": item["strategy_name"],
                    "strategy_type": item["strategy_type"],
                }
                passed_items.append(compact)
                trigger_item = dict(compact)
        return {
            "buy_strategy_results": summaries,
            "passed_buy_strategies": passed_items,
            "trigger_buy_strategy": trigger_item,
        }

    def resolve_symbol_slot_no(self, symbol_row, preferred_slot_no=None):
        try:
            slot_no = int(preferred_slot_no or 0)
        except Exception:
            slot_no = 0
        if slot_no > 0:
            return slot_no
        symbol_row = self._row_to_dict(symbol_row)
        items = self._safe_json_loads(symbol_row.get("source_conditions_json"), [])
        latest = None
        for item in items:
            try:
                current_slot = int(item.get("slot_no") or 0)
            except Exception:
                current_slot = 0
            if current_slot <= 0:
                continue
            ts = str(item.get("ts") or "")
            if latest is None or ts >= latest[0]:
                latest = (ts, current_slot)
        return int(latest[1]) if latest else 0

    def _evaluate_buy_strategy_by_no(self, strategy_no, symbol_row, news_scores, news_min_score=0, mode="detected"):
        symbol_row = self._row_to_dict(symbol_row)
        row = self.get_strategy_by_no("buy", strategy_no)
        if not row:
            return {
                "strategy_no": int(strategy_no or 0),
                "strategy_type": "unknown",
                "passed": False,
                "threshold": 0,
                "score": None,
                "reason": "전략 번호를 찾지 못했습니다.",
            }
        if not int(row["enabled"] or 0):
            return {
                "strategy_no": int(row["strategy_no"] or 0),
                "strategy_type": row["strategy_type"],
                "passed": False,
                "threshold": 0,
                "score": None,
                "reason": "비활성 전략입니다.",
            }
        params = self._safe_json_loads(row["params_json"], {})
        if str(row["strategy_type"] or "") == "news_filter" and int(news_min_score or 0) > 0:
            params = dict(params or {})
            params["min_score"] = int(news_min_score)
        snapshot = self.get_realtime_market_snapshot((symbol_row or {}).get("code"))
        outcome = self._evaluate_one(row["strategy_type"], params, news_scores, mode, snapshot=snapshot, symbol_row=symbol_row)
        outcome.update({
            "strategy_no": int(row["strategy_no"] or 0),
            "strategy_id": row["strategy_id"],
            "strategy_name": row["strategy_name"],
        })
        return outcome

    def evaluate_slot_buy_policy(self, symbol_row, news_scores, slot_no=0):
        symbol_row = self._row_to_dict(symbol_row)
        resolved_slot_no = self.resolve_symbol_slot_no(symbol_row, slot_no)
        if resolved_slot_no <= 0:
            return {
                "passed": False,
                "results": [],
                "terminal_reason": "조건식 슬롯 식별 실패",
                "trigger_strategy_type": "slot_buy",
                "news_scores": news_scores or {},
                "mode": "detected",
                "entry_source": "slot_buy",
                "entry_slot_no": 0,
                "policy_source": "unknown",
                "applied_sell_strategy_nos": [],
                "buy_expression_items": [],
                "news_min_score": 0,
                "buy_strategy_results": [],
                "passed_buy_strategies": [],
                "trigger_buy_strategy": {},
            }
        policy = self.resolve_slot_strategy_policy(resolved_slot_no)
        expression_items = self._safe_json_loads(policy.get("buy_expression_json"), [])
        if not expression_items:
            return {
                "passed": False,
                "results": [],
                "terminal_reason": "매수 전략 미설정",
                "trigger_strategy_type": "slot_buy",
                "news_scores": news_scores or {},
                "mode": "detected",
                "entry_source": "slot_buy",
                "entry_slot_no": resolved_slot_no,
                "policy_source": policy.get("source", "default"),
                "applied_sell_strategy_nos": self._safe_json_loads(policy.get("sell_strategy_nos_json"), []),
                "buy_expression_items": expression_items,
                "news_min_score": int(policy.get("news_min_score") or 0),
                "buy_strategy_results": [],
                "passed_buy_strategies": [],
                "trigger_buy_strategy": {},
            }
        check = self.validate_buy_expression(expression_items)
        if not check.get("ok"):
            return {
                "passed": False,
                "results": [],
                "terminal_reason": check.get("message") or "매수 전략식 오류",
                "trigger_strategy_type": "slot_buy",
                "news_scores": news_scores or {},
                "mode": "detected",
                "entry_source": "slot_buy",
                "entry_slot_no": resolved_slot_no,
                "policy_source": policy.get("source", "default"),
                "applied_sell_strategy_nos": self._safe_json_loads(policy.get("sell_strategy_nos_json"), []),
                "buy_expression_items": expression_items,
                "news_min_score": int(policy.get("news_min_score") or 0),
                "buy_strategy_results": [],
                "passed_buy_strategies": [],
                "trigger_buy_strategy": {},
            }
        results = []
        current_value = None
        pending_op = None
        terminal_reason = ""
        trigger_strategy_type = "slot_buy"
        for item in expression_items:
            if str(item.get("kind") or "") == "op":
                pending_op = str(item.get("value") or "").upper()
                continue
            outcome = self._evaluate_buy_strategy_by_no(item.get("no"), symbol_row, news_scores, policy.get("news_min_score", 0), mode="detected")
            results.append(outcome)
            if current_value is None:
                current_value = bool(outcome.get("passed"))
            else:
                if pending_op == "AND":
                    current_value = bool(current_value and outcome.get("passed"))
                else:
                    current_value = bool(current_value or outcome.get("passed"))
            if outcome.get("passed"):
                trigger_strategy_type = outcome.get("strategy_type") or trigger_strategy_type
            else:
                terminal_reason = outcome.get("reason") or terminal_reason
            pending_op = None
        metadata = self._build_buy_strategy_metadata(results)
        return {
            "passed": bool(current_value),
            "results": results,
            "terminal_reason": terminal_reason if not bool(current_value) else "",
            "trigger_strategy_type": trigger_strategy_type,
            "news_scores": news_scores or {},
            "mode": "detected",
            "entry_source": "slot_buy",
            "entry_slot_no": resolved_slot_no,
            "policy_source": policy.get("source", "default"),
            "applied_sell_strategy_nos": self._safe_json_loads(policy.get("sell_strategy_nos_json"), []),
            "buy_expression_items": expression_items,
            "news_min_score": int(policy.get("news_min_score") or 0),
            "buy_strategy_results": metadata.get("buy_strategy_results") or [],
            "passed_buy_strategies": metadata.get("passed_buy_strategies") or [],
            "trigger_buy_strategy": metadata.get("trigger_buy_strategy") or {},
        }

    def get_effective_news_trade_sell_strategy_nos(self):
        policy = self.get_news_trade_policy()
        sell_strategy_nos = self._safe_json_loads(policy.get("sell_strategy_nos_json"), [])
        normalized = self.normalize_strategy_nos("sell", sell_strategy_nos)
        if normalized:
            return normalized
        default_row = self.get_default_strategy_policy()
        return self.normalize_strategy_nos("sell", self._safe_json_loads(default_row.get("sell_strategy_nos_json"), []))

    def evaluate_news_trade_candidate(self, symbol_row, news_scores):
        policy = self.get_news_trade_policy()
        if not bool(int(policy["enabled"] or 0)):
            return {
                "passed": False,
                "results": [],
                "terminal_reason": "뉴스매매 비활성",
                "trigger_strategy_type": "news_trade",
                "news_scores": news_scores or {},
                "mode": "recheck",
                "entry_source": "news_trade",
                "entry_slot_no": None,
                "policy_source": "news_trade",
                "applied_sell_strategy_nos": self.get_effective_news_trade_sell_strategy_nos(),
                "news_trade_min_score": int(policy["min_score"] or 0),
                "buy_strategy_results": [],
                "passed_buy_strategies": [],
                "trigger_buy_strategy": {},
            }
        min_score = int(policy["min_score"] or 0)
        final_score = float((news_scores or {}).get("final_score", 0) or 0)
        passed = final_score >= min_score
        result = {
            "strategy_no": 0,
            "strategy_type": "news_trade",
            "strategy_name": "뉴스매매",
            "passed": passed,
            "threshold": min_score,
            "score": final_score,
            "reason": "뉴스매매 점수 부족" if not passed else "뉴스매매 점수 통과",
        }
        metadata = self._build_buy_strategy_metadata([result])
        return {
            "passed": passed,
            "results": [result],
            "terminal_reason": "" if passed else result["reason"],
            "trigger_strategy_type": "news_trade",
            "news_scores": news_scores or {},
            "mode": "recheck",
            "entry_source": "news_trade",
            "entry_slot_no": None,
            "policy_source": "news_trade",
            "applied_sell_strategy_nos": self.get_effective_news_trade_sell_strategy_nos(),
            "news_trade_min_score": min_score,
            "buy_strategy_results": metadata.get("buy_strategy_results") or [],
            "passed_buy_strategies": metadata.get("passed_buy_strategies") or [],
            "trigger_buy_strategy": metadata.get("trigger_buy_strategy") or {},
        }

    def _evaluate_sell_strategy_by_no(self, strategy_no, position_row, cycle_row=None, active_state=None, now_dt=None):
        row = self.get_strategy_by_no("sell", strategy_no)
        if not row or not int(row["enabled"] or 0):
            return {
                "strategy_no": int(strategy_no or 0),
                "strategy_type": str((row or {}).get("strategy_type") or "unknown"),
                "strategy_name": str((row or {}).get("strategy_name") or ""),
                "passed": False,
                "reason": "비활성 또는 미존재 전략",
                "state_changed": False,
            }
        params = self._safe_json_loads(row["params_json"], {})
        active_state = dict(active_state or {})
        now_dt = now_dt or datetime.datetime.now()
        strategy_type = str(row["strategy_type"] or "")
        eval_rate = float(position_row.get("eval_rate") or 0)
        current_price = float(position_row.get("current_price") or 0)
        avg_price = float(position_row.get("avg_price") or 0)
        state_changed = False
        if strategy_type == "stop_loss":
            threshold = float(params.get("stop_loss_pct", -3.0) or -3.0)
            passed = eval_rate <= threshold
            return {"strategy_no": int(row["strategy_no"] or 0), "strategy_type": strategy_type, "strategy_name": row["strategy_name"], "passed": passed, "reason": "손절 도달" if passed else "손절 미도달", "state_changed": False}
        if strategy_type == "take_profit":
            threshold = float(params.get("take_profit_pct", 5.0) or 5.0)
            passed = eval_rate >= threshold
            return {"strategy_no": int(row["strategy_no"] or 0), "strategy_type": strategy_type, "strategy_name": row["strategy_name"], "passed": passed, "reason": "익절 도달" if passed else "익절 미도달", "state_changed": False}
        if strategy_type == "trailing_stop":
            start_pct = float(params.get("trail_start_pct", 3.0) or 3.0)
            gap_pct = float(params.get("trail_gap_pct", 1.5) or 1.5)
            highest_rate = float(active_state.get("trail_high_rate") or eval_rate)
            highest_price = float(active_state.get("trail_high_price") or current_price or avg_price)
            if eval_rate > highest_rate:
                highest_rate = eval_rate
                active_state["trail_high_rate"] = highest_rate
                state_changed = True
            if current_price > highest_price:
                highest_price = current_price
                active_state["trail_high_price"] = highest_price
                state_changed = True
            passed = highest_rate >= start_pct and eval_rate <= (highest_rate - gap_pct)
            return {"strategy_no": int(row["strategy_no"] or 0), "strategy_type": strategy_type, "strategy_name": row["strategy_name"], "passed": passed, "reason": "트레일링 스탑 도달" if passed else "트레일링 스탑 미도달", "state_changed": state_changed, "active_state": active_state}
        if strategy_type == "time_exit":
            hold_minutes = int(params.get("hold_minutes", 60) or 60)
            start_text = ""
            if cycle_row:
                start_text = str(cycle_row.get("buy_filled_at") or cycle_row.get("buy_order_at") or cycle_row.get("entry_detected_at") or "")
            start_dt = None
            if start_text:
                try:
                    start_dt = datetime.datetime.strptime(start_text, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    start_dt = None
            passed = False
            if start_dt is not None:
                passed = (now_dt - start_dt).total_seconds() >= (hold_minutes * 60)
            return {"strategy_no": int(row["strategy_no"] or 0), "strategy_type": strategy_type, "strategy_name": row["strategy_name"], "passed": passed, "reason": "보유시간 도달" if passed else "보유시간 미도달", "state_changed": False}
        if strategy_type == "market_close_exit":
            exit_hhmm = str(params.get("exit_hhmm", "1520") or "1520").zfill(4)
            current_hhmm = now_dt.strftime("%H%M")
            passed = now_dt.weekday() < 5 and current_hhmm >= exit_hhmm
            return {"strategy_no": int(row["strategy_no"] or 0), "strategy_type": strategy_type, "strategy_name": row["strategy_name"], "passed": passed, "reason": "장마감 청산 시간 도달" if passed else "장마감 청산 시간 미도달", "state_changed": False}
        return {"strategy_no": int(row["strategy_no"] or 0), "strategy_type": strategy_type, "strategy_name": row["strategy_name"], "passed": False, "reason": "매도 전략 미구현", "state_changed": False}

    def evaluate_sell_strategy_list(self, sell_strategy_nos, position_row, cycle_row=None, active_state=None):
        strategy_nos = self.normalize_strategy_nos("sell", sell_strategy_nos)
        active_state = dict(active_state or {})
        results = []
        passed = False
        trigger_no = 0
        trigger_reason = ""
        state_changed = False
        for strategy_no in strategy_nos:
            outcome = self._evaluate_sell_strategy_by_no(strategy_no, position_row, cycle_row=cycle_row, active_state=active_state)
            results.append(outcome)
            if outcome.get("state_changed") and isinstance(outcome.get("active_state"), dict):
                active_state = dict(outcome.get("active_state") or {})
                state_changed = True
            if outcome.get("passed") and not passed:
                passed = True
                trigger_no = int(outcome.get("strategy_no") or 0)
                trigger_reason = outcome.get("reason") or ""
        return {
            "passed": passed,
            "results": results,
            "trigger_strategy_no": trigger_no,
            "trigger_reason": trigger_reason,
            "active_state": active_state,
            "state_changed": state_changed,
            "applied_sell_strategy_nos": strategy_nos,
        }

    def evaluate_buy_chain(self, symbol_row, news_scores, mode="detected"):
        symbol_row = self._row_to_dict(symbol_row)
        rows = self.get_chain_items("buy")
        results = []
        passed = True
        terminal_reason = ""
        trigger_strategy_type = ""
        for row in rows:
            if not int(row["strategy_enabled"] or 0):
                continue
            params = json.loads(row["params_json"] or "{}")
            strategy_type = row["strategy_type"]
            if strategy_type == "news_trade" and mode != "recheck":
                continue
            if strategy_type == "news_filter" and mode == "recheck":
                continue
            snapshot = self.get_realtime_market_snapshot((symbol_row or {}).get("code"))
            outcome = self._evaluate_one(strategy_type, params, news_scores, mode, snapshot=snapshot, symbol_row=symbol_row)
            outcome["strategy_id"] = row["strategy_id"]
            outcome["strategy_name"] = row["strategy_name"]
            results.append(outcome)
            if not outcome.get("passed"):
                passed = False
                terminal_reason = outcome.get("reason", strategy_type)
                break
            trigger_strategy_type = strategy_type
        return {
            "passed": passed,
            "results": results,
            "terminal_reason": terminal_reason,
            "trigger_strategy_type": trigger_strategy_type or mode,
            "news_scores": news_scores or {},
            "mode": mode,
        }

    def _evaluate_one(self, strategy_type, params, news_scores, mode, snapshot=None, symbol_row=None):
        min_score = int(params.get("min_score", 0) or 0)
        final_score = float((news_scores or {}).get("final_score", 0) or 0)
        importance_score = float((news_scores or {}).get("importance_score", 0) or 0)
        frequency_score = float((news_scores or {}).get("frequency_score", 0) or 0)
        snapshot = dict(snapshot or {})
        current_price = float(snapshot.get("current_price") or 0)
        vwap_intraday = float(snapshot.get("vwap_intraday") or 0)
        sell_pressure_ratio = float(snapshot.get("sell_pressure_ratio") or 0)
        sell_hoga_total = float(snapshot.get("sell_hoga_total") or 0)
        buy_hoga_total = float(snapshot.get("buy_hoga_total") or 0)

        if strategy_type == "institution_trend_a":
            net_buy_amount = self._resolve_numeric_metric(
                snapshot,
                symbol_row,
                "institution_net_buy_amount",
                "institutional_net_buy_amount",
                "institution_net_amount",
                "institution_buy_amount",
            )
            if net_buy_amount == 0:
                return {
                    "strategy_type": strategy_type,
                    "passed": False,
                    "threshold": 0,
                    "score": net_buy_amount,
                    "reason": "기관 순매수 금액 값 부족",
                    "details": {"institution_net_buy_amount": net_buy_amount},
                }
            passed = net_buy_amount > 0
            return {
                "strategy_type": strategy_type,
                "passed": passed,
                "threshold": 0,
                "score": net_buy_amount,
                "reason": "기관 순매수 통과" if passed else "기관 순매수 아님",
                "details": {"institution_net_buy_amount": net_buy_amount},
            }
        if strategy_type == "institution_trend_b":
            min_ratio_pct = float(params.get("min_net_buy_ratio_pct", 1.0) or 1.0)
            net_buy_ratio_pct = self._resolve_numeric_metric(
                snapshot,
                symbol_row,
                "institution_net_buy_ratio_pct",
                "institutional_net_buy_ratio_pct",
                "institution_net_buy_ratio",
                "institution_buy_ratio_pct",
                "institution_buy_ratio",
            )
            if net_buy_ratio_pct <= 0:
                return {
                    "strategy_type": strategy_type,
                    "passed": False,
                    "threshold": min_ratio_pct,
                    "score": net_buy_ratio_pct,
                    "reason": "기관 순매수 비율 값 부족",
                    "details": {"institution_net_buy_ratio_pct": net_buy_ratio_pct},
                }
            passed = net_buy_ratio_pct >= min_ratio_pct
            return {
                "strategy_type": strategy_type,
                "passed": passed,
                "threshold": min_ratio_pct,
                "score": net_buy_ratio_pct,
                "reason": "기관 순매수 비율 통과" if passed else "기관 순매수 비율 부족",
                "details": {"institution_net_buy_ratio_pct": net_buy_ratio_pct},
            }
        if strategy_type == "institution_trend_c":
            streak_count = int(params.get("streak_count", 2) or 2)
            interval_type = str(params.get("interval_type") or "day").strip().lower()
            key_map = {
                "day": [
                    "institution_streak_day",
                    "institution_positive_streak_day",
                    "institution_consecutive_net_buy_day",
                ],
                "60m": [
                    "institution_streak_60m",
                    "institution_positive_streak_60m",
                    "institution_consecutive_net_buy_60m",
                ],
                "5m": [
                    "institution_streak_5m",
                    "institution_positive_streak_5m",
                    "institution_consecutive_net_buy_5m",
                ],
            }
            streak_value = self._resolve_numeric_metric(
                snapshot,
                symbol_row,
                *(key_map.get(interval_type) or key_map["day"])
            )
            if streak_value <= 0:
                return {
                    "strategy_type": strategy_type,
                    "passed": False,
                    "threshold": streak_count,
                    "score": streak_value,
                    "reason": "기관 연속 순매수 구간 값 부족",
                    "details": {
                        "interval_type": interval_type,
                        "institution_positive_streak": streak_value,
                    },
                }
            passed = streak_value >= streak_count
            return {
                "strategy_type": strategy_type,
                "passed": passed,
                "threshold": streak_count,
                "score": streak_value,
                "reason": "기관 연속 순매수 통과" if passed else "기관 연속 순매수 구간 부족",
                "details": {
                    "interval_type": interval_type,
                    "institution_positive_streak": streak_value,
                },
            }
        if strategy_type == "foreign_trend_a":
            net_buy_amount = self._resolve_numeric_metric(
                snapshot,
                symbol_row,
                "foreign_net_buy_amount",
                "foreigner_net_buy_amount",
                "foreign_net_amount",
                "foreign_buy_amount",
            )
            if net_buy_amount == 0:
                return {
                    "strategy_type": strategy_type,
                    "passed": False,
                    "threshold": 0,
                    "score": net_buy_amount,
                    "reason": "외국인 순매수 금액 값 부족",
                    "details": {"foreign_net_buy_amount": net_buy_amount},
                }
            passed = net_buy_amount > 0
            return {
                "strategy_type": strategy_type,
                "passed": passed,
                "threshold": 0,
                "score": net_buy_amount,
                "reason": "외국인 순매수 통과" if passed else "외국인 순매수 아님",
                "details": {"foreign_net_buy_amount": net_buy_amount},
            }
        if strategy_type == "foreign_trend_b":
            min_ratio_pct = float(params.get("min_net_buy_ratio_pct", 0.5) or 0.5)
            net_buy_ratio_pct = self._resolve_numeric_metric(
                snapshot,
                symbol_row,
                "foreign_net_buy_ratio_pct",
                "foreigner_net_buy_ratio_pct",
                "foreign_net_buy_ratio",
                "foreign_buy_ratio_pct",
                "foreign_buy_ratio",
            )
            if net_buy_ratio_pct <= 0:
                return {
                    "strategy_type": strategy_type,
                    "passed": False,
                    "threshold": min_ratio_pct,
                    "score": net_buy_ratio_pct,
                    "reason": "외국인 순매수 비율 값 부족",
                    "details": {"foreign_net_buy_ratio_pct": net_buy_ratio_pct},
                }
            passed = net_buy_ratio_pct >= min_ratio_pct
            return {
                "strategy_type": strategy_type,
                "passed": passed,
                "threshold": min_ratio_pct,
                "score": net_buy_ratio_pct,
                "reason": "외국인 순매수 비율 통과" if passed else "외국인 순매수 비율 부족",
                "details": {"foreign_net_buy_ratio_pct": net_buy_ratio_pct},
            }
        if strategy_type == "foreign_trend_c":
            streak_count = int(params.get("streak_count", 2) or 2)
            interval_type = str(params.get("interval_type") or "60m").strip().lower()
            key_map = {
                "day": [
                    "foreign_streak_day",
                    "foreigner_positive_streak_day",
                    "foreign_consecutive_net_buy_day",
                ],
                "60m": [
                    "foreign_streak_60m",
                    "foreigner_positive_streak_60m",
                    "foreign_consecutive_net_buy_60m",
                ],
                "5m": [
                    "foreign_streak_5m",
                    "foreigner_positive_streak_5m",
                    "foreign_consecutive_net_buy_5m",
                ],
            }
            streak_value = self._resolve_numeric_metric(
                snapshot,
                symbol_row,
                *(key_map.get(interval_type) or key_map["60m"])
            )
            if streak_value <= 0:
                return {
                    "strategy_type": strategy_type,
                    "passed": False,
                    "threshold": streak_count,
                    "score": streak_value,
                    "reason": "외국인 연속 순매수 구간 값 부족",
                    "details": {
                        "interval_type": interval_type,
                        "foreign_positive_streak": streak_value,
                    },
                }
            passed = streak_value >= streak_count
            return {
                "strategy_type": strategy_type,
                "passed": passed,
                "threshold": streak_count,
                "score": streak_value,
                "reason": "외국인 연속 순매수 통과" if passed else "외국인 연속 순매수 구간 부족",
                "details": {
                    "interval_type": interval_type,
                    "foreign_positive_streak": streak_value,
                },
            }

        if strategy_type == "news_filter":
            passed = final_score >= min_score
            return {
                "strategy_type": strategy_type,
                "passed": passed,
                "threshold": min_score,
                "score": final_score,
                "reason": "뉴스 점수 부족" if not passed else "뉴스 필터 통과",
            }
        if strategy_type == "news_trade":
            passed = final_score >= min_score and importance_score >= 60 and frequency_score >= 20
            return {
                "strategy_type": strategy_type,
                "passed": passed,
                "threshold": min_score,
                "score": final_score,
                "reason": "뉴스 매매 점수 부족" if not passed else "뉴스 매매 통과",
            }
        if strategy_type in ["vwap", "vwap_b"]:
            if current_price <= 0 or vwap_intraday <= 0:
                return {
                    "strategy_type": strategy_type,
                    "passed": False,
                    "threshold": "current_price>=vwap_intraday",
                    "score": current_price,
                    "reason": "VWAP 실시간 값 부족",
                    "details": {"current_price": current_price, "vwap_intraday": vwap_intraday},
                }
            default_comparison = "below" if strategy_type == "vwap_b" else "above_or_equal"
            comparison = str(params.get("comparison") or default_comparison).strip().lower()
            if comparison == "below":
                passed = current_price < vwap_intraday
            elif comparison == "below_or_equal":
                passed = current_price <= vwap_intraday
            elif comparison == "above":
                passed = current_price > vwap_intraday
            else:
                passed = current_price >= vwap_intraday
            return {
                "strategy_type": strategy_type,
                "passed": passed,
                "threshold": comparison,
                "score": current_price,
                "reason": (
                    "VWAP 통과"
                    if passed and comparison in ["above", "above_or_equal"]
                    else "VWAP 2 통과"
                    if passed
                    else "현재가가 VWAP보다 낮음"
                    if comparison in ["above", "above_or_equal"]
                    else "VWAP이 현재가보다 높지 않음"
                ),
                "details": {"current_price": current_price, "vwap_intraday": vwap_intraday},
            }
        if strategy_type in ["sell_pressure_a", "sell_pressure_b", "sell_pressure_c", "sell_pressure_d"]:
            default_map = {
                "sell_pressure_a": 2.0,
                "sell_pressure_b": 1.5,
                "sell_pressure_c": 1.2,
                "sell_pressure_d": 1.0,
            }
            threshold_value = float(
                params.get("min_ratio", params.get("max_ratio", default_map.get(strategy_type, 1.5)))
                or default_map.get(strategy_type, 1.5)
            )
            if sell_pressure_ratio <= 0:
                return {
                    "strategy_type": strategy_type,
                    "passed": False,
                    "threshold": threshold_value,
                    "score": sell_pressure_ratio,
                    "reason": "매도우위 실시간 값 부족",
                    "details": {"sell_pressure_ratio": sell_pressure_ratio},
                }
            if strategy_type == "sell_pressure_b":
                min_ratio = threshold_value
                passed = sell_pressure_ratio > min_ratio
                return {
                    "strategy_type": strategy_type,
                    "passed": passed,
                    "threshold": min_ratio,
                    "score": sell_pressure_ratio,
                    "reason": "매도우위 초과 통과" if passed else "매도우위 초과 미달",
                    "details": {"sell_pressure_ratio": sell_pressure_ratio},
                }
            max_ratio = float(params.get("max_ratio", default_map.get(strategy_type, 1.5)) or default_map.get(strategy_type, 1.5))
            passed = sell_pressure_ratio <= max_ratio
            return {
                "strategy_type": strategy_type,
                "passed": passed,
                "threshold": max_ratio,
                "score": sell_pressure_ratio,
                "reason": "매도우위 통과" if passed else "매도우위 과다",
                "details": {"sell_pressure_ratio": sell_pressure_ratio},
            }
        if strategy_type in ["buy_pressure_a", "buy_pressure_b", "buy_pressure_c"]:
            default_map = {
                "buy_pressure_a": 1.25,
                "buy_pressure_b": 1.5,
                "buy_pressure_c": 2.0,
            }
            min_buy_ratio = float(params.get("min_buy_ratio", default_map.get(strategy_type, 1.25)) or default_map.get(strategy_type, 1.25))
            max_buy_ratio = float(
                params.get("max_buy_ratio", params.get("min_buy_ratio", default_map.get(strategy_type, 1.5)))
                or default_map.get(strategy_type, 1.5)
            )
            if buy_hoga_total <= 0:
                return {
                    "strategy_type": strategy_type,
                    "passed": False,
                    "threshold": max_buy_ratio if strategy_type == "buy_pressure_b" else min_buy_ratio,
                    "score": 0.0,
                    "reason": "매수우위 실시간 값 부족",
                    "details": {
                        "buy_hoga_total": buy_hoga_total,
                        "sell_hoga_total": sell_hoga_total,
                    },
                }
            if sell_hoga_total > 0:
                buy_pressure_ratio = round(float(buy_hoga_total / sell_hoga_total), 4)
            else:
                buy_pressure_ratio = 9999.0
            if strategy_type == "buy_pressure_b":
                passed = buy_pressure_ratio < max_buy_ratio
                return {
                    "strategy_type": strategy_type,
                    "passed": passed,
                    "threshold": max_buy_ratio,
                    "score": buy_pressure_ratio,
                    "reason": "매수우위 미만 통과" if passed else "매수우위 과다",
                    "details": {
                        "buy_pressure_ratio": buy_pressure_ratio,
                        "buy_hoga_total": buy_hoga_total,
                        "sell_hoga_total": sell_hoga_total,
                        "sell_pressure_ratio": sell_pressure_ratio,
                    },
                }
            passed = buy_pressure_ratio >= min_buy_ratio
            return {
                "strategy_type": strategy_type,
                "passed": passed,
                "threshold": min_buy_ratio,
                "score": buy_pressure_ratio,
                "reason": "매수우위 통과" if passed else "매수우위 부족",
                "details": {
                    "buy_pressure_ratio": buy_pressure_ratio,
                    "buy_hoga_total": buy_hoga_total,
                    "sell_hoga_total": sell_hoga_total,
                    "sell_pressure_ratio": sell_pressure_ratio,
                },
            }

        return {
            "strategy_type": strategy_type,
            "passed": True,
            "threshold": 0,
            "score": None,
            "reason": "MVP 기본 통과 ({0})".format(mode),
        }
