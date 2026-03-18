# -*- coding: utf-8 -*-
import datetime
import json
import os
import sqlite3
import threading


class PersistenceManager(object):
    def __init__(self, paths):
        self.paths = paths
        self._conn = None
        self._lock = threading.RLock()

    def connect(self):
        if self._conn is None:
            self._conn = sqlite3.connect(self.paths.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        return self._conn

    def initialize(self):
        conn = self.connect()
        with self._lock:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    account_no TEXT PRIMARY KEY,
                    account_name TEXT,
                    is_enabled INTEGER DEFAULT 0,
                    is_primary INTEGER DEFAULT 0,
                    settings_json TEXT DEFAULT '{}',
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS condition_catalog (
                    condition_id TEXT PRIMARY KEY,
                    condition_index INTEGER,
                    condition_name TEXT,
                    is_available INTEGER DEFAULT 1,
                    extra_json TEXT DEFAULT '{}',
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS active_condition_slots (
                    slot_no INTEGER PRIMARY KEY,
                    condition_id TEXT,
                    is_enabled INTEGER DEFAULT 0,
                    is_realtime INTEGER DEFAULT 0,
                    current_count INTEGER DEFAULT 0,
                    last_event_at TEXT,
                    settings_json TEXT DEFAULT '{}',
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS strategy_definitions (
                    strategy_id TEXT PRIMARY KEY,
                    strategy_kind TEXT,
                    strategy_type TEXT,
                    strategy_name TEXT,
                    enabled INTEGER DEFAULT 1,
                    scope_type TEXT DEFAULT 'global',
                    scope_targets_json TEXT DEFAULT '[]',
                    params_json TEXT DEFAULT '{}',
                    extra_json TEXT DEFAULT '{}',
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS strategy_chain_items (
                    chain_item_id TEXT PRIMARY KEY,
                    strategy_id TEXT,
                    chain_name TEXT,
                    chain_order INTEGER,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS tracked_symbols (
                    code TEXT PRIMARY KEY,
                    name TEXT,
                    first_detected_at TEXT,
                    last_detected_at TEXT,
                    expire_at TEXT,
                    detected_price REAL DEFAULT 0,
                    current_state TEXT,
                    is_holding INTEGER DEFAULT 0,
                    has_open_order INTEGER DEFAULT 0,
                    news_watch_priority INTEGER DEFAULT 0,
                    last_news_checked_at TEXT,
                    last_news_sent_at TEXT,
                    last_important_news_at TEXT,
                    source_conditions_json TEXT DEFAULT '[]',
                    buy_attempt_count INTEGER DEFAULT 0,
                    is_spam INTEGER DEFAULT 0,
                    extra_json TEXT DEFAULT '{}',
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS symbol_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date TEXT,
                    ts TEXT,
                    code TEXT,
                    name TEXT,
                    event_type TEXT,
                    source_condition_slot INTEGER,
                    source_condition_name TEXT,
                    account_scope TEXT,
                    payload_json TEXT DEFAULT '{}',
                    extra_json TEXT DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_symbol_events_code_ts ON symbol_events(code, ts);
                CREATE INDEX IF NOT EXISTS idx_symbol_events_type_ts ON symbol_events(event_type, ts);

                CREATE TABLE IF NOT EXISTS trade_cycles (
                    cycle_id TEXT PRIMARY KEY,
                    trade_date TEXT,
                    account_no TEXT,
                    code TEXT,
                    name TEXT,
                    entry_detected_at TEXT,
                    buy_order_at TEXT,
                    buy_filled_at TEXT,
                    sell_signal_at TEXT,
                    sell_order_at TEXT,
                    sell_filled_at TEXT,
                    source_conditions_json TEXT DEFAULT '[]',
                    buy_filters_json TEXT DEFAULT '[]',
                    sell_filters_json TEXT DEFAULT '[]',
                    news_scores_json TEXT DEFAULT '{}',
                    status TEXT,
                    pnl_realized REAL DEFAULT 0,
                    extra_json TEXT DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_trade_cycles_trade_date_acc ON trade_cycles(trade_date, account_no);
                CREATE INDEX IF NOT EXISTS idx_trade_cycles_code_status ON trade_cycles(code, status);

                CREATE TABLE IF NOT EXISTS positions (
                    account_no TEXT,
                    code TEXT,
                    name TEXT,
                    qty INTEGER DEFAULT 0,
                    avg_price REAL DEFAULT 0,
                    current_price REAL DEFAULT 0,
                    eval_profit REAL DEFAULT 0,
                    eval_rate REAL DEFAULT 0,
                    buy_chain_id TEXT,
                    active_sell_state_json TEXT DEFAULT '{}',
                    updated_at TEXT,
                    PRIMARY KEY (account_no, code)
                );

                CREATE TABLE IF NOT EXISTS open_orders (
                    account_no TEXT,
                    order_no TEXT,
                    code TEXT,
                    name TEXT,
                    order_status TEXT,
                    order_qty INTEGER DEFAULT 0,
                    unfilled_qty INTEGER DEFAULT 0,
                    filled_qty INTEGER DEFAULT 0,
                    order_price REAL DEFAULT 0,
                    order_gubun TEXT,
                    order_time TEXT,
                    updated_at TEXT,
                    raw_json TEXT DEFAULT '{}',
                    PRIMARY KEY (account_no, order_no)
                );

                CREATE INDEX IF NOT EXISTS idx_open_orders_code ON open_orders(code, account_no);
                CREATE INDEX IF NOT EXISTS idx_open_orders_unfilled ON open_orders(account_no, unfilled_qty);

                CREATE TABLE IF NOT EXISTS order_policy_logs (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT,
                    cycle_id TEXT,
                    account_no TEXT,
                    code TEXT,
                    name TEXT,
                    policy TEXT,
                    stage TEXT,
                    action TEXT,
                    detail_json TEXT DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_order_policy_logs_cycle ON order_policy_logs(cycle_id, ts);
                CREATE INDEX IF NOT EXISTS idx_order_policy_logs_code ON order_policy_logs(code, account_no, ts);

                CREATE TABLE IF NOT EXISTS daily_account_summary (
                    trade_date TEXT,
                    account_no TEXT,
                    eval_profit_total REAL DEFAULT 0,
                    realized_profit_total REAL DEFAULT 0,
                    holding_count INTEGER DEFAULT 0,
                    sold_count INTEGER DEFAULT 0,
                    extra_json TEXT DEFAULT '{}',
                    PRIMARY KEY (trade_date, account_no)
                );

                CREATE TABLE IF NOT EXISTS daily_trade_review_summary (
                    trade_date TEXT,
                    account_no TEXT,
                    snapshot_ts TEXT,
                    holding_eval_total REAL DEFAULT 0,
                    realized_profit_total REAL DEFAULT 0,
                    total_pnl REAL DEFAULT 0,
                    holding_count INTEGER DEFAULT 0,
                    sold_count INTEGER DEFAULT 0,
                    is_finalized INTEGER DEFAULT 0,
                    extra_json TEXT DEFAULT '{}',
                    PRIMARY KEY (trade_date, account_no)
                );

                CREATE INDEX IF NOT EXISTS idx_daily_trade_review_summary_date ON daily_trade_review_summary(trade_date, account_no);

                CREATE TABLE IF NOT EXISTS daily_trade_review_items (
                    item_id TEXT PRIMARY KEY,
                    trade_date TEXT,
                    account_no TEXT,
                    snapshot_ts TEXT,
                    row_type TEXT,
                    code TEXT,
                    name TEXT,
                    avg_price REAL DEFAULT 0,
                    ref_price REAL DEFAULT 0,
                    eval_profit REAL DEFAULT 0,
                    realized_profit REAL DEFAULT 0,
                    contribution_profit REAL DEFAULT 0,
                    strategy_text TEXT DEFAULT '',
                    condition_name TEXT DEFAULT '',
                    cycle_id TEXT DEFAULT '',
                    item_status TEXT DEFAULT '',
                    extra_json TEXT DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_daily_trade_review_items_date ON daily_trade_review_items(trade_date, account_no, row_type);

                CREATE TABLE IF NOT EXISTS naver_api_keys (
                    key_set_id INTEGER PRIMARY KEY,
                    client_id TEXT,
                    client_secret TEXT,
                    enabled INTEGER DEFAULT 0,
                    priority INTEGER DEFAULT 0,
                    daily_limit INTEGER DEFAULT 25000,
                    daily_used INTEGER DEFAULT 0,
                    daily_error_count INTEGER DEFAULT 0,
                    last_used_at TEXT,
                    last_error TEXT,
                    cooldown_until TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS telegram_channels (
                    channel_group TEXT,
                    slot_no INTEGER,
                    bot_token TEXT,
                    chat_id TEXT,
                    enabled INTEGER DEFAULT 0,
                    is_required INTEGER DEFAULT 0,
                    updated_at TEXT,
                    PRIMARY KEY (channel_group, slot_no)
                );

                CREATE TABLE IF NOT EXISTS news_search_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date TEXT,
                    ts TEXT,
                    code TEXT,
                    name TEXT,
                    trigger_type TEXT,
                    trigger_condition_slot INTEGER,
                    query_text TEXT,
                    key_set_id INTEGER,
                    result_count INTEGER DEFAULT 0,
                    status TEXT,
                    extra_json TEXT DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS news_articles (
                    article_hash TEXT PRIMARY KEY,
                    trade_date TEXT,
                    code TEXT,
                    name TEXT,
                    article_title TEXT,
                    article_url TEXT,
                    press_name TEXT,
                    published_at TEXT,
                    summary_text TEXT,
                    importance_score REAL DEFAULT 0,
                    frequency_score REAL DEFAULT 0,
                    final_score REAL DEFAULT 0,
                    is_sent INTEGER DEFAULT 0,
                    sent_channels_json TEXT DEFAULT '[]',
                    extra_json TEXT DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_news_articles_code_pub ON news_articles(code, published_at);
                CREATE INDEX IF NOT EXISTS idx_news_articles_score ON news_articles(final_score);

                CREATE TABLE IF NOT EXISTS news_recheck_queue (
                    queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT,
                    reason TEXT,
                    scheduled_at TEXT,
                    priority INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    extra_json TEXT DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_news_recheck_queue_status_priority ON news_recheck_queue(status, priority, scheduled_at);

                CREATE TABLE IF NOT EXISTS spam_symbols (
                    code TEXT PRIMARY KEY,
                    name TEXT,
                    added_at TEXT,
                    reason TEXT,
                    memo TEXT,
                    block_trade INTEGER DEFAULT 1,
                    block_news_send INTEGER DEFAULT 1,
                    block_news_search INTEGER DEFAULT 1,
                    extra_json TEXT DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS telegram_send_logs (
                    send_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT,
                    channel_group TEXT,
                    slot_no INTEGER,
                    target_chat_id TEXT,
                    message_kind TEXT,
                    related_code TEXT,
                    send_status TEXT,
                    error_message TEXT,
                    extra_json TEXT DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS telegram_trade_sessions (
                    user_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    selected_account_no TEXT DEFAULT '',
                    current_menu TEXT DEFAULT '',
                    pending_action_json TEXT DEFAULT '{}',
                    last_message_id TEXT DEFAULT '',
                    updated_at TEXT,
                    PRIMARY KEY (user_id, chat_id)
                );

                CREATE TABLE IF NOT EXISTS telegram_trade_action_logs (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT,
                    user_id TEXT,
                    chat_id TEXT,
                    account_no TEXT,
                    action_type TEXT,
                    target_type TEXT,
                    target_value TEXT,
                    result TEXT,
                    message TEXT,
                    extra_json TEXT DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS user_runtime_profiles (
                    user_id TEXT PRIMARY KEY,
                    profile_json TEXT DEFAULT '{}',
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS slot_strategy_policy (
                    slot_no INTEGER PRIMARY KEY,
                    buy_expression_json TEXT DEFAULT '[]',
                    sell_strategy_nos_json TEXT DEFAULT '[]',
                    news_min_score INTEGER DEFAULT 0,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS default_strategy_policy (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    buy_expression_json TEXT DEFAULT '[]',
                    sell_strategy_nos_json TEXT DEFAULT '[]',
                    news_min_score INTEGER DEFAULT 0,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS news_trade_policy (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    enabled INTEGER DEFAULT 0,
                    min_score INTEGER DEFAULT 80,
                    sell_strategy_nos_json TEXT DEFAULT '[]',
                    updated_at TEXT
                );
                """
            )
            self._seed_defaults(conn)
            self._migrate_schema(conn)
            conn.commit()

    def _seed_defaults(self, conn):
        now = self.now_ts()
        for slot_no in range(1, 11):
            conn.execute(
                """
                INSERT OR IGNORE INTO active_condition_slots (
                    slot_no, condition_id, is_enabled, is_realtime, current_count, last_event_at, settings_json, updated_at
                ) VALUES (?, NULL, 0, 0, 0, NULL, '{}', ?)
                """,
                (slot_no, now),
            )
        for key_set_id in range(1, 7):
            conn.execute(
                """
                INSERT OR IGNORE INTO naver_api_keys (
                    key_set_id, enabled, priority, updated_at
                ) VALUES (?, 0, ?, ?)
                """,
                (key_set_id, key_set_id, now),
            )
        for channel_group in ["news", "trade"]:
            for slot_no in range(1, 4):
                conn.execute(
                    """
                    INSERT OR IGNORE INTO telegram_channels (
                        channel_group, slot_no, enabled, is_required, updated_at
                    ) VALUES (?, ?, 0, ?, ?)
                    """,
                    (channel_group, slot_no, 1 if slot_no == 1 else 0, now),
                )
        conn.execute(
            """
            INSERT OR IGNORE INTO default_strategy_policy (
                id, buy_expression_json, sell_strategy_nos_json, news_min_score, updated_at
            ) VALUES (1, '[]', '[]', 0, ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO news_trade_policy (
                id, enabled, min_score, sell_strategy_nos_json, updated_at
            ) VALUES (1, 0, 80, '[]', ?)
            """,
            (now,),
        )

    def _migrate_schema(self, conn):
        self._ensure_column(conn, 'accounts', 'settings_json', "TEXT DEFAULT '{}'" )
        self._ensure_column(conn, 'trade_cycles', 'extra_json', "TEXT DEFAULT '{}'" )
        self._ensure_column(conn, 'positions', 'active_sell_state_json', "TEXT DEFAULT '{}'" )
        self._ensure_column(conn, 'tracked_symbols', 'detected_price', "REAL DEFAULT 0" )
        self._ensure_column(conn, 'strategy_definitions', 'strategy_no', "INTEGER" )
        self._ensure_column(conn, 'strategy_definitions', 'is_assignable_to_slot', "INTEGER DEFAULT 1" )
        self._ensure_column(conn, 'strategy_definitions', 'is_news_filter', "INTEGER DEFAULT 0" )
        self._ensure_column(conn, 'strategy_definitions', 'is_news_trade', "INTEGER DEFAULT 0" )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS slot_strategy_policy (
                slot_no INTEGER PRIMARY KEY,
                buy_expression_json TEXT DEFAULT '[]',
                sell_strategy_nos_json TEXT DEFAULT '[]',
                news_min_score INTEGER DEFAULT 0,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS default_strategy_policy (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                buy_expression_json TEXT DEFAULT '[]',
                sell_strategy_nos_json TEXT DEFAULT '[]',
                news_min_score INTEGER DEFAULT 0,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS news_trade_policy (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                enabled INTEGER DEFAULT 0,
                min_score INTEGER DEFAULT 80,
                sell_strategy_nos_json TEXT DEFAULT '[]',
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_trade_sessions (
                user_id TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                selected_account_no TEXT DEFAULT '',
                current_menu TEXT DEFAULT '',
                pending_action_json TEXT DEFAULT '{}',
                last_message_id TEXT DEFAULT '',
                updated_at TEXT,
                PRIMARY KEY (user_id, chat_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_trade_action_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                user_id TEXT,
                chat_id TEXT,
                account_no TEXT,
                action_type TEXT,
                target_type TEXT,
                target_value TEXT,
                result TEXT,
                message TEXT,
                extra_json TEXT DEFAULT '{}'
            )
            """
        )
        self._seed_policy_defaults(conn)
        self._seed_strategy_flags(conn)
        self._seed_strategy_numbers(conn)

    def _ensure_column(self, conn, table_name, column_name, ddl):
        rows = conn.execute("PRAGMA table_info(%s)" % table_name).fetchall()
        names = set([row[1] for row in rows])
        if column_name not in names:
            conn.execute("ALTER TABLE {0} ADD COLUMN {1} {2}".format(table_name, column_name, ddl))

    def _seed_policy_defaults(self, conn):
        now = self.now_ts()
        conn.execute(
            """
            INSERT OR IGNORE INTO default_strategy_policy (
                id, buy_expression_json, sell_strategy_nos_json, news_min_score, updated_at
            ) VALUES (1, '[]', '[]', 0, ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO news_trade_policy (
                id, enabled, min_score, sell_strategy_nos_json, updated_at
            ) VALUES (1, 0, 80, '[]', ?)
            """,
            (now,),
        )

    def _seed_strategy_flags(self, conn):
        conn.execute(
            "UPDATE strategy_definitions SET is_news_filter=CASE WHEN strategy_type='news_filter' THEN 1 ELSE COALESCE(is_news_filter, 0) END"
        )
        conn.execute(
            "UPDATE strategy_definitions SET is_news_trade=CASE WHEN strategy_type='news_trade' THEN 1 ELSE COALESCE(is_news_trade, 0) END"
        )
        conn.execute(
            "UPDATE strategy_definitions SET is_assignable_to_slot=CASE WHEN strategy_type='news_trade' THEN 0 ELSE COALESCE(is_assignable_to_slot, 1) END"
        )

    def _seed_strategy_numbers(self, conn):
        for kind in ['buy', 'sell']:
            rows = conn.execute(
                """
                SELECT s.strategy_id, s.strategy_no
                FROM strategy_definitions s
                LEFT JOIN strategy_chain_items c ON c.strategy_id = s.strategy_id
                WHERE s.strategy_kind=?
                ORDER BY COALESCE(c.chain_order, 9999), s.created_at, s.strategy_id
                """,
                (kind,),
            ).fetchall()
            used_numbers = set()
            for row in rows:
                try:
                    number = int(row[1] or 0)
                except Exception:
                    number = 0
                if number > 0:
                    used_numbers.add(number)
            next_no = 1
            for row in rows:
                try:
                    current_no = int(row[1] or 0)
                except Exception:
                    current_no = 0
                if current_no > 0:
                    continue
                while next_no in used_numbers:
                    next_no += 1
                conn.execute(
                    "UPDATE strategy_definitions SET strategy_no=? WHERE strategy_id=?",
                    (next_no, row[0]),
                )
                used_numbers.add(next_no)
                next_no += 1

    def now_ts(self):
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def today_str(self):
        return datetime.date.today().isoformat()

    def execute(self, sql, params=None):
        params = params or ()
        with self._lock:
            conn = self.connect()
            cur = conn.execute(sql, params)
            conn.commit()
            return cur

    def executemany(self, sql, rows):
        with self._lock:
            conn = self.connect()
            cur = conn.executemany(sql, rows)
            conn.commit()
            return cur

    def fetchall(self, sql, params=None):
        params = params or ()
        with self._lock:
            cur = self.connect().execute(sql, params)
            return cur.fetchall()

    def fetchone(self, sql, params=None):
        params = params or ()
        with self._lock:
            cur = self.connect().execute(sql, params)
            return cur.fetchone()

    def write_event(self, event_type, payload):
        trade_date = self.today_str()
        ts = self.now_ts()
        record = {
            "ts": ts,
            "event": event_type,
            "payload": payload,
        }
        filename = os.path.join(self.paths.data_dir, "events_{0}.jsonl".format(trade_date))
        with self._lock:
            with open(filename, "a", encoding="utf-8") as fp:
                fp.write(json.dumps(record, ensure_ascii=False) + "\n")

    def save_runtime_state(self, state):
        with self._lock:
            with open(self.paths.runtime_state_path, "w", encoding="utf-8") as fp:
                json.dump(state, fp, ensure_ascii=False, indent=2)

    def load_runtime_state(self):
        if not os.path.exists(self.paths.runtime_state_path):
            return {}
        with open(self.paths.runtime_state_path, "r", encoding="utf-8") as fp:
            return json.load(fp)
