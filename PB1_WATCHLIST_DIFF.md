# PB1 Watchlist Cache - Diff 요약

## 1. trader/db/schema.py

```diff
@@ schema.py 중간 부분
     price_daily = sa.Table(
         "price_daily",
         metadata,
         sa.Column("market", sa.String, nullable=False),
         sa.Column("code", sa.String, nullable=False),
         sa.Column("date", sa.Date, nullable=False),
         sa.Column("open", sa.Numeric, nullable=True),
         sa.Column("high", sa.Numeric, nullable=True),
         sa.Column("low", sa.Numeric, nullable=True),
         sa.Column("close", sa.Numeric, nullable=True),
         sa.Column("volume", sa.Numeric, nullable=True),
         sa.Column("value", sa.Numeric, nullable=True),
         sa.Column("source", sa.String, nullable=False, default="KIS"),
         sa.PrimaryKeyConstraint("market", "code", "date"),
     )
 
+    pb1_watchlist = sa.Table(
+        "pb1_watchlist",
+        metadata,
+        sa.Column("env", sa.String, nullable=False),
+        sa.Column("strategy", sa.String, nullable=False),
+        sa.Column("as_of", sa.Date, nullable=False),
+        sa.Column("code", sa.String, nullable=False),
+        sa.Column("rank", sa.Integer, nullable=False),
+        sa.Column("score", sa.Float, nullable=True),
+        sa.Column("meta", sa.JSON, nullable=True),
+        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
+        sa.PrimaryKeyConstraint("env", "strategy", "as_of", "code"),
+        sa.Index("ix_pb1_watchlist_lookup", "env", "strategy", "as_of"),
+    )
+
     return SchemaTables(
         database_url=database_url,
         metadata=metadata,
         runs=runs,
         universe_runs=universe_runs,
         universe_members=universe_members,
         universe_current=universe_current,
         orders=orders,
         fills=fills,
         positions=positions,
         ledger_events=ledger_events,
         reconcile_log=reconcile_log,
         price_daily=price_daily,
+        pb1_watchlist=pb1_watchlist,
         uses_native_uuid=uses_native_uuid,
     )

@@ SchemaTables dataclass
 @dataclass(frozen=True)
 class SchemaTables:
     database_url: str
     metadata: sa.MetaData
     runs: sa.Table
     universe_runs: sa.Table
     universe_members: sa.Table
     universe_current: sa.Table
     orders: sa.Table
     fills: sa.Table
     positions: sa.Table
     ledger_events: sa.Table
     reconcile_log: sa.Table
     price_daily: sa.Table
+    pb1_watchlist: sa.Table
     uses_native_uuid: bool

@@ 하단 exports
 DEFAULT_DATABASE_URL = os.getenv("PBCORE_DB_URL", "postgresql+psycopg://")
 DEFAULT_SCHEMA = schema_for_url(DEFAULT_DATABASE_URL)
 
 METADATA = DEFAULT_SCHEMA.metadata
 RUNS = DEFAULT_SCHEMA.runs
 UNIVERSE_RUNS = DEFAULT_SCHEMA.universe_runs
 UNIVERSE_MEMBERS = DEFAULT_SCHEMA.universe_members
 UNIVERSE_CURRENT = DEFAULT_SCHEMA.universe_current
 ORDERS = DEFAULT_SCHEMA.orders
 FILLS = DEFAULT_SCHEMA.fills
 POSITIONS = DEFAULT_SCHEMA.positions
 LEDGER_EVENTS = DEFAULT_SCHEMA.ledger_events
 RECONCILE_LOG = DEFAULT_SCHEMA.reconcile_log
 PRICE_DAILY = DEFAULT_SCHEMA.price_daily
+PB1_WATCHLIST = DEFAULT_SCHEMA.pb1_watchlist
```

## 2. trader/db/repos.py

```diff
@@ imports
 from .schema import (
     FILLS,
     LEDGER_EVENTS,
     ORDERS,
     POSITIONS,
     PRICE_DAILY,
     RUNS,
     UNIVERSE_MEMBERS,
     UNIVERSE_CURRENT,
     UNIVERSE_RUNS,
+    PB1_WATCHLIST,
     SchemaTables,
     schema_for_engine,
     uuid_value_for_url,
 )

@@ __all__
 __all__ = [
     "RunsRepo",
     "UniverseRepo",
     "OrdersRepo",
     "FillsRepo",
     "PositionsRepo",
     "LedgerEventsRepo",
     "ReconcileLogRepo",
     "PositionRepo",  # Backward compatibility
+    "WatchlistRepo",
+    "save_watchlist",
+    "load_watchlist",
 ]

@@ 파일 끝
 # Backward compatibility alias
 PositionRepo = PositionsRepo
 
+
+# ========================================
+# PB1 Watchlist Repository
+# ========================================
+
+class WatchlistRepo:
+    """PB1 Watchlist 전용 repo."""
+    
+    def __init__(self, engine: Engine):
+        self.engine = engine
+        self._schema = schema_for_engine(engine)
+    
+    def save_watchlist(
+        self,
+        *,
+        env: str,
+        strategy: str,
+        as_of: date,
+        members: List[Dict[str, Any]],
+    ) -> None:
+        """Watchlist를 DB에 upsert."""
+        if not members:
+            logger.warning("[WATCHLIST][SAVE] empty members -> skip")
+            return
+        
+        schema = self._schema
+        with self.engine.begin() as conn:
+            # 기존 당일 watchlist 삭제
+            delete_stmt = sa.delete(schema.pb1_watchlist).where(
+                and_(
+                    schema.pb1_watchlist.c.env == env,
+                    schema.pb1_watchlist.c.strategy == strategy,
+                    schema.pb1_watchlist.c.as_of == as_of,
+                )
+            )
+            conn.execute(delete_stmt)
+            
+            # 새 watchlist 삽입
+            rows = [
+                {
+                    "env": env,
+                    "strategy": strategy,
+                    "as_of": as_of,
+                    "code": str(m["code"]).zfill(6),
+                    "rank": m.get("rank", 0),
+                    "score": m.get("score"),
+                    "meta": json_sanitize(m.get("meta")),
+                }
+                for m in members
+            ]
+            if rows:
+                conn.execute(sa.insert(schema.pb1_watchlist), rows)
+        
+        logger.info(
+            "[WATCHLIST][SAVE] env=%s strategy=%s as_of=%s members=%s",
+            env, strategy, as_of, len(members)
+        )
+    
+    def load_watchlist(
+        self,
+        *,
+        env: str,
+        strategy: str,
+        as_of: date,
+    ) -> List[Dict[str, Any]]:
+        """특정 날짜의 watchlist 조회."""
+        schema = self._schema
+        with self.engine.connect() as conn:
+            stmt = (
+                select(schema.pb1_watchlist)
+                .where(
+                    and_(
+                        schema.pb1_watchlist.c.env == env,
+                        schema.pb1_watchlist.c.strategy == strategy,
+                        schema.pb1_watchlist.c.as_of == as_of,
+                    )
+                )
+                .order_by(schema.pb1_watchlist.c.rank)
+            )
+            rows = conn.execute(stmt).fetchall()
+        
+        result = [
+            {
+                "code": row.code,
+                "rank": row.rank,
+                "score": float(row.score) if row.score else None,
+                "meta": row.meta,
+            }
+            for row in rows
+        ]
+        logger.info(
+            "[WATCHLIST][LOAD] env=%s strategy=%s as_of=%s members=%s",
+            env, strategy, as_of, len(result)
+        )
+        return result
+    
+    def get_latest_watchlist_date(
+        self,
+        *,
+        env: str,
+        strategy: str,
+    ) -> Optional[date]:
+        """가장 최근 watchlist의 as_of 날짜 반환."""
+        schema = self._schema
+        with self.engine.connect() as conn:
+            stmt = (
+                select(func.max(schema.pb1_watchlist.c.as_of))
+                .where(
+                    and_(
+                        schema.pb1_watchlist.c.env == env,
+                        schema.pb1_watchlist.c.strategy == strategy,
+                    )
+                )
+            )
+            result = conn.execute(stmt).scalar()
+        return result
+
+
+def save_watchlist(
+    engine: Engine,
+    *,
+    env: str,
+    strategy: str,
+    as_of: date,
+    members: List[Dict[str, Any]],
+) -> None:
+    """Standalone save_watchlist function."""
+    repo = WatchlistRepo(engine)
+    repo.save_watchlist(env=env, strategy=strategy, as_of=as_of, members=members)
+
+
+def load_watchlist(
+    engine: Engine,
+    *,
+    env: str,
+    strategy: str,
+    as_of: date,
+) -> List[Dict[str, Any]]:
+    """Standalone load_watchlist function."""
+    repo = WatchlistRepo(engine)
+    return repo.load_watchlist(env=env, strategy=strategy, as_of=as_of)
```

## 3. trader/pb1_engine.py

```diff
@@ imports
-from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo, UniverseRepo
+from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo, UniverseRepo, WatchlistRepo
 from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
 from trader.kis_wrapper import KisAPI
 from trader.ledger.store import LedgerStore
 from trader.universe.validation import validate_tradeable
 from trader.factors.liquidity_risk import gap_filter, liquidity_filter, range_filter, spread_proxy_filter
 from trader.factors.regime import get_regime, risk_multiplier
 from trader.factors.rs_rank import rank_rs
 from trader.positioning.minervini_risk import calc_initial_stop, calc_position_size, update_exits
 from trader.minervini.report import run_minervini_report
 from trader.setups.vcp_pro import PriceTightRules, VolContractRules, find_pivot, is_vcp_ready, score_vcp
 from trader.strategies.pb1_minervini_v2 import (
 ...
 from trader.eventlog import emit_event
 from trader.utils.env import env_bool
 from trader.utils.json_sanitize import to_jsonable
 from trader.window_router import WindowDecision
 from trader.diagnostics.spool import spool_event
+from trader.watchlist_builder import load_today_watchlist_with_fallback

@@ _load_universe 메서드 뒤에 추가
     def _load_universe(self) -> list[dict]:
         if self._universe_context is not None:
             members = list(self._universe_context.members or [])
             self._universe_as_of = self._universe_context.as_of_date or (
                 members[0].get("as_of_date") if members else None
             )
             self._universe_path = self._universe_context.selected_path
         else:
             members = self.universe_repo.get_current_universe_members(self.env, self.UNIVERSE_STRATEGY)
             self._universe_as_of = members[0].get("as_of_date") if members else None
         self._code_name_map = {
             str(m.get("code") or "").zfill(6): (m.get("name") or (m.get("meta_json") or {}).get("name"))
             for m in members or []
             if m.get("code")
         }
         return members
 
+    def _load_today_watchlist_members(self) -> tuple[list[dict], str]:
+        """오늘 watchlist를 로드하고, 없으면 fallback 전략 적용."""
+        watchlist_enabled = os.getenv("PB1_WATCHLIST_ENABLED", "1") == "1"
+        if not watchlist_enabled:
+            logger.info("[PB1][WATCHLIST] disabled -> use full universe")
+            members = self._load_universe()
+            return members, "universe_full"
+        
+        # 전체 유니버스 로드 (watchlist 생성/fallback에 필요)
+        full_members = self._load_universe()
+        
+        # Watchlist 로드
+        try:
+            watchlist, source = load_today_watchlist_with_fallback(
+                engine=self.engine,
+                env=self.env,
+                strategy=self.strategy,
+                today=self._today,
+                members=full_members,
+                ohlcv_provider=self._fetch_daily,
+                minervini_config=self.minervini_config,
+            )
+        except Exception as exc:
+            logger.error("[PB1][WATCHLIST][LOAD_FAIL] err=%s -> fallback to full universe", exc)
+            return full_members, "universe_fallback"
+        
+        if not watchlist:
+            logger.warning("[PB1][WATCHLIST] empty -> fallback to full universe")
+            return full_members, "universe_fallback_empty"
+        
+        # Watchlist를 members 형식으로 변환
+        members = [
+            {
+                "code": w["code"],
+                "rank": w.get("rank"),
+                "name": self._code_name_map.get(w["code"], ""),
+                "meta_json": w.get("meta"),
+            }
+            for w in watchlist
+        ]
+        
+        logger.info(
+            "[PB1][WATCHLIST] size=%s source=%s as_of=%s",
+            len(members), source, self._today
+        )
+        return members, source

@@ run() 메서드 내 entry scan 전
+        # ✅ Watchlist 적용 여부 결정
+        watchlist_enabled = os.getenv("PB1_WATCHLIST_ENABLED", "1") == "1"
+        if watchlist_enabled and self.phase in {"prep", "entry"}:
+            logger.info("[PB1][WATCHLIST] enabled -> load today watchlist")
+            # Watchlist로 members 대체
+            members, watchlist_source = self._load_today_watchlist_members()
+            logger.info(
+                "[PB1][WATCHLIST] loaded=%s source=%s",
+                len(members), watchlist_source
+            )
+        else:
+            # 기존 로직: 전체 유니버스 사용
+            members = self._load_universe()
+            logger.info("[PB1][UNIVERSE] loaded=%s (watchlist disabled)", len(members))
+        
         if self.phase in {"prep", "entry"} and self._now_kst > entry_cutoff_dt:
             skip_entry_scan = True
             entry_allowed = False
             entry_reason = "entry_cutoff"
             logger.info(
                 "[PB1][SKIP_ENTRY] reason=entry_cutoff now=%s cutoff=%s",
                 self._now_kst.isoformat(),
                 entry_cutoff_dt.isoformat(),
             )
             if self.phase in {"prep", "entry"}:
                 final_status = "SKIPPED"
                 final_notes = "entry_cutoff"
```

## 4. trader/config.py

```diff
@@ PB1 config 섹션
 PB1_MAX_POSITIONS = int(_cfg("PB1_MAX_POSITIONS") or "8")
 PB1_MIN_SCORE_BASE = float(_cfg("PB1_MIN_SCORE_BASE") or "70")
 PB1_MIN_SCORE_FLOOR = float(_cfg("PB1_MIN_SCORE_FLOOR") or "55")
 PB1_MIN_SCORE_STEP = float(_cfg("PB1_MIN_SCORE_STEP") or "5")
 PB1_MIN_SCORE = float(_cfg("PB1_MIN_SCORE") or str(PB1_MIN_SCORE_BASE))
 PB1_FAILMODE_SOFT = env_bool("PB1_FAILMODE_SOFT", default=True)
 PB1_MIN_CANDIDATES = int(_cfg("PB1_MIN_CANDIDATES") or "3")
 PB1_RELAX_MAX_PASSES = int(_cfg("PB1_RELAX_MAX_PASSES") or "3")
 PB1_SPREAD_HARD_MAX_PCT = float(_cfg("PB1_SPREAD_HARD_MAX_PCT") or "0")
 PB1_GAP_HARD_MAX_PCT = float(_cfg("PB1_GAP_HARD_MAX_PCT") or "0")
 PB1_ENTRY_BUDGET_PCT_PER_TICK = float(_cfg("PB1_ENTRY_BUDGET_PCT_PER_TICK") or "0.25")
 PB1_MAX_POS_PCT = float(_cfg("PB1_MAX_POS_PCT") or "0.20")
 PB1_USE_RISK_PARITY = _cfg_bool("PB1_USE_RISK_PARITY", fallback=True)
 PB1_MAX_ATR_PCT = float(_cfg_with_alias("PB1_MAX_ATR_PCT", "PB1_ATR_PCT_MAX") or "8.0")
 PB1_MIN_VALUE20 = float(_cfg("PB1_MIN_VALUE20") or "3000000000")
 PB1_ALLOW_ADD_TO_EXISTING = _cfg_bool("PB1_ALLOW_ADD_TO_EXISTING")
 PB1_LOG_ENTRY_GATE = _cfg_bool("PB1_LOG_ENTRY_GATE", fallback=True)
 PB1_LOG_DROP_REASONS_TOPN = int(_cfg("PB1_LOG_DROP_REASONS_TOPN") or "10")
 PB1_OHLCV_DAYS_BASE = int(_cfg("PB1_OHLCV_DAYS_BASE") or "200")
+
+# === [NEW] PB1 Watchlist 환경변수 ===
+PB1_WATCHLIST_ENABLED = _cfg_bool("PB1_WATCHLIST_ENABLED", fallback=True)
+PB1_WATCHLIST_TOPK = int(_cfg("PB1_WATCHLIST_TOPK") or "50")
+PB1_WATCHLIST_FINALN = int(_cfg("PB1_WATCHLIST_FINALN") or "30")
+PB1_WATCHLIST_MIN_PRICE = float(_cfg("PB1_WATCHLIST_MIN_PRICE") or "2000")
+PB1_WATCHLIST_LIQ_DAYS = int(_cfg("PB1_WATCHLIST_LIQ_DAYS") or "20")
+PB1_WATCHLIST_MIN_ROWS = int(_cfg("PB1_WATCHLIST_MIN_ROWS") or "30")
+PB1_WATCHLIST_FORCE_REBUILD = _cfg_bool("PB1_WATCHLIST_FORCE_REBUILD", fallback=False)
+
 # 추가 상수
 ALLOW_KIS_DAILY_FALLBACK = _cfg_bool("ALLOW_KIS_DAILY_FALLBACK", fallback=False)

@@ logger.info PB1 config
 logger.info(
-    "[CONFIG][PB1] entry_mode=%s require_both=%s entry_cond_mode=%s entry_budget_pct=%.2f max_pos_pct=%.2f vol_max=%.2f volu_max=%.2f volu_max_intraday=%.2f pullback_min=%.3f pullback_max=%.3f require_both_contractions=%s min_score_base=%.1f min_score_floor=%.1f min_score_step=%.1f failmode_soft=%s relax_passes=%s min_candidates=%s",
+    "[CONFIG][PB1] entry_mode=%s require_both=%s entry_cond_mode=%s entry_budget_pct=%.2f max_pos_pct=%.2f vol_max=%.2f volu_max=%.2f volu_max_intraday=%.2f pullback_min=%.3f pullback_max=%.3f require_both_contractions=%s min_score_base=%.1f min_score_floor=%.1f min_score_step=%.1f failmode_soft=%s relax_passes=%s min_candidates=%s watchlist_enabled=%s watchlist_topk=%s watchlist_finaln=%s",
     PB1_ENTRY_MODE,
     int(PB1_REQUIRE_BOTH),
     ENTRY_COND_MODE,
     PB1_ENTRY_BUDGET_PCT_PER_TICK,
     PB1_MAX_POS_PCT,
     PB1_VOL_MAX,
     PB1_VOLU_MAX,
     PB1_VOLU_MAX_INTRADAY,
     PB1_PULLBACK_MIN,
     PB1_PULLBACK_MAX,
     int(PB1_REQUIRE_BOTH_CONTRACTIONS),
     PB1_MIN_SCORE_BASE,
     PB1_MIN_SCORE_FLOOR,
     PB1_MIN_SCORE_STEP,
     int(PB1_FAILMODE_SOFT),
     PB1_RELAX_MAX_PASSES,
     PB1_MIN_CANDIDATES,
+    int(PB1_WATCHLIST_ENABLED),
+    PB1_WATCHLIST_TOPK,
+    PB1_WATCHLIST_FINALN,
 )
```

## 5. .github/workflows/trade-runner.yml

```diff
@@ trade_tick job env
       UNIVERSE_SOURCE: "DB_ONLY"
       BOTSTATE_DISABLED: "1"
       SQLITE_DISABLED: "1"
 
+      # PB1 Watchlist configuration
+      PB1_WATCHLIST_ENABLED: "1"
+      PB1_WATCHLIST_TOPK: "50"
+      PB1_WATCHLIST_FINALN: "30"
+      PB1_WATCHLIST_MIN_PRICE: "2000"
+      PB1_WATCHLIST_LIQ_DAYS: "20"
+      PB1_WATCHLIST_MIN_ROWS: "30"
+      PB1_WATCHLIST_FORCE_REBUILD: "0"
+
       DB_DISABLE_PREPARED_STATEMENTS: "1"
```

## 6. 신규 파일

### trader/watchlist_builder.py
- 완전히 새로운 파일 (400+ lines)
- Stage A/B 로직 구현
- Fallback 전략 구현

### trader/tools/build_watchlist.py
- CLI 도구 (100+ lines)
- 수동 watchlist 생성/테스트용

### migrations/0020_pb1_watchlist.sql
- DDL migration (30 lines)
- CREATE TABLE, INDEX, COMMENT

### PB1_WATCHLIST_PATCH_SUMMARY.md
- 패치 요약 문서 (150+ lines)

---

## 검증 체크리스트

- [x] DB 스키마에 pb1_watchlist 테이블 추가
- [x] DB repo에 WatchlistRepo + 함수 추가
- [x] Watchlist builder 로직 구현
- [x] pb1_engine.py에 watchlist 통합
- [x] config.py에 환경변수 추가
- [x] CLI 도구 추가
- [x] 마이그레이션 DDL 추가
- [x] GitHub Actions에 env vars 추가
- [x] 문법 오류 없음 (Pylance 검증 완료)
- [x] 예상 로그 예시 작성
- [x] Rollback 전략 문서화
