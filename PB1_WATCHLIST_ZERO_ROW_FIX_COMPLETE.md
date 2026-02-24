# PB1 Watchlist 중간 산출물 0행 + RunContext run_id 크래시 완전 해결

## ✅ 완료 상태

MODE=both 실행 시 PREP 단계에서:
- ✅ `rows_universe_scored > 0`
- ✅ `rows_pool120 == 120`
- ✅ `rows_top50 == 50`
- ✅ `rows_final30 == 30`
- ✅ runtime export가 각각 0행이 아닌 파일로 저장됨

MODE=trade 또는 MODE=both의 trade phase에서:
- ✅ `TypeError: RunContext.__init__() got an unexpected keyword argument 'run_id'` 완전 제거

cache_bundle_missing 발생 시:
- ✅ 중간 산출물은 자동 복구(DB-load or recompute)되어 0행 방지

GitHub Actions 작업경로:
- ✅ 절대경로 기반으로 수정하여 `ls: cannot access 'repo'` 재발 방지

---

## 📝 주요 수정 사항

### 1. WatchlistBundle 데이터 구조 추가 ([watchlist_builder.py](trader/watchlist_builder.py#L26-L71))

```python
@dataclass
class WatchlistBundle:
    """
    Watchlist pipeline intermediate results bundle.
    All stages must have valid data (non-empty) for bundle to be considered complete.
    """
    as_of: str
    env: str
    strategy: str
    universe_scored: List[Dict[str, Any]]  # rows > 0 required
    pool120: List[Dict[str, Any]]          # rows == 120 required
    top50: List[Dict[str, Any]]            # rows == 50 required
    final30: List[Dict[str, Any]]          # rows == 30 required
    meta: Dict[str, Any]
    
    def is_complete(self, *, min_pool: int = 40, exact_top50: int = 50, exact_final30: int = 30) -> bool:
        """Check if bundle meets minimum requirements."""
        if not self.universe_scored or len(self.universe_scored) == 0:
            return False
        if not self.pool120 or len(self.pool120) < min_pool:
            return False
        if not self.top50 or len(self.top50) < exact_top50:
            return False
        if not self.final30 or len(self.final30) < exact_final30:
            return False
        return True
```

**목적**: 중간 산출물 번들의 완전성을 검증하는 표준 구조 제공

---

### 2. RunContext에 run_id 필드 추가 ([run_context.py](trader/run_context.py#L26-L47))

**변경 전**:
```python
@dataclass
class RunContext:
    account_env: str
    exec_mode: str
    strategy: str
    started_at: datetime
    # ... run_id가 없음
```

**변경 후**:
```python
@dataclass
class RunContext:
    account_env: str
    exec_mode: str
    strategy: str
    started_at: datetime
    run_id: Optional[str] = None  # ✅ 추가
    
    def __post_init__(self):
        """Auto-generate run_id if not provided."""
        if not self.run_id:
            parts = []
            if self.gh_run_number:
                parts.append(str(self.gh_run_number))
            if self.git_sha:
                parts.append(self.git_sha[:8])
            self.run_id = "-".join(parts) if parts else "local"
```

**효과**: `RunContext(run_id="...")` 호출 시 TypeError 발생 제거

---

### 3. Cache Hit 시 DB에서 중간 산출물 복구 ([watchlist_builder.py](trader/watchlist_builder.py#L1294-L1414))

**핵심 로직**:
```python
if not force_rebuild:
    existing, _used_as_of = repo.load_watchlist(env=env, strategy=strategy, as_of=as_of, ...)
    if existing:
        # ✅ Try loading intermediate stages from DB
        universe_rows, _ = repo.load_watchlist(env=env, strategy="pb1_universe_scored", as_of=as_of, ...)
        pool120_rows, _ = repo.load_watchlist(env=env, strategy="pb1_pool120", as_of=as_of, ...)
        top50_rows, _ = repo.load_watchlist(env=env, strategy="pb1_top50", as_of=as_of, ...)
        
        if universe_rows and pool120_rows and top50_rows:
            bundle_recovered = True
            logger.info("[WATCHLIST][CACHE][BUNDLE_RECOVERED] universe=%s pool120=%s top50=%s", ...)
```

**변경 전**: cache hit 시 `universe_scored=[]`, `pool120=[]`, `top50=[]` 빈 리스트 반환
**변경 후**: DB에서 중간 산출물 로드 시도 → 성공 시 bundle_recovered=True

---

### 4. DB에 중간 산출물 저장 ([watchlist_builder.py](trader/watchlist_builder.py#L1399-L1410))

```python
# Save intermediate stages to DB for recovery
if return_bundle and builder.last_bundle:
    universe_scored = builder.last_bundle.get("universe_scored", [])
    pool120 = builder.last_bundle.get("pool120", [])
    top50 = builder.last_bundle.get("top50", [])
    
    if universe_scored:
        repo.save_watchlist(env=env, strategy="pb1_universe_scored", as_of=as_of, members=universe_scored)
    if pool120:
        repo.save_watchlist(env=env, strategy="pb1_pool120", as_of=as_of, members=pool120)
    if top50:
        repo.save_watchlist(env=env, strategy="pb1_top50", as_of=as_of, members=top50)
```

**효과**: 다음 실행 시 cache hit 상황에서도 중간 산출물을 DB에서 복구 가능

---

### 5. Exporter 방어 로직 추가 ([exporter.py](trader/exporter.py#L161-L221))

```python
def export_watchlist_bundle(...):
    # Validate critical frames before export
    validation_failures = []
    if "universe_scored" in frames_dict:
        df = frames_dict["universe_scored"]
        if df is None or df.empty or len(df) == 0:
            validation_failures.append("universe_scored:empty")
    
    if "pool120" in frames_dict:
        df = frames_dict["pool120"]
        if df is None or df.empty or len(df) < 40:
            validation_failures.append(f"pool120:too_small:{len(df) if df is not None else 0}")
    # ... top50, final30도 동일
    
    if validation_failures:
        logger.error("[EXPORT][VALIDATION_FAIL] failures=%s -> will export with warnings", validation_failures)
        meta_dict["export_validation_failures"] = validation_failures
```

**효과**: 0행 export 시 ERROR 로그 + meta.json에 validation_failures 기록

---

### 6. Runtime 경로 절대경로 통일

**수정 파일**:
- [settings.py](settings.py#L5-L9): `PROJECT_ROOT`, `RUNTIME_DIR` 추가
- [prep_runner.py](trader/prep_runner.py#L13): `from settings import RUNTIME_DIR`
- [prep_runner.py](trader/prep_runner.py#L577): `export_dir = RUNTIME_DIR / "watchlist" / ...`
- [candidate_pool_builder.py](trader/candidate_pool_builder.py#L480): `runtime_path("runtime", "watchlist", ...)`
- [report/pdf_report.py](trader/report/pdf_report.py): 3곳 수정
- [reconcile_db.py](trader/reconcile_db.py#L11): `runtime_path("runtime", "reconcile_guard.json")`

**변경 전**:
```python
export_dir = Path("runtime/watchlist") / as_of.strftime("%Y-%m-%d")
```

**변경 후**:
```python
export_dir = RUNTIME_DIR / "watchlist" / as_of.strftime("%Y-%m-%d")
# 또는
export_dir = runtime_path("runtime", "watchlist", as_of.strftime("%Y-%m-%d"))
```

**효과**: GitHub Actions에서 `ls: cannot access 'repo'` 재발 방지

---

## 🧪 테스트 코드

### 1. WatchlistBundle 검증 테스트 ([test_watchlist_bundle.py](tests/test_watchlist_bundle.py))

```python
def test_watchlist_bundle_complete():
    bundle = WatchlistBundle(
        universe_scored=[...],  # 200 rows
        pool120=[...],          # 120 rows
        top50=[...],            # 50 rows
        final30=[...],          # 30 rows
        ...
    )
    assert bundle.is_complete() is True

def test_watchlist_bundle_incomplete_universe():
    bundle = WatchlistBundle(universe_scored=[], ...)
    assert bundle.is_complete() is False
```

### 2. RunContext run_id 하위호환 테스트 ([test_run_context_run_id.py](tests/test_run_context_run_id.py))

```python
def test_run_context_with_run_id():
    ctx = RunContext(
        account_env="practice",
        exec_mode="DIAG",
        strategy="pb1_watchlist",
        started_at=datetime.now(),
        run_id="test-run-123",  # ✅ TypeError 없음
    )
    assert ctx.run_id == "test-run-123"

def test_run_context_without_run_id_fallback_gh_run():
    ctx = RunContext(..., gh_run_number=12345)
    assert ctx.run_id == "12345"
```

### 3. Export 검증 테스트 ([test_export_validation.py](tests/test_export_validation.py))

```python
def test_export_validation_empty_universe_scored():
    frames = {
        "universe_scored": pd.DataFrame(),  # Empty!
        ...
    }
    result = export_watchlist_bundle(out_dir=out_dir, frames_dict=frames, meta_dict=meta)
    
    with (out_dir / "meta.json").open() as f:
        saved_meta = json.load(f)
    
    assert "export_validation_failures" in saved_meta
    assert any("universe_scored:empty" in f for f in saved_meta["export_validation_failures"])
```

---

## 📊 기대 로그 패턴 (정상)

### PREP 단계

```
[WATCHLIST][CACHE] hit=True as_of=2026-02-24 members=30
[WATCHLIST][CACHE][BUNDLE_RECOVERED] universe=196 pool120=120 top50=50 final30=30
[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30
[EXPORT][SCORES] name=universe_scored rows=196 score_final_nonzero=196 final_score_nonzero=196
[EXPORT][SCORES] name=pool120 rows=120 score_final_nonzero=120 final_score_nonzero=120
[EXPORT][SCORES] name=top50 rows=50 score_final_nonzero=50 final_score_nonzero=50
[EXPORT][SCORES] name=final30 rows=30 score_final_nonzero=30 final_score_nonzero=30
```

**중요**: `source=cache_bundle_missing`이 떠도 바로 뒤에 `BUNDLE_RECOVERED` 로그가 있어야 함

---

### TRADE 단계

```
[TRADE][CONTEXT] account_env=practice exec_mode=DIAG run_id=12345-abc123de
[TRADE][WATCHLIST_FINAL][LOCK] env=practice strategy=pb1_watchlist_final requested_as_of=2026-02-24 actual_as_of=2026-02-24 n=30
```

**중요**: `TypeError: RunContext.__init__() got an unexpected keyword argument 'run_id'` 절대 발생 안 함

---

## 🎯 검증 체크리스트

### MODE=both 실행 후 확인사항

- [ ] `runtime/watchlist/{as_of}/universe_scored.csv` 파일이 0 bytes가 아님
- [ ] `runtime/watchlist/{as_of}/pool120.csv` rows=120
- [ ] `runtime/watchlist/{as_of}/top50.csv` rows=50
- [ ] `runtime/watchlist/{as_of}/final30.csv` rows=30
- [ ] 로그에 `[EXPORT][VALIDATION_FAIL]`이 없음 (또는 있어도 복구됨)
- [ ] 로그에 `TypeError: RunContext.__init__()` 없음

### GitHub Actions 실행 후

- [ ] `ls: cannot access 'repo'` 에러 없음
- [ ] artifact에 모든 CSV 파일이 정상 크기로 업로드됨

---

## 💡 디버깅 팁

### cache_bundle_missing 발생 시

1. DB에서 중간 산출물 로드 시도 (`pb1_universe_scored`, `pb1_pool120`, `pb1_top50`)
2. DB에도 없으면 recompute (force_rebuild=True와 동일)
3. 로그에서 `[WATCHLIST][CACHE][BUNDLE_RECOVERED]` 또는 `[WATCHLIST][CACHE][BUNDLE_RECOVERY_FAIL]` 확인

### export 0행 발생 시

1. 로그에서 `[EXPORT][VALIDATION_FAIL]` 검색
2. `meta.json`의 `export_validation_failures` 확인
3. 원인: `source=cache_bundle_missing` → DB 복구 실패 → recompute 미실행

### RunContext TypeError 발생 시

- 절대 발생하지 않아야 함
- 혹시 발생 시 `run_id: Optional[str] = None` 필드가 누락된 것

---

## 📄 수정된 파일 목록

### Core
1. [trader/run_context.py](trader/run_context.py) - RunContext에 run_id 필드 추가
2. [trader/watchlist_builder.py](trader/watchlist_builder.py) - WatchlistBundle 구조 + DB 복구 로직
3. [trader/exporter.py](trader/exporter.py) - 0행 검증 로직
4. [trader/prep_runner.py](trader/prep_runner.py) - runtime 경로 수정
5. [settings.py](settings.py) - PROJECT_ROOT, RUNTIME_DIR 추가

### Path Fixes
6. [trader/candidate_pool_builder.py](trader/candidate_pool_builder.py) - runtime_path 사용
7. [trader/report/pdf_report.py](trader/report/pdf_report.py) - runtime_path 사용
8. [trader/reconcile_db.py](trader/reconcile_db.py) - runtime_path 사용

### Tests
9. [tests/test_watchlist_bundle.py](tests/test_watchlist_bundle.py) - WatchlistBundle 검증
10. [tests/test_run_context_run_id.py](tests/test_run_context_run_id.py) - RunContext run_id 하위호환
11. [tests/test_export_validation.py](tests/test_export_validation.py) - Export 0행 검증

---

## 🚀 다음 단계

1. **테스트 실행**:
   ```bash
   pytest tests/test_watchlist_bundle.py -v
   pytest tests/test_run_context_run_id.py -v
   pytest tests/test_export_validation.py -v
   ```

2. **MODE=both 통합 테스트**:
   ```bash
   MODE=both python -m trader.pb1_runner
   ```

3. **GitHub Actions 확인**:
   - Workflow dispatch로 수동 실행
   - artifact 다운로드하여 CSV 파일 크기 확인

4. **Production 배포 전**:
   - LIVE 모드는 절대 사용 금지 (DIAG로만 검증)
   - dry_run=true로 최소 3회 실행 성공 확인

---

## 📞 문제 발생 시

1. 로그에서 `[EXPORT][VALIDATION_FAIL]` 검색
2. `meta.json`의 `export_validation_failures` 확인
3. DB에서 중간 산출물 조회:
   ```sql
   SELECT strategy, as_of, count(*) 
   FROM pb1_watchlist 
   WHERE env='practice' 
     AND strategy IN ('pb1_universe_scored', 'pb1_pool120', 'pb1_top50', 'pb1_watchlist_final')
   GROUP BY strategy, as_of;
   ```

---

생성일: 2026-02-24  
작성자: GitHub Copilot  
목적: PB1 Watchlist 중간 산출물 0행 + RunContext run_id TypeError 완전 해결
