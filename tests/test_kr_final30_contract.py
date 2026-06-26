from trader.kr.artifacts import normalize_and_validate_scored_final30


def make_valid_scored_final30_rows(n=30, as_of="2026-06-19"):
    return [
        {
            "symbol": f"{i:06d}",
            "as_of": as_of,
            "rank": i,
            "final_score": 100 - i,
            "tech_score": 50 + i,
            "breakout_score": 1.0,
            "pullback_score": 1.0,
            "momentum_score": 1.0,
            "entry_style_selected": "ENTRY_PULLBACK",
            "ma20": 100.0 + i,
            "ma50": 90.0 + i,
            "ma150": 80.0 + i,
            "rs_pctile": 80.0,
            "vcp_score": 1.0,
            "atr_pct": 0.03,
            "close": 120.0 + i,
        }
        for i in range(1, n + 1)
    ]


def test_canonical_final30_validated_rows_are_accepted_by_engine_guard():
    rows = make_valid_scored_final30_rows(30)
    normalized, contract = normalize_and_validate_scored_final30(
        rows,
        expected_as_of="2026-06-19",
        require_exact_rows=30,
        source="kr_canonical_artifact",
    )
    assert len(normalized) == 30
    assert contract["contract_ok"] == 1
    assert contract["usable"] == 1
    assert contract["locked"] == 1
    assert normalized[0]["code"] == "000001"
    assert normalized[0]["score_final"] == 99
    assert normalized[0]["rs_percentile"] == 80.0
    assert normalized[0]["rank_final30"] == 1


def test_canonical_final30_missing_critical_columns_aborts():
    rows = make_valid_scored_final30_rows(30)
    for row in rows:
        row.pop("ma20", None)
    _, contract = normalize_and_validate_scored_final30(
        rows,
        expected_as_of="2026-06-19",
        require_exact_rows=30,
        source="kr_canonical_artifact",
    )
    assert contract["contract_ok"] == 0
    assert contract["usable"] == 0
    assert "ma20" in contract["missing_critical_fields"]


def test_canonical_final30_blank_code_and_symbol_rejected():
    rows = make_valid_scored_final30_rows(30)
    rows[0].pop("code", None)
    rows[0].pop("symbol", None)
    normalized, contract = normalize_and_validate_scored_final30(
        rows, expected_as_of="2026-06-19", require_exact_rows=30, source="kr_canonical_artifact"
    )
    assert contract["contract_ok"] == 0
    assert contract["usable"] == 0
    assert "invalid_code" in contract["reasons"]
    assert contract["invalid_code_count"] == 1
    assert normalized[0]["code"] == ""


def test_canonical_final30_zero_code_rejected():
    rows = make_valid_scored_final30_rows(30)
    rows[0]["symbol"] = "000000"
    _, contract = normalize_and_validate_scored_final30(
        rows, expected_as_of="2026-06-19", require_exact_rows=30, source="kr_canonical_artifact"
    )
    assert contract["contract_ok"] == 0
    assert "invalid_code" in contract["reasons"]


def test_canonical_final30_masked_code_rejected():
    rows = make_valid_scored_final30_rows(30)
    rows[0]["symbol"] = "***6360"
    _, contract = normalize_and_validate_scored_final30(
        rows, expected_as_of="2026-06-19", require_exact_rows=30, source="kr_canonical_artifact"
    )
    assert contract["contract_ok"] == 0
    assert "invalid_code" in contract["reasons"]


def test_canonical_final30_duplicate_code_rejected():
    rows = make_valid_scored_final30_rows(30)
    rows[1]["symbol"] = rows[0]["symbol"]
    _, contract = normalize_and_validate_scored_final30(
        rows, expected_as_of="2026-06-19", require_exact_rows=30, source="kr_canonical_artifact"
    )
    assert contract["contract_ok"] == 0
    assert "code_unique_not_30" in contract["reasons"]


def test_canonical_final30_short_numeric_code_is_padded():
    rows = make_valid_scored_final30_rows(30)
    rows[0]["symbol"] = "6360"
    normalized, contract = normalize_and_validate_scored_final30(
        rows, expected_as_of="2026-06-19", require_exact_rows=30, source="kr_canonical_artifact"
    )
    assert contract["contract_ok"] == 1
    assert normalized[0]["code"] == "006360"


def test_shared_final30_contract_reassigns_rank_and_meta_rank():
    from trader.contracts.final30_contract import assert_final30_contract

    input_rows = [
        {
            "code": f"{i:06d}",
            "name": f"stock{i}",
            "as_of": "2026-06-19",
            "rank": 0,
            "rank_final30": 0,
            "final_score": float(100 - i),
            "meta": {"rank": 0, "rank_final30": 0},
        }
        for i in range(1, 31)
    ]

    rows, info = assert_final30_contract(
        input_rows,
        as_of="2026-06-19",
        env="practice",
        source="test.final30_contract",
        require_count=30,
    )

    assert len(rows) == 30
    assert [r["rank_final30"] for r in rows] == list(range(1, 31))
    assert [r["meta"]["rank_final30"] for r in rows] == list(range(1, 31))
    assert info["ok"] is True
    assert info["contract_hash"]
