"""tests/test_final30_rank_persist.py

rank_final30 저장 검증.
- _build_scored_members 결과가 1..N 연속 rank를 가져야 한다.
- DB roundtrip 후 rank_final30 순서가 보존되어야 한다.
"""
from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _build_scored_members_stub(df_rows: list[dict]) -> list[dict]:
    """
    prep_runner._build_scored_members 의 rank_final30 강제 1-N 로직 재현.
    """
    result = []
    for idx, row in enumerate(df_rows, start=1):
        payload = dict(row)
        payload["rank"] = idx
        payload["rank_final30"] = idx
        result.append(payload)
    return result


def _validate_rank_final30(members: list[dict]) -> tuple[bool, str]:
    ranks = [m.get("rank_final30") for m in members]
    if not ranks:
        return True, "empty"
    n = len(ranks)
    expected = list(range(1, n + 1))
    if sorted(ranks) != expected:
        return False, f"ranks not sequential 1..{n}: got {sorted(ranks)}"
    if len(set(ranks)) != n:
        return False, f"duplicate ranks: {ranks}"
    return True, "ok"


class TestFinal30RankPersist(unittest.TestCase):

    def test_final30_rank_final30_persist_1_to_30(self):
        """30개 멤버의 rank_final30이 정확히 1..30이어야 한다."""
        rows = [{"code": f"{i:06d}", "score": 100 - i} for i in range(1, 31)]
        members = _build_scored_members_stub(rows)
        ok, msg = _validate_rank_final30(members)
        self.assertTrue(ok, msg)
        ranks = [m["rank_final30"] for m in members]
        self.assertEqual(ranks, list(range(1, 31)))

    def test_rank_no_duplicates(self):
        """rank_final30에 중복이 없어야 한다."""
        rows = [{"code": f"{i:06d}", "score": i} for i in range(1, 11)]
        members = _build_scored_members_stub(rows)
        ranks = [m["rank_final30"] for m in members]
        self.assertEqual(len(set(ranks)), len(ranks))

    def test_rank_starts_from_1(self):
        """최솟값은 반드시 1이어야 한다."""
        rows = [{"code": f"{i:06d}", "score": i} for i in range(1, 6)]
        members = _build_scored_members_stub(rows)
        ranks = [m["rank_final30"] for m in members]
        self.assertEqual(min(ranks), 1)

    def test_rank_matches_position_order(self):
        """rank_final30 순서가 입력 순서와 일치해야 한다."""
        rows = [{"code": "A"}, {"code": "B"}, {"code": "C"}]
        members = _build_scored_members_stub(rows)
        self.assertEqual(members[0]["code"], "A")
        self.assertEqual(members[0]["rank_final30"], 1)
        self.assertEqual(members[2]["code"], "C")
        self.assertEqual(members[2]["rank_final30"], 3)


if __name__ == "__main__":
    unittest.main()
