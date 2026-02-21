from __future__ import annotations

import json

from trader.final_list_store import load_final30, save_final30


def test_save_load_and_overwrite_policy(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    env = "practice"
    as_of = "2026-02-20"

    p1 = save_final30(
        env=env,
        as_of=as_of,
        symbols=["005930", "000660"],
        meta={"source": "test"},
        overwrite=False,
    )
    assert p1.exists()
    assert load_final30(env, as_of) == ["005930", "000660"]

    # overwrite=False이면 기존 파일 유지
    p2 = save_final30(
        env=env,
        as_of=as_of,
        symbols=["035420"],
        meta={"source": "new"},
        overwrite=False,
    )
    assert p1 == p2
    assert load_final30(env, as_of) == ["005930", "000660"]

    # overwrite=True일 때만 교체
    save_final30(
        env=env,
        as_of=as_of,
        symbols=["035420"],
        meta={"source": "overwrite"},
        overwrite=True,
    )
    assert load_final30(env, as_of) == ["035420"]

    payload = json.loads(p1.read_text(encoding="utf-8"))
    assert payload["env"] == env
    assert payload["as_of"] == as_of
    assert payload["name"] == "final30"
