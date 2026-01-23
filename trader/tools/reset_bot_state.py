import argparse
import os
import pathlib
import shutil
import stat


def rm(path: pathlib.Path) -> None:
    p = pathlib.Path(path)
    if p.is_symlink() or p.is_file():
        p.unlink(missing_ok=True)
    elif p.is_dir():
        shutil.rmtree(p, ignore_errors=True)


def chmod_rw(root: pathlib.Path) -> None:
    root = pathlib.Path(root)
    for p in [root] + list(root.rglob("*")):
        try:
            m = p.stat().st_mode
            os.chmod(p, m | stat.S_IWUSR | stat.S_IRUSR)
        except Exception:
            pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bot-state-dir", required=True)
    ap.add_argument("--keep-universe-lkg", type=int, default=1)
    ap.add_argument("--yes", action="store_true")
    args = ap.parse_args()
    if not args.yes:
        raise SystemExit("Add --yes to proceed")

    bs = pathlib.Path(args.bot_state_dir)

    events_dir = bs / "runtime" / "events"
    if events_dir.exists():
        for path in events_dir.glob("*.jsonl"):
            rm(path)
    rm(events_dir)
    rm(bs / "runtime")
    rm(bs / "trader_ledger")
    rm(bs / "locks")
    rm(bs / "state.json")

    if args.keep_universe_lkg:
        (bs / "universe_lkg").mkdir(parents=True, exist_ok=True)

    (bs / "runtime").mkdir(parents=True, exist_ok=True)
    (bs / "runtime" / "diagnostics").mkdir(parents=True, exist_ok=True)
    (bs / "trader_ledger").mkdir(parents=True, exist_ok=True)
    (bs / "locks").mkdir(parents=True, exist_ok=True)
    chmod_rw(bs)
    print(f"[RESET] bot_state reset done: {bs}")


if __name__ == "__main__":
    main()
