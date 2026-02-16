# trader/minervini_runner.py
import os
import runpy

def main():
    """
    Thin wrapper so `python -m trader.minervini_runner` works.

    We reuse the existing prep pipeline by forcing MINERVINI_ONLY=1.
    This keeps all safety/guards/DB logging consistent with production code paths.
    """
    os.environ.setdefault("MINERVINI_ONLY", "1")

    # delegate to prep_runner entrypoint (module execution)
    runpy.run_module("trader.prep_runner", run_name="__main__")


if __name__ == "__main__":
    main()
