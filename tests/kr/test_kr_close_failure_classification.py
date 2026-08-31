import subprocess
from pathlib import Path


CLASSIFIER = Path("scripts/wsl/kr-close-failure-classifier.sh")


def classify(log: str, rc: int) -> list[str]:
    command = f'source "{CLASSIFIER}"; classify_kr_close_failure "$1" "$2"'
    result = subprocess.run(["bash", "-c", command, "classifier", log, str(rc)],
                            check=True, capture_output=True, text=True)
    return result.stdout.strip().split("\t")


def test_kis_temporary_close_failure_becomes_rc75():
    assert classify("KisBalanceUnavailable: balance timed out", 2) == [
        "KIS_BALANCE_TIMEOUT", "1", "75", "RETRYABLE_DEGRADED"]


def test_python_failure_retains_original_rc():
    assert classify("Traceback: NameError: broken_program", 1) == [
        "NON_KIS_RUNTIME_FAILURE", "0", "1", "FAIL"]


def test_db_schema_failure_retains_original_rc():
    assert classify("ProgrammingError: relation orders does not exist", 2) == [
        "NON_KIS_RUNTIME_FAILURE", "0", "2", "FAIL"]
