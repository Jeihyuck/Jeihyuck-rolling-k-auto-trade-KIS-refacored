import pytest

from scripts.kr_assert_no_skips import check


def test_completed_junit_passes(tmp_path):
    path = tmp_path / "result.xml"
    path.write_text('<testsuites><testsuite><testcase name="actual"/></testsuite></testsuites>')
    check(str(path))


def test_skipped_junit_fails(tmp_path):
    path = tmp_path / "result.xml"
    path.write_text('<testsuite><testcase name="missing"><skipped message="no DB"/></testcase></testsuite>')
    with pytest.raises(SystemExit, match="1 skipped"):
        check(str(path))


def test_empty_junit_fails(tmp_path):
    path = tmp_path / "result.xml"
    path.write_text("<testsuites/>")
    with pytest.raises(SystemExit, match="no collected"):
        check(str(path))
