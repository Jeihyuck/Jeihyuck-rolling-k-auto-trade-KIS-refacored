from trader.kr.runner.session_policy import exit_code_for_result

def test_exit_codes():
    assert exit_code_for_result({'status':'FAIL','reason':'KR_PREP_ARTIFACT_MISSING'})==2
    assert exit_code_for_result({'status':'SKIP','reason':'PREOPEN_NO_ORDER'})==2
    assert exit_code_for_result({'status':'OK','reason':'OK'})==0
