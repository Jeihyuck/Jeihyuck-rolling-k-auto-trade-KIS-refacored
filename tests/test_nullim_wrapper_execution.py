import os, subprocess
from pathlib import Path
ROOT=Path(__file__).parents[1]
WRAPPERS={'run-kr-prep.sh':('KR','prep'),'run-kr-am.sh':('KR','am'),'run-kr-afternoon.sh':('KR','afternoon'),'run-kr-close.sh':('KR','close'),'run-us-prep.sh':('US','prep'),'run-us-prep-recovery.sh':('US','prep_recovery'),'check-us-prep-before-am.sh':('US','am_preflight'),'run-us-am.sh':('US','am'),'run-us-afternoon.sh':('US','afternoon'),'run-us-close.sh':('US','close')}
def test_preflight_only_wrappers_ignore_stale_paths_and_log_metadata(tmp_path):
 for script,(market,session) in WRAPPERS.items():
  env={**os.environ,'NULLIM_APP_DIR':'/old/path','NULLIM_RESOLVED_REPO_ROOT':'/another/old/path','NULLIM_PREFLIGHT_ONLY':'1','ALLOW_STALE_CODE':'1','ALLOW_DIRTY_CODE':'1'}
  r=subprocess.run(['bash',str(ROOT/'scripts/wsl'/script)],cwd=tmp_path,env=env,text=True,capture_output=True)
  assert r.returncode==0,r.stderr+r.stdout
  assert 'STALE_ENV_IGNORED' in r.stderr and '[DEPLOY][OK]' in r.stdout
  line=(ROOT/'runtime/logs/deploy-preflight.log').read_text().splitlines()[-1]
  assert f'market={market}' in line and f'session={session}' in line
def test_reassertion_overwrites_dotenv_path_values(tmp_path):
    command='source scripts/wsl/nullim-repo-root.sh; NULLIM_APP_DIR=/old/from-dotenv; NULLIM_RESOLVED_REPO_ROOT=/old/resolved; nullim_reassert_repo_root scripts/wsl/run-kr-am.sh; printf "%s|%s" "$NULLIM_APP_DIR" "$NULLIM_RESOLVED_REPO_ROOT"'
    r=subprocess.run(['bash','-c',command],cwd=ROOT,text=True,capture_output=True,check=True)
    assert r.stdout == f'{ROOT}|{ROOT}'
    assert 'STALE_ENV_AFTER_DOTENV_IGNORED' in r.stderr
