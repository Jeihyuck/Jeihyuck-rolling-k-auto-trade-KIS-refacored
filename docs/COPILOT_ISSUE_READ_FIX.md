# GitHub Copilot Agent `github/issue_read` 진단 경고 해결

VS Code 진단에 아래와 같은 메시지가 보이면, 커스텀 에이전트 프롬프트에서
지원되지 않는 도구 이름(`github/issue_read`)을 참조하고 있다는 뜻입니다.

- `알 수 없는 도구 'github/issue_read'`

## 빠른 해결

아래 스크립트를 실행하면 Ask/Explore/Plan 에이전트 프롬프트 파일에서
`github/issue_read` 토큰을 제거합니다.

```bash
python scripts/fix_copilot_issue_read_tool.py
```

## 특정 파일만 수정

```bash
python scripts/fix_copilot_issue_read_tool.py \
  /home/codespace/.vscode-remote/data/User/globalStorage/github.copilot-chat/ask-agent/Ask.agent.md \
  /home/codespace/.vscode-remote/data/User/globalStorage/github.copilot-chat/explore-agent/Explore.agent.md \
  /home/codespace/.vscode-remote/data/User/globalStorage/github.copilot-chat/plan-agent/Plan.agent.md
```

## 변경 미리보기

```bash
python scripts/fix_copilot_issue_read_tool.py --dry-run
```
