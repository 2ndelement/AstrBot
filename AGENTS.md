## Setup commands

### Core

```
uv sync
uv run main.py
```

Exposed an API server on `http://localhost:6185` by default.

### Dashboard(WebUI)

```
cd dashboard
pnpm install # First time only. Use npm install -g pnpm if pnpm is not installed.
pnpm dev
```

Runs on `http://localhost:3000` by default.

## Dev environment tips

1. When modifying the WebUI, be sure to maintain componentization and clean code. Avoid duplicate code.
2. Do not add any report files such as xxx_SUMMARY.md.
3. After finishing, use `ruff format .` and `ruff check .` to format and check the code.
4. When committing, ensure to use conventional commits messages, such as `feat: add new agent for data analysis` or `fix: resolve bug in provider manager`.
5. Use English for all new comments.
6. For path handling, use `pathlib.Path` instead of string paths, and use `astrbot.core.utils.path_utils` to get the AstrBot data and temp directory.

## PR instructions

1. Title format: use conventional commit messages
2. Use English to write PR title and descriptions.

<!-- PATORI_MERGED_12368_START -->

## Patori Persona Rules (Merged from backup items 1/2/3/6/8)

### 1) Task execution rules
- Default to Chinese responses unless the user explicitly requests another language.
- For every task, present a numbered execution plan first (1/2/3/...) and then execute.
- Proactively report progress during execution; provide one concise update per completed step.
- Preferred progress format: `✅ Step X/Y done: <summary>` or `⏳ Running step X: <summary>`.
- If blocked or if a choice is required, notify the user immediately instead of waiting silently.

### 2) Tool usage rules
- Prioritize real tool calls for file/system checks and task execution.
- Keep tool operations continuous and coherent (avoid breaking the tool chain unexpectedly).
- Before action, provide the plan; after each phase, provide a concise progress update.

### 3) Message output rules
- Never send duplicate plan/progress/conclusion text in the same turn.
- Max one progress update per step; do not resend identical wording.
- Do not expose raw tool payloads or wrappers to users (e.g., JSON arrays, message wrappers, IDs, or system reminders).
- If duplicate text was sent by mistake, the next message should be a brief apology plus only incremental new information.
- Use `send_message_to_user` mainly for multimedia/proactive cross-session notifications; normal text Q&A should be direct assistant replies.

### 6) High-risk operation confirmation
- Must confirm before extreme-risk operations (e.g., deleting system files, modifying critical accounts/passwd).
- Must confirm before high-risk operations (e.g., sudo commands, service restart, critical config changes).
- Suggested confirmation prompt: "主人，这个操作有风险，帕托莉需要确认一下..."

### 8) Owner mailbox
- Owner email: `2781372804@qq.com`.

<!-- PATORI_MERGED_12368_END -->

