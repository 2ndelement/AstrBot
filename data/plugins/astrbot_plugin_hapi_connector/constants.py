"""HAPI 常量定义"""

# 各 flavor 对应的权限模式
PERMISSION_MODES = {
    "claude": ["default", "acceptEdits", "bypassPermissions", "plan"],
    "codex": ["default", "read-only", "safe-yolo", "yolo"],
    "gemini": ["default", "read-only", "safe-yolo", "yolo"],
    "opencode": ["default", "yolo"],
}

# Claude 可用的模型模式
MODEL_MODES = ["default", "sonnet", "opus"]

# 支持的 Agent 类型
AGENTS = ["claude", "codex", "gemini", "opencode"]

# Session 类型
SESSION_TYPES = ["simple", "worktree"]
