from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger, FunctionTool

# 根据仓库结构，从 tools.image_tool 导入图片提取函数
# 注意：我们不再需要导入 GetImageFromContextTool，因为工具逻辑已经移入下方的主类中
from .tools.image_tool import extract_images_from_event


@register(
    "astrbot_plugin_image_url_base64_to_mcp",
    "Thetail001",
    "帮助 MCP 工具从上下文中获取图片 URL 或 Base64 数据。",
    "1.1.5",
)
class ImageContextPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        # 移除了 self.context.add_llm_tools(...)
        # 现在工具通过下方的 @filter.llm_tool 自动注册，并自动绑定到本插件

    # ==================== 核心修改：将工具定义移入插件类 ====================
    @filter.llm_tool(name="get_image_from_context")
    async def get_image_from_context(
        self,
        event: AstrMessageEvent,
        return_type: str = "url",
        look_back_limit: int = 5,
    ):
        """
        从当前的对话上下文中获取图片数据。

        Args:
            return_type (string): 返回类型。'url' (默认) 返回图片链接；'base64' 返回用于注入的占位符。
            look_back_limit (int): 查找历史消息的最大条数。默认为 5。
        Returns:
            如果成功，返回图片 URL 或 Base64 占位符。如果失败，返回错误信息。
        """
        # 根据 LLM 的需求决定是否强制转换 Base64
        prefer_base64 = return_type == "base64"

        # 调用 tools/image_tool.py 中的逻辑
        images = await extract_images_from_event(
            event, look_back_limit=look_back_limit, prefer_base64=prefer_base64
        )

        if not images:
            return "Error: 上下文中未找到任何图片。"

        # 获取第一张图片
        image_data = images[0]
        img_type = image_data.get("type")
        img_content = image_data.get("data")

        # 逻辑：如果 LLM 要 URL 且我们有 URL
        if return_type == "url" and img_type == "url":
            return img_content  # 直接返回 URL，LLM 最容易处理

        # 逻辑：如果 LLM 要 Base64，或者我们只有 Base64
        # 返回占位符，并引导 LLM 将其传递给其他工具
        placeholder = "base64://ASTRBOT_PLUGIN_CACHE_PENDING"
        return f"Success: 图片已准备好。请在调用后续工具时，将图片参数设置为: {placeholder}"

    # ======================================================================

    @filter.on_using_llm_tool()
    async def on_tool_use(
        self, event: AstrMessageEvent, tool: FunctionTool, tool_args: dict
    ):
        """
        Intercepts tool execution BEFORE it happens.
        Injects real Base64 data if placeholders or empty args are found.
        """
        # 防止递归调用自己
        if tool.name == "get_image_from_context":
            return

        target_keys = ["image", "image_url", "url", "img", "base64", "file", "data"]
        should_inject = False
        target_key = None

        for key in target_keys:
            if key in tool_args:
                val = tool_args[key]
                # 检查特殊占位符或空值
                if (
                    val == "base64://ASTRBOT_PLUGIN_CACHE_PENDING"
                    or val == "IMAGE_DATA_READY_INTERNAL"
                ):
                    should_inject = True
                    target_key = key
                    break
                elif not val or val in ["placeholder", "image"]:
                    should_inject = True
                    target_key = key
                    break

        if should_inject:
            logger.info(
                f"[ImageContextPlugin] Intercepting tool '{tool.name}'. Injecting image data..."
            )

            # Hook phase: We MUST force download/convert to Base64 to satisfy MCP
            images = await extract_images_from_event(event, prefer_base64=True)
            if images:
                img_data = images[0]["data"]
                img_type = images[0]["type"]

                val_to_inject = img_data
                # 自动补全 data URI scheme，方便前端或某些工具识别
                if img_type == "base64" and not img_data.startswith("data:"):
                    val_to_inject = f"data:image/jpeg;base64,{img_data}"

                tool_args[target_key] = val_to_inject
                logger.info(
                    f"[ImageContextPlugin] Injection SUCCESS for '{target_key}'. Len: {len(val_to_inject)}"
                )
            else:
                logger.warning(
                    "[ImageContextPlugin] Failed to fetch images for injection."
                )

    @filter.command("test_get_image")
    async def test_get_image(self, event: AstrMessageEvent):
        """测试命令：检查是否能从当前消息提取图片"""
        images = await extract_images_from_event(event)
        yield event.plain_result(f"Found images: {len(images)}")
