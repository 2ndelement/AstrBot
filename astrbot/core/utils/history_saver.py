import json

from astrbot import logger
from astrbot.core.conversation_mgr import ConversationManager
from astrbot.core.platform.astr_message_event import AstrMessageEvent
from astrbot.core.provider.entities import ProviderRequest


async def persist_agent_history(
    conversation_manager: ConversationManager,
    *,
    event: AstrMessageEvent,
    req: ProviderRequest,
    summary_note: str,
) -> None:
    """Persist agent interaction into conversation history.

    Fix: always re-fetch the latest history from DB before appending, to
    avoid the race-condition where a CronJob snapshot overwrites newer
    user-bot turns that were saved while the cron was running.
    """
    if not req or not req.conversation:
        return

    cid = req.conversation.cid
    umo = event.unified_msg_origin

    # Re-fetch the freshest history from DB (avoids stale-snapshot overwrite)
    history: list[dict] = []
    try:
        fresh_conv = await conversation_manager.get_conversation(umo, cid)
        if fresh_conv:
            history = json.loads(fresh_conv.history or "[]")
        else:
            # Fallback: use the snapshot embedded in req (better than nothing)
            logger.warning(
                "persist_agent_history: could not re-fetch conversation %s, "
                "falling back to snapshot history.",
                cid,
            )
            history = json.loads(req.conversation.history or "[]")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to load fresh conversation history: %s", exc)

    history.append({"role": "user", "content": "Output your last task result below."})
    history.append({"role": "assistant", "content": summary_note})
    await conversation_manager.update_conversation(
        umo,
        cid,
        history=history,
    )
