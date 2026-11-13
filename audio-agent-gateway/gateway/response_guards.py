"""Gateway-owned envelopes must not become assistant-authored conversation facts."""

import re

RESERVED_ENVELOPE = re.compile(
    r"</?(?:gateway_system_event|permission_request|background_work_progress|restored_context)(?:\s|>)",
    re.IGNORECASE,
)
