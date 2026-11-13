"""Provider-neutral frame validation; S2S has no image-buffer transport."""

import base64
import time


class VisualInput:
    def __init__(self, forward=lambda image: False):
        self.forward = forward
        self.accepted = self.dropped = 0
        self.reset()

    def reset(self):
        self.last_at = self.last_occurred = None

    def append(self, event):
        if event.get("media_type", "image/jpeg") != "image/jpeg":
            raise ValueError("视觉帧必须是 JPEG 图片")
        occurred = event.get("occurred_at")
        if occurred is not None and (type(occurred) is not int or occurred < 0):
            raise ValueError("Invalid visual frame timestamp")
        at = time.monotonic()
        reason = ""
        if occurred is not None and self.last_occurred is not None and occurred < self.last_occurred:
            reason = "out_of_order"
        elif self.last_at is not None and at - self.last_at < 1:
            reason = "rate_limited"
        else:
            image = str(event.get("image", "")).strip()
            if len(image) > 256 * 1024:
                raise ValueError("视觉帧超过 256 KiB 限制")
            try:
                raw = base64.b64decode(image, validate=True)
            except ValueError:
                raise ValueError("视觉帧不是有效的 Base64 数据") from None
            if len(raw) < 4 or not raw.startswith(b"\xff\xd8") or not raw.endswith(b"\xff\xd9"):
                raise ValueError("视觉帧必须是完整的 JPEG 图片")
            if self.forward(image) is False:
                reason = "provider_unsupported"
        if reason:
            self.dropped += 1
            return {"accepted": False, "reason": reason}
        self.last_at = at
        if occurred is not None:
            self.last_occurred = occurred
        self.accepted += 1
        return {"accepted": True}
