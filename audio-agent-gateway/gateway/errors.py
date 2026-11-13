class GatewayError(ValueError):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code, self.status = code, status


def tool_failure(code, message, *, status="failed", retryable=False, **details):
    return {
        "status": status,
        "error": True,
        "error_code": code,
        "user_message": message,
        "retryable": retryable,
        **details,
    }
