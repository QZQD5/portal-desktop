"""The child API is private to Desktop's main process."""
import secrets


def authorized(request, settings):
    # Renderer/browser requests cross Desktop IPC; cookies and query keys are not accepted.
    return (
        request.client is not None
        and request.client.host in {"127.0.0.1", "::1", "testclient"}
        and request.url.hostname in {"127.0.0.1", "localhost", "::1", "testserver"}
        and "origin" not in request.headers
        and (
            settings.development
            or (bool(settings.access_token) and secrets.compare_digest(
                request.headers.get("authorization", ""), "Bearer " + settings.access_token
            ))
        )
    )
