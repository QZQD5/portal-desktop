import json

from .config import ROOT

RESOURCES = ROOT / "gateway/resources"


def frontend_instructions(prompt, runtime):
    profile = (RESOURCES / "ASSISTANT.md").read_text(encoding="utf-8").strip()[:4000]
    if not prompt or not profile:
        raise ValueError("Frontend prompt and ASSISTANT.md must not be empty")
    shared = [(RESOURCES / name).read_text(encoding="utf-8") for name in (
        "ROUTING-PROMPT.md", "BEING-PROMPT.md", "MEMORY-PROMPT.md",
    )]
    context = json.dumps(runtime, ensure_ascii=False).replace("<", "\\u003c")
    return "\n".join([prompt, *shared, f"<assistant_profile>{profile}</assistant_profile>",
                      f"<runtime_context>{context}</runtime_context>"])
