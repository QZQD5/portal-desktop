import hashlib
import re

from .store import now_ms


def clean(value, limit):
    return re.sub(r"\s+", " ", value).strip()[:limit]


def matches(values, needle):
    exact = [value for value in values if value.lower() == needle.lower()]
    return exact or [
        value for value in values if needle.lower() in value.lower() or value.lower() in needle.lower()
    ]


def execute(store, args):
    lists = store.get("notes_v2")
    if lists is None:
        lists = {name: {"items": items, "updated_at": 0} for name, items in store.get("notes", {}).items()}
    action, name = args["action"], clean(args.get("list", ""), 30)
    if action == "lists":
        return {
            "status": "ok" if lists else "empty",
            "lists": [
                {"list": key, "count": len(value["items"]), "updated_at": value["updated_at"]}
                for key, value in sorted(lists.items(), key=lambda item: item[1]["updated_at"], reverse=True)
            ],
        }
    if not name:
        raise ValueError("List name required")
    candidates = matches(lists, name)
    if len(candidates) > 1:
        return {"status": "ambiguous", "candidates": candidates[:10]}
    if candidates:
        name = candidates[0]
    elif action != "add":
        return {"status": "not_found", "candidates": list(lists)[:10]}
    elif len(lists) >= 20:
        return {"status": "list_full", "list": name}
    entry = lists.setdefault(name, {"items": [], "updated_at": now_ms()})
    if action == "show":
        return {
            "status": "ok",
            "list": name,
            "items": [
                {"id": "item_" + hashlib.sha256(text.encode()).hexdigest()[:12], "text": text}
                for text in entry["items"]
            ],
        }
    result = {"status": "ok", "list": name}
    items = [clean(item, 100) for item in args.get("items", []) if item.strip()][:20]
    if action in {"add", "remove"} and not items:
        raise ValueError("Items required")
    if action == "add":
        added, duplicates = [], []
        for item in items:
            if len(entry["items"]) >= 100:
                break
            if item.lower() in [text.lower() for text in entry["items"]]:
                duplicates.append(item)
            else:
                entry["items"].append(item)
                added.append(item)
        result.update(added=added, duplicates=duplicates)
        if not added and not duplicates:
            return {"status": "list_full", "list": name}
        if added:
            entry["updated_at"] = now_ms()
    elif action == "remove":
        removed, missing, ambiguous = [], [], []
        for item in items:
            found = matches(entry["items"], item)
            if len(found) == 1:
                entry["items"].remove(found[0])
                removed.append(found[0])
            elif found:
                ambiguous.append({"text": item, "candidates": found[:10]})
            else:
                missing.append(item)
        result.update(
            status="ambiguous" if ambiguous else "ok" if removed else "not_found",
            removed=removed,
            not_found=missing,
            ambiguous=ambiguous,
        )
        if removed:
            entry["updated_at"] = now_ms()
    elif action == "clear":
        result["removed"] = len(entry["items"])
        if entry["items"]:
            entry.update(items=[], updated_at=now_ms())
    elif action == "drop":
        del lists[name]
    store.put("notes_v2", lists)
    return result
