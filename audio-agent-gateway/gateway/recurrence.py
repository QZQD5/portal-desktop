"""Calendar recurrence anchored to the original wall clock, including DST gaps."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def next_occurrence(start, recurrence, now, zone="UTC"):
    if recurrence not in {"daily", "weekly", "weekdays"}:
        return None
    try:
        timezone = ZoneInfo(zone)
    except (ValueError, KeyError):
        timezone = ZoneInfo("UTC")
    initial = datetime.fromtimestamp(start / 1000, timezone).replace(tzinfo=None)
    current = datetime.fromtimestamp(now / 1000, timezone).replace(tzinfo=None)
    step = 7 if recurrence == "weekly" else 1
    days = 0 if start > now else max(step, ((current.date() - initial.date()).days + step - 1) // step * step)
    for _ in range(8):
        target = initial + timedelta(days=days)
        if recurrence != "weekdays" or target.weekday() < 5:
            candidates = [target.replace(tzinfo=timezone, fold=fold).timestamp() for fold in (0, 1)]
            exact = [
                value
                for value in candidates
                if datetime.fromtimestamp(value, timezone).replace(tzinfo=None) == target
            ]
            # Repeated hours choose the first instant; skipped hours move forward.
            candidate = min(exact) if exact else max(candidates)
            if round(candidate * 1000) > now:
                return round(candidate * 1000)
        days += step
    return None
