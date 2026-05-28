import os
import threading
from datetime import datetime, timezone


# Temporary monitor for soilteste_1 diagnostics. Remove this module and its hooks
# when tp557-errors collection is no longer needed.
TP557_ERROR_PIVOT_ID = "soilteste_1"
TP557_ERROR_TOPIC = "tp557-errors"
TP557_ERROR_LOG_FILENAME = "soilteste_1_tp557-errors.txt"

_write_lock = threading.Lock()


def get_tp557_error_log_path(log_dir="logs_mqtt"):
    safe_log_dir = str(log_dir or "logs_mqtt").strip() or "logs_mqtt"
    return os.path.join(safe_log_dir, TP557_ERROR_LOG_FILENAME)


def record_tp557_error_message(payload, *, topic=TP557_ERROR_TOPIC, ts=None, log_dir="logs_mqtt"):
    try:
        safe_ts = float(ts if ts is not None else datetime.now(timezone.utc).timestamp())
    except (TypeError, ValueError):
        safe_ts = datetime.now(timezone.utc).timestamp()

    timestamp = datetime.fromtimestamp(safe_ts, timezone.utc).isoformat()
    safe_topic = str(topic or TP557_ERROR_TOPIC).strip() or TP557_ERROR_TOPIC
    payload_text = str(payload if payload is not None else "")
    path = get_tp557_error_log_path(log_dir=log_dir)

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    entry = (
        f"timestamp={timestamp}\n"
        f"topic={safe_topic}\n"
        "payload:\n"
        f"{payload_text}\n"
        "---\n"
    )

    with _write_lock:
        with open(path, "a", encoding="utf-8", newline="\n") as file:
            file.write(entry)

    return path


def read_tp557_error_log(log_dir="logs_mqtt"):
    path = get_tp557_error_log_path(log_dir=log_dir)
    if not os.path.exists(path):
        return ""
    with _write_lock:
        with open(path, "r", encoding="utf-8", errors="replace") as file:
            return file.read()
