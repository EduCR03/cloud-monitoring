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


def list_tp557_error_events(log_dir="logs_mqtt"):
    content = read_tp557_error_log(log_dir=log_dir)
    if not content.strip():
        return []

    events = []
    chunks = content.split("\n---\n")
    for index, chunk in enumerate(chunks):
        raw_chunk = chunk.strip("\n")
        if not raw_chunk.strip():
            continue
        lines = raw_chunk.splitlines()
        timestamp = ""
        topic = TP557_ERROR_TOPIC
        payload_lines = []
        reading_payload = False
        for line in lines:
            if reading_payload:
                payload_lines.append(line)
                continue
            if line.startswith("timestamp="):
                timestamp = line[len("timestamp=") :].strip()
                continue
            if line.startswith("topic="):
                topic = line[len("topic=") :].strip() or TP557_ERROR_TOPIC
                continue
            if line == "payload:":
                reading_payload = True

        ts = 0.0
        if timestamp:
            try:
                ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
            except ValueError:
                ts = 0.0
        payload = "\n".join(payload_lines)
        events.append(
            {
                "id": f"tp557-errors-{index + 1}",
                "type": "tp557_errors",
                "topic": topic,
                "source_topic": topic,
                "ts": ts,
                "at": timestamp,
                "summary": "Mensagem recebida em tp557-errors.",
                "raw_payload": payload,
                "details": {"source_topic": topic, "raw_payload": payload},
            }
        )

    return sorted(events, key=lambda item: float(item.get("ts") or 0), reverse=True)
