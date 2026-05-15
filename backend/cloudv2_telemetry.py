import hashlib
import json
import logging
import os
import re
import statistics
import threading
import time
import copy

from backend.cloudv2_db import resolve_database_settings
from backend.cloudv2_dashboard import DATA_DIR, ensure_dirs, slugify, write_json_atomic
from backend.cloudv2_persistence import TelemetryPersistence
from backend.cloudv2_security import get_db_purge_password
from backend.cloudv2_time import ts_to_dashboard_str


TOPIC_CLOUDV2 = "cloudv2"
TOPIC_PING = "cloudv2-ping"
TOPIC_CLOUD2 = "cloud2"
TOPIC_NETWORK = "cloudv2-network"
TOPIC_INFO = "cloudv2-info"
TOPIC_SHUTDOWN = "cloudv2-shutdown"
TOPIC_ERROR = "cloudv2-error"
TOPIC_SCHEDULING = "cloudv2-scheduling"
TOPIC_CONFIG = "cloudv2-config"
EVENT_ONLY_TOPICS = (TOPIC_SHUTDOWN, TOPIC_ERROR, TOPIC_SCHEDULING)
MONITOR_TOPICS = (
    TOPIC_CLOUDV2,
    TOPIC_PING,
    TOPIC_CLOUD2,
    TOPIC_NETWORK,
    TOPIC_INFO,
    TOPIC_SHUTDOWN,
    TOPIC_ERROR,
    TOPIC_SCHEDULING,
    TOPIC_CONFIG,
)
PROBE_RESPONSE_TOPICS = {TOPIC_NETWORK, TOPIC_INFO}
CONNECTIVITY_TOPICS = (TOPIC_CLOUDV2, TOPIC_PING, TOPIC_INFO, TOPIC_NETWORK)
TIMELINE_MINI_BINS = 96
TIMELINE_MINI_WINDOW_SEC = 30 * 24 * 3600
TIMELINE_MINI_EMPTY_FALLBACK_SEC = 24 * 3600
SUMMARY_CARDS_HISTORY_WINDOW_SEC = 30 * 24 * 3600
SUMMARY_CARDS_HISTORY_BUCKET_SEC = 3600

STATUS_LABELS = {
    "green": "Online",
    "red": "Offline",
    "gray": "Inicial",
}

STATUS_RANK = {
    "red": 0,
    "gray": 1,
    "green": 2,
}

QUALITY_LABELS = {
    "green": "Saudavel",
    "yellow": "Atencao",
    "critical": "Critico",
}

QUALITY_RANK = {
    "critical": 0,
    "yellow": 1,
    "green": 2,
}

# Tolerancia percentual para considerar intervalos cloudv2 equivalentes.
CLOUDV2_MEDIAN_TOLERANCE_PCT = 0.5
PROBE_STATUS_INFO_MIN_FIRMWARE = (2, 8, 0)
CONCENTRATOR_TECH_LABEL = "concentrador"


def _ts_to_str(ts):
    return ts_to_dashboard_str(ts)


def _refresh_timestamp_field(item, ts_key="ts", at_key="at"):
    if not isinstance(item, dict):
        return item
    ts_value = _safe_float(item.get(ts_key), None)
    if ts_value is not None:
        item[at_key] = _ts_to_str(ts_value)
    elif at_key not in item:
        item[at_key] = "-"
    return item


def _refresh_named_timestamp_fields(item, field_pairs):
    if not isinstance(item, dict):
        return item
    for ts_key, at_key in field_pairs:
        ts_value = _safe_float(item.get(ts_key), None)
        if ts_value is not None:
            item[at_key] = _ts_to_str(ts_value)
        elif at_key not in item:
            item[at_key] = "-"
    return item


def _clone_timestamped_items(items, field_pairs=None):
    if not isinstance(items, list):
        return []
    normalized = []
    pairs = tuple(field_pairs or (("ts", "at"),))
    for item in items:
        if not isinstance(item, dict):
            continue
        cloned = dict(item)
        _refresh_named_timestamp_fields(cloned, pairs)
        normalized.append(cloned)
    return normalized


def _format_ago(seconds):
    if seconds is None:
        return "-"
    if seconds < 0:
        return "0s atras"
    if seconds < 60:
        return f"{int(seconds)}s atras"
    if seconds < 3600:
        return f"{int(seconds / 60)}m atras"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h atras"
    return f"{seconds / 86400:.1f}d atras"


def _safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_text(value):
    return str(value or "").strip()


def _parse_signal_technology_combined(value):
    raw = _normalize_text(value)
    if not raw:
        return ("", "")
    if "/" not in raw:
        return (raw, "")
    left, right = raw.split("/", 1)
    signal = _normalize_text(left)
    technology = _normalize_text(right)
    if signal in ("-", "--"):
        signal = ""
    if technology in ("-", "--"):
        technology = ""
    return (signal, technology)


def _parse_duration_seconds(value):
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None

    if text.isdigit():
        return int(text)

    clock_match = re.match(r"^(\d+):(\d+)$", text)
    if clock_match:
        minutes = int(clock_match.group(1))
        seconds = int(clock_match.group(2))
        return (minutes * 60) + seconds

    unit_match = re.match(r"^(\d+(?:\.\d+)?)\s*(s|sec|secs|m|min|mins|h|hr|hrs)$", text)
    if unit_match:
        amount = float(unit_match.group(1))
        unit = unit_match.group(2)
        if unit.startswith("h"):
            return int(amount * 3600)
        if unit.startswith("m"):
            return int(amount * 60)
        return int(amount)

    return None


def validate_pivot_id(pivot_id):
    value = str(pivot_id or "")
    first_underscore_index = value.find("_")
    if first_underscore_index == -1:
        return False

    if "__" in value:
        return False

    first_part = value[:first_underscore_index]
    for char in first_part:
        if not char.isalpha():
            return False

    after_first_underscore = value[first_underscore_index + 1 :]

    second_underscore_index = after_first_underscore.find("_")
    if second_underscore_index == -1:
        number_part = after_first_underscore
    else:
        number_part = after_first_underscore[second_underscore_index + 1 :]

    if len(number_part) == 0:
        return False

    for char in number_part:
        if not char.isdigit():
            return False

    return True


def parse_device_payload(payload):
    text = str(payload or "").strip()
    if not text:
        return None, "payload vazio"
    if not text.startswith("#"):
        return None, "payload sem prefixo #"
    if not text.endswith("$"):
        return None, "payload sem sufixo $"

    core = text[1:-1]
    if not core:
        return None, "payload sem conteudo interno"

    parts = [part.strip() for part in core.split("-")]
    if len(parts) < 2:
        return None, "payload sem campos suficientes"

    idp = parts[0]
    pivot_id = parts[1]
    if not idp:
        return None, "campo IDP vazio"
    if not pivot_id:
        return None, "campo pivot_id vazio"
    if not validate_pivot_id(pivot_id):
        return None, "pivot_id invalido"

    parsed = {
        "raw": text,
        "idp": idp,
        "pivot_id": pivot_id,
        "parts": parts,
    }
    return parsed, None


def parse_ping_rssi(parsed):
    if not isinstance(parsed, dict):
        return None

    if str(parsed.get("idp") or "").strip() != "8":
        return None

    parts = parsed.get("parts")
    if not isinstance(parts, list) or len(parts) < 3:
        return None

    raw_rssi = str(parts[2] or "").strip()
    if not raw_rssi or not re.fullmatch(r"\d+", raw_rssi):
        return None

    rssi_value = _safe_int(raw_rssi, None)
    if rssi_value is None or rssi_value < 0 or rssi_value > 31:
        return None
    return rssi_value


def parse_pivot_config_payload(parsed):
    if not isinstance(parsed, dict):
        return None

    raw_idp = str(parsed.get("idp") or "").strip()
    if raw_idp not in ("3", "03"):
        return None

    parts = parsed.get("parts")
    if not isinstance(parts, list) or len(parts) < 2:
        return None

    return {
        "contactor": _normalize_text(parts[2]) if len(parts) > 2 else None,
        "pressure": _normalize_text(parts[3]) if len(parts) > 3 else None,
        "pressurization_time": _safe_int(parts[4], None) if len(parts) > 4 else None,
        "on_time": _safe_int(parts[5], None) if len(parts) > 5 else None,
        "off_time": _safe_int(parts[6], None) if len(parts) > 6 else None,
        "read_time": _safe_int(parts[7], None) if len(parts) > 7 else None,
    }


def parse_network_config_payload(parsed):
    if not isinstance(parsed, dict):
        return None

    raw_idp = str(parsed.get("idp") or "").strip()
    if raw_idp not in ("2", "02"):
        return None

    parts = parsed.get("parts")
    if not isinstance(parts, list) or len(parts) < 2:
        return None

    return {
        "gprs_id": _normalize_text(parts[2]) if len(parts) > 2 else None,
        "modem_apn": _normalize_text(parts[3]) if len(parts) > 3 else None,
        "wifi_ssid": _normalize_text(parts[4]) if len(parts) > 4 else None,
        "wifi_pass": _normalize_text(parts[5]) if len(parts) > 5 else None,
    }


def _parse_config_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = _normalize_text(value).lower()
    if text in ("1", "true", "sim", "yes", "on"):
        return True
    if text in ("0", "false", "nao", "não", "no", "off"):
        return False
    return None


def parse_rush_config_payload(parsed):
    if not isinstance(parsed, dict):
        return None

    raw_idp = str(parsed.get("idp") or "").strip()
    if raw_idp not in ("4", "04"):
        return None

    parts = parsed.get("parts")
    if not isinstance(parts, list) or len(parts) < 2:
        return None

    return {
        "start_time_hhmm": _normalize_text(parts[2]) if len(parts) > 2 else None,
        "end_time_hhmm": _normalize_text(parts[3]) if len(parts) > 3 else None,
        "enabled": _parse_config_bool(parts[4]) if len(parts) > 4 else None,
    }


def parse_sector_config_payload(parsed):
    if not isinstance(parsed, dict):
        return None

    raw_idp = str(parsed.get("idp") or "").strip()
    if raw_idp not in ("5", "05"):
        return None

    parts = parsed.get("parts")
    if not isinstance(parts, list) or len(parts) < 2:
        return None

    sectors = []
    for index in range(4):
        start_index = 3 + (index * 2)
        end_index = start_index + 1
        sectors.append(
            {
                "start_angle": _safe_int(parts[start_index], None) if len(parts) > start_index else None,
                "end_angle": _safe_int(parts[end_index], None) if len(parts) > end_index else None,
            }
        )

    return {
        "sector_number": _safe_int(parts[2], None) if len(parts) > 2 else None,
        "sectors": sectors,
    }


def parse_physical_barrier_config_payload(parsed):
    if not isinstance(parsed, dict):
        return None

    raw_idp = str(parsed.get("idp") or "").strip()
    if raw_idp not in ("22",):
        return None

    parts = parsed.get("parts")
    if not isinstance(parts, list) or len(parts) < 2:
        return None

    return {
        "start_angle": _safe_int(parts[2], None) if len(parts) > 2 else None,
        "end_angle": _safe_int(parts[3], None) if len(parts) > 3 else None,
        "automatic_return": _parse_config_bool(parts[4]) if len(parts) > 4 else None,
        "water_return": _parse_config_bool(parts[5]) if len(parts) > 5 else None,
        "time_leaving_barrier": _safe_int(parts[6], None) if len(parts) > 6 else None,
    }


PIVOT_CONFIG_VALUE_KEYS = (
    "contactor",
    "pressure",
    "pressurization_time",
    "on_time",
    "off_time",
    "read_time",
)
NETWORK_CONFIG_VALUE_KEYS = ("gprs_id", "modem_apn", "wifi_ssid", "wifi_pass")
RUSH_CONFIG_VALUE_KEYS = ("start_time_hhmm", "end_time_hhmm", "enabled")
SECTOR_CONFIG_VALUE_KEYS = ("sector_number", "sectors")
PHYSICAL_BARRIER_CONFIG_VALUE_KEYS = (
    "start_angle",
    "end_angle",
    "automatic_return",
    "water_return",
    "time_leaving_barrier",
)


def _new_pivot_config_state():
    state = {
        "last_request_ts": None,
        "last_request_topic": None,
        "last_request_payload": None,
        "last_response_ts": None,
        "last_response_topic": None,
        "last_response_payload": None,
    }
    for key in PIVOT_CONFIG_VALUE_KEYS:
        state[key] = None
    return state


def _new_network_config_state():
    state = {
        "last_request_ts": None,
        "last_request_topic": None,
        "last_request_payload": None,
        "last_response_ts": None,
        "last_response_topic": None,
        "last_response_payload": None,
    }
    for key in NETWORK_CONFIG_VALUE_KEYS:
        state[key] = None
    return state


def _new_rush_config_state():
    state = {
        "last_request_ts": None,
        "last_request_topic": None,
        "last_request_payload": None,
        "last_response_ts": None,
        "last_response_topic": None,
        "last_response_payload": None,
    }
    for key in RUSH_CONFIG_VALUE_KEYS:
        state[key] = None
    return state


def _new_sector_config_state():
    state = {
        "last_request_ts": None,
        "last_request_topic": None,
        "last_request_payload": None,
        "last_response_ts": None,
        "last_response_topic": None,
        "last_response_payload": None,
        "sector_number": None,
        "sectors": [{"start_angle": None, "end_angle": None} for _ in range(4)],
    }
    return state


def _new_physical_barrier_config_state():
    state = {
        "last_request_ts": None,
        "last_request_topic": None,
        "last_request_payload": None,
        "last_response_ts": None,
        "last_response_topic": None,
        "last_response_payload": None,
    }
    for key in PHYSICAL_BARRIER_CONFIG_VALUE_KEYS:
        state[key] = None
    return state


def _normalize_pivot_config_state(value):
    normalized = _new_pivot_config_state()
    if not isinstance(value, dict):
        return normalized

    normalized["last_request_ts"] = _safe_float(value.get("last_request_ts"), None)
    normalized["last_response_ts"] = _safe_float(value.get("last_response_ts"), None)
    normalized["last_request_topic"] = _normalize_text(value.get("last_request_topic")) or None
    normalized["last_request_payload"] = _normalize_text(value.get("last_request_payload")) or None
    normalized["last_response_topic"] = _normalize_text(value.get("last_response_topic")) or None
    normalized["last_response_payload"] = _normalize_text(value.get("last_response_payload")) or None
    normalized["contactor"] = _normalize_text(value.get("contactor")) or None
    normalized["pressure"] = _normalize_text(value.get("pressure")) or None
    for key in ("pressurization_time", "on_time", "off_time", "read_time"):
        normalized[key] = _safe_int(value.get(key), None)
    return normalized


def _normalize_network_config_state(value):
    normalized = _new_network_config_state()
    if not isinstance(value, dict):
        return normalized

    normalized["last_request_ts"] = _safe_float(value.get("last_request_ts"), None)
    normalized["last_response_ts"] = _safe_float(value.get("last_response_ts"), None)
    normalized["last_request_topic"] = _normalize_text(value.get("last_request_topic")) or None
    normalized["last_request_payload"] = _normalize_text(value.get("last_request_payload")) or None
    normalized["last_response_topic"] = _normalize_text(value.get("last_response_topic")) or None
    normalized["last_response_payload"] = _normalize_text(value.get("last_response_payload")) or None
    for key in NETWORK_CONFIG_VALUE_KEYS:
        normalized[key] = _normalize_text(value.get(key)) or None
    return normalized


def _normalize_rush_config_state(value):
    normalized = _new_rush_config_state()
    if not isinstance(value, dict):
        return normalized

    normalized["last_request_ts"] = _safe_float(value.get("last_request_ts"), None)
    normalized["last_response_ts"] = _safe_float(value.get("last_response_ts"), None)
    normalized["last_request_topic"] = _normalize_text(value.get("last_request_topic")) or None
    normalized["last_request_payload"] = _normalize_text(value.get("last_request_payload")) or None
    normalized["last_response_topic"] = _normalize_text(value.get("last_response_topic")) or None
    normalized["last_response_payload"] = _normalize_text(value.get("last_response_payload")) or None
    normalized["start_time_hhmm"] = _normalize_text(value.get("start_time_hhmm")) or None
    normalized["end_time_hhmm"] = _normalize_text(value.get("end_time_hhmm")) or None
    normalized["enabled"] = _parse_config_bool(value.get("enabled"))
    return normalized


def _normalize_sector_config_state(value):
    normalized = _new_sector_config_state()
    if not isinstance(value, dict):
        return normalized

    normalized["last_request_ts"] = _safe_float(value.get("last_request_ts"), None)
    normalized["last_response_ts"] = _safe_float(value.get("last_response_ts"), None)
    normalized["last_request_topic"] = _normalize_text(value.get("last_request_topic")) or None
    normalized["last_request_payload"] = _normalize_text(value.get("last_request_payload")) or None
    normalized["last_response_topic"] = _normalize_text(value.get("last_response_topic")) or None
    normalized["last_response_payload"] = _normalize_text(value.get("last_response_payload")) or None
    parsed_count = _safe_int(value.get("sector_number"), None)
    normalized["sector_number"] = parsed_count if parsed_count is not None and 1 <= parsed_count <= 4 else None
    raw_sectors = value.get("sectors") if isinstance(value.get("sectors"), list) else []
    sectors = []
    for index in range(4):
        raw_sector = raw_sectors[index] if index < len(raw_sectors) and isinstance(raw_sectors[index], dict) else {}
        sectors.append(
            {
                "start_angle": _safe_int(raw_sector.get("start_angle"), None),
                "end_angle": _safe_int(raw_sector.get("end_angle"), None),
            }
        )
    normalized["sectors"] = sectors
    return normalized


def _normalize_physical_barrier_config_state(value):
    normalized = _new_physical_barrier_config_state()
    if not isinstance(value, dict):
        return normalized

    normalized["last_request_ts"] = _safe_float(value.get("last_request_ts"), None)
    normalized["last_response_ts"] = _safe_float(value.get("last_response_ts"), None)
    normalized["last_request_topic"] = _normalize_text(value.get("last_request_topic")) or None
    normalized["last_request_payload"] = _normalize_text(value.get("last_request_payload")) or None
    normalized["last_response_topic"] = _normalize_text(value.get("last_response_topic")) or None
    normalized["last_response_payload"] = _normalize_text(value.get("last_response_payload")) or None
    normalized["start_angle"] = _safe_int(value.get("start_angle"), None)
    normalized["end_angle"] = _safe_int(value.get("end_angle"), None)
    normalized["automatic_return"] = _parse_config_bool(value.get("automatic_return"))
    normalized["water_return"] = _parse_config_bool(value.get("water_return"))
    normalized["time_leaving_barrier"] = _safe_int(value.get("time_leaving_barrier"), None)
    return normalized


SUPPORTED_CONFIG_IDPS = ("02", "03", "04", "05", "22")


def _config_state_spec(idp):
    normalized = str(idp or "").strip().zfill(2)
    if normalized == "02":
        return ("network_config", parse_network_config_payload, _normalize_network_config_state)
    if normalized == "03":
        return ("pivot_config", parse_pivot_config_payload, _normalize_pivot_config_state)
    if normalized == "04":
        return ("rush_config", parse_rush_config_payload, _normalize_rush_config_state)
    if normalized == "05":
        return ("sector_config", parse_sector_config_payload, _normalize_sector_config_state)
    if normalized == "22":
        return ("physical_barrier_config", parse_physical_barrier_config_payload, _normalize_physical_barrier_config_state)
    return (None, None, None)


def _validate_config_payload_field(value, field_name):
    text = _normalize_text(value)
    if not text or text == "-":
        raise ValueError(f"{field_name} obrigatorio")
    if any(char in text for char in ("#", "$", "\r", "\n")):
        raise ValueError(f"{field_name} contem caractere invalido")
    return text


def _validate_config_payload_int(value, field_name, min_value=0, max_value=255):
    text = _validate_config_payload_field(value, field_name)
    parsed = _safe_int(text, None)
    if parsed is None or str(parsed) != text:
        raise ValueError(f"{field_name} deve ser numerico")
    if parsed < min_value or parsed > max_value:
        raise ValueError(f"{field_name} fora do limite")
    return parsed


def _validate_config_payload_hhmm(value, field_name):
    text = _validate_config_payload_field(value, field_name)
    if not re.fullmatch(r"\d{1,4}", text):
        raise ValueError(f"{field_name} deve ser HHMM")
    normalized = text.zfill(4)
    hour = _safe_int(normalized[:2], None)
    minute = _safe_int(normalized[2:], None)
    if hour is None or minute is None or hour > 23 or minute > 59:
        raise ValueError(f"{field_name} invalido")
    return normalized


def _validate_config_payload_bool(value, field_name):
    parsed = _parse_config_bool(value)
    if parsed is None:
        raise ValueError(f"{field_name} deve ser 0 ou 1")
    return "1" if parsed else "0"


def _validate_config_payload_sector_count(value):
    parsed = _validate_config_payload_int(value, "sector_number", 1, 4)
    return parsed


def _read_sector_angle_value(safe_values, index, key):
    sectors = safe_values.get("sectors")
    if isinstance(sectors, list) and index < len(sectors) and isinstance(sectors[index], dict):
        return sectors[index].get(key)
    return safe_values.get(f"sector_{index + 1}_{key}")


def _parse_version_triplet(value):
    raw = _normalize_text(value).lower()
    if not raw:
        return None
    match = re.search(r"v?\s*(\d+)\.(\d+)\.(\d+)", raw)
    if not match:
        return None
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def _compare_version_triplet(left, right):
    safe_left = tuple(left or ())
    safe_right = tuple(right or ())
    for index in range(3):
        left_value = int(safe_left[index] if index < len(safe_left) else 0)
        right_value = int(safe_right[index] if index < len(safe_right) else 0)
        if left_value > right_value:
            return 1
        if left_value < right_value:
            return -1
    return 0


def _normalize_probe_status_text(value):
    return re.sub(r"\s+", " ", _normalize_text(value))


def _normalize_probe_response_info(value):
    if not isinstance(value, dict):
        return None

    timestamp_ts = _safe_int(value.get("board_timestamp_ts"), None)
    normalized = {
        "rssi": _safe_int(value.get("rssi"), None),
        "technology": _normalize_probe_status_text(value.get("technology")),
        "board_timestamp_ts": timestamp_ts,
        "board_timestamp_at": _ts_to_str(timestamp_ts),
        "operator": _normalize_probe_status_text(value.get("operator")),
        "modem_name": _normalize_probe_status_text(value.get("modem_name")),
        "imei": _normalize_probe_status_text(value.get("imei")),
        "keep_alive_sec": _safe_int(value.get("keep_alive_sec"), None),
        "socket_timeout_sec": _safe_int(value.get("socket_timeout_sec"), None),
        "apn_time_ms": _safe_int(value.get("apn_time_ms"), None),
        "apn": _normalize_probe_status_text(value.get("apn")),
        "networks": _safe_int(value.get("networks"), None),
        "esp_temp_c": _safe_float(value.get("esp_temp_c"), None),
        "firmware": _normalize_probe_status_text(value.get("firmware")),
    }

    if not any(item not in (None, "", "-") for item in normalized.values()):
        return None
    return normalized


def parse_probe_status_payload(parsed):
    if not isinstance(parsed, dict):
        return None
    if _normalize_text(parsed.get("idp")) != "11":
        return None

    parts = parsed.get("parts")
    if not isinstance(parts, list) or len(parts) < 15:
        return None

    payload_parts = [_normalize_text(part) for part in parts[2:]]
    if len(payload_parts) < 13:
        return None

    head = payload_parts[:3]
    tail = payload_parts[-8:]
    middle = payload_parts[3:-8]
    if len(middle) < 2:
        return None

    firmware = _normalize_probe_status_text(tail[7])
    firmware_parts = _parse_version_triplet(firmware)
    if not firmware_parts or _compare_version_triplet(firmware_parts, PROBE_STATUS_INFO_MIN_FIRMWARE) < 0:
        return None

    info = {
        "rssi": _safe_int(head[0], None),
        "technology": head[1],
        "board_timestamp_ts": _safe_int(head[2], None),
        "operator": middle[0],
        "modem_name": "-".join(part for part in middle[1:] if part),
        "imei": tail[0],
        "keep_alive_sec": _safe_int(tail[1], None),
        "socket_timeout_sec": _safe_int(tail[2], None),
        "apn_time_ms": _safe_int(tail[3], None),
        "apn": tail[4],
        "networks": _safe_int(tail[5], None),
        "esp_temp_c": _safe_float(tail[6], None),
        "firmware": firmware,
    }
    return _normalize_probe_response_info(info)


class TelemetryStore:
    def __init__(self, config, log_dir):
        self.log = logging.getLogger("cloudv2.telemetry")
        self.log_dir = log_dir

        self.monitor_topics = tuple(config.get("monitor_topics") or MONITOR_TOPICS)
        self.refresh_sec = max(1, int(config.get("dashboard_refresh_sec", 5)))
        self.enable_background_worker = bool(config.get("enable_background_worker", True))
        self.require_apply_to_start = bool(config.get("require_apply_to_start", True))
        self.history_mode = str(config.get("history_mode", "merge")).strip().lower()
        self.continuous_monitoring_mode = bool(config.get("continuous_monitoring_mode", True))
        if self.continuous_monitoring_mode and self.history_mode != "merge":
            self.log.warning(
                "Modo continuo ativo: history_mode=%s ignorado e ajustado para merge.",
                self.history_mode,
            )
            self.history_mode = "merge"

        self.ping_expected_sec = max(1, int(config.get("ping_interval_minutes", 3)) * 60)
        self.tolerance_factor = max(1.0, float(config.get("tolerance_factor", 3.0)))
        self.attention_disconnected_pct_threshold = min(
            100.0,
            max(0.0, float(config.get("attention_disconnected_pct_threshold", 20.0))),
        )
        self.critical_disconnected_pct_threshold = min(
            100.0,
            max(
                self.attention_disconnected_pct_threshold,
                float(config.get("critical_disconnected_pct_threshold", 50.0)),
            ),
        )
        self.attention_disconnected_window_hours = max(
            1,
            int(config.get("attention_disconnected_window_hours", 24)),
        )
        self.attention_disconnected_window_sec = self.attention_disconnected_window_hours * 3600
        self.cloudv2_window = max(3, int(config.get("cloudv2_median_window", 20)))
        self.cloudv2_min_samples = max(2, int(config.get("cloudv2_min_samples", 5)))
        if self.cloudv2_min_samples > self.cloudv2_window:
            self.cloudv2_min_samples = self.cloudv2_window
        self.cloudv2_median_tolerance_pct = CLOUDV2_MEDIAN_TOLERANCE_PCT

        self.dedupe_window_sec = max(1, int(config.get("dedupe_window_sec", 8)))
        self.history_retention_hours = max(24, int(config.get("history_retention_hours", 24)))
        self.retention_sec = self.history_retention_hours * 3600
        self.max_events_per_pivot_panel = max(
            100,
            int(config.get("max_events_per_pivot_panel", config.get("max_events_per_pivot", 5000))),
        )
        self.max_events_per_pivot_list = max(
            100,
            int(config.get("max_events_per_pivot_list", self.max_events_per_pivot_panel)),
        )
        if self.max_events_per_pivot_list > self.max_events_per_pivot_panel:
            self.max_events_per_pivot_list = self.max_events_per_pivot_panel
        # Compatibilidade com logica existente (painel detalhado).
        self.max_events_per_pivot = self.max_events_per_pivot_panel
        self.show_pending_ping_pivots = bool(config.get("show_pending_ping_pivots", False))

        self.probe_default_interval_sec = max(10, int(config.get("probe_default_interval_sec", 300)))
        self.probe_min_interval_sec = max(10, int(config.get("probe_min_interval_sec", 60)))
        if self.probe_default_interval_sec < self.probe_min_interval_sec:
            self.probe_default_interval_sec = self.probe_min_interval_sec
        self.probe_timeout_factor = max(1.0, float(config.get("probe_timeout_factor", 1.25)))
        self.probe_timeout_streak_alert = max(1, int(config.get("probe_timeout_streak_alert", 2)))
        state_cache_ttl = _safe_float(config.get("api_state_cache_ttl_sec"), 2.0)
        quality_cache_ttl = _safe_float(config.get("api_quality_cache_ttl_sec"), 2.0)
        self.api_state_cache_ttl_sec = min(5.0, max(0.0, state_cache_ttl if state_cache_ttl is not None else 2.0))
        self.api_quality_cache_ttl_sec = min(
            5.0,
            max(0.0, quality_cache_ttl if quality_cache_ttl is not None else 2.0),
        )

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._worker = threading.Thread(target=self._background_loop, name="cloudv2-telemetry", daemon=True)
        self._started = False
        self._dirty = True
        self._last_write_ts = 0.0
        self._event_seq = 0
        self._probe_sender = None
        self._modem_reset_sender = None
        self._pivot_config_sender = None
        self._api_cache_generation = 0
        self._state_snapshot_cache = {}
        self._quality_cards_cache = {}
        self._cloud2_filter_options_cache = {}

        self.pivots = {}
        self.pending_ping_unknown = {}
        self.pending_expected_pivots = {}
        self.malformed_messages = []
        self.duplicate_count = 0

        self._dedupe_cache = {}
        self._probe_settings = self._normalize_probe_settings(config.get("probe_settings", {}))
        self._active_session_by_pivot = {}
        self._active_run_id = None
        self._monitoring_mode = "idle" if self.require_apply_to_start else "live"

        self.db_settings = resolve_database_settings(
            {
                "db_backend": config.get("db_backend", "sqlite"),
                "sqlite_db_path": config.get("sqlite_db_path", os.path.join(DATA_DIR, "telemetry.sqlite3")),
                "database_url": config.get("database_url", ""),
            }
        )
        self.sqlite_db_path = self.db_settings.sqlite_db_path
        self.db_backend = self.db_settings.backend
        self.database_url = self.db_settings.database_url
        self.persistence = TelemetryPersistence(
            db_path=self.sqlite_db_path,
            db_backend=self.db_backend,
            database_url=self.database_url,
            max_events_per_pivot=self.max_events_per_pivot_panel,
            log=self.log,
        )

        self.runtime_path = os.path.join(DATA_DIR, "runtime_store.json")

    def _restore_pending_expected_pivots_locked(self, pending_expected):
        if not isinstance(pending_expected, dict):
            return

        cleaned_expected = {}
        for pivot_id, raw_entry in pending_expected.items():
            normalized = str(pivot_id or "").strip()
            if not normalized:
                continue
            entry = raw_entry if isinstance(raw_entry, dict) else {}
            cleaned_expected[normalized] = {
                "pivot_id": normalized,
                "added_at_ts": _safe_float(entry.get("added_at_ts"), None),
                "source": str(entry.get("source") or "ui").strip() or "ui",
            }
        self.pending_expected_pivots = cleaned_expected

    def _merge_pending_expected_pivots_locked(self, pending_expected):
        if not isinstance(pending_expected, dict):
            return False

        changed = False
        for pivot_id, raw_entry in pending_expected.items():
            normalized = str(pivot_id or "").strip()
            if not normalized:
                continue
            entry = raw_entry if isinstance(raw_entry, dict) else {}
            incoming_added_at_ts = _safe_float(entry.get("added_at_ts"), None)
            incoming_source = str(entry.get("source") or "ui").strip() or "ui"
            current = self.pending_expected_pivots.get(normalized)
            if isinstance(current, dict):
                current_added_at_ts = _safe_float(current.get("added_at_ts"), None)
                merged_added_at_ts = current_added_at_ts
                if merged_added_at_ts is None:
                    merged_added_at_ts = incoming_added_at_ts
                elif incoming_added_at_ts is not None:
                    merged_added_at_ts = min(merged_added_at_ts, incoming_added_at_ts)
                merged_source = str(current.get("source") or incoming_source).strip() or incoming_source
                next_entry = {
                    "pivot_id": normalized,
                    "added_at_ts": merged_added_at_ts,
                    "source": merged_source,
                }
                if current != next_entry:
                    self.pending_expected_pivots[normalized] = next_entry
                    changed = True
                continue

            self.pending_expected_pivots[normalized] = {
                "pivot_id": normalized,
                "added_at_ts": incoming_added_at_ts,
                "source": incoming_source,
            }
            changed = True
        return changed

    def _persist_pending_expected_pivots_locked(self):
        items = [
            {
                "pivot_id": pivot_id,
                "added_at_ts": _safe_float(entry.get("added_at_ts"), None),
                "source": str(entry.get("source") or "ui").strip() or "ui",
            }
            for pivot_id, entry in sorted(self.pending_expected_pivots.items(), key=lambda item: item[0].lower())
        ]
        try:
            self.persistence.replace_expected_pivots_pending(items)
        except RuntimeError:
            return

    def _load_pending_expected_pivots_from_persistence(self):
        try:
            loaded_items = self.persistence.load_expected_pivots_pending()
        except RuntimeError:
            return

        loaded_map = {}
        for item in loaded_items:
            if not isinstance(item, dict):
                continue
            pivot_id = str(item.get("pivot_id") or "").strip()
            if not pivot_id:
                continue
            loaded_map[pivot_id] = item

        with self._lock:
            self._restore_pending_expected_pivots_locked(loaded_map)
            if self._cleanup_expected_pivots_locked():
                self._persist_pending_expected_pivots_locked()

    def _load_pending_expected_pivots_from_runtime(self):
        if not os.path.exists(self.runtime_path):
            return

        try:
            with open(self.runtime_path, "r", encoding="utf-8") as file:
                loaded = json.load(file)
        except (OSError, json.JSONDecodeError):
            return

        if not isinstance(loaded, dict):
            return

        with self._lock:
            merged = self._merge_pending_expected_pivots_locked(loaded.get("pending_expected_pivots"))
            cleaned = self._cleanup_expected_pivots_locked()
            if merged or cleaned:
                self._persist_pending_expected_pivots_locked()

    def _ensure_manual_session_rotation_allowed(self, action_label):
        if self.continuous_monitoring_mode:
            raise ValueError(
                f"modo continuo ativo: acao '{action_label}' desabilitada para evitar reset de sessao/run"
            )

    def start(self):
        ensure_dirs()
        os.makedirs(self.log_dir, exist_ok=True)
        self.persistence.start()
        self._load_pending_expected_pivots_from_persistence()
        self._load_pending_expected_pivots_from_runtime()

        db_probe_settings = self.persistence.load_probe_settings()
        if isinstance(db_probe_settings, dict) and db_probe_settings:
            self._probe_settings.update(db_probe_settings)

        if self.history_mode == "fresh":
            self.persistence.deactivate_all_active_sessions()
            self.persistence.deactivate_all_active_runs()

        for pivot_id, setting in self._probe_settings.items():
            self.persistence.upsert_probe_setting(
                pivot_id,
                bool(setting.get("enabled")),
                int(setting.get("interval_sec", self.probe_default_interval_sec)),
            )

        if self.require_apply_to_start:
            with self._lock:
                self._active_session_by_pivot = {}
                self._active_run_id = None
                self._monitoring_mode = "idle"
                self.pivots = {}
                self.pending_ping_unknown = {}
                self.malformed_messages = []
                self.duplicate_count = 0
                self._dirty = True
                self._invalidate_api_caches_locked()
            self._clear_dashboard_data_files()
            self.write()
        else:
            if self.history_mode == "merge":
                active_run = self.persistence.get_or_create_active_run(source="runtime_start")
                active_run_id = str((active_run or {}).get("run_id") or "").strip()
                with self._lock:
                    self._active_run_id = active_run_id or None
                    active_sessions = self.persistence.get_active_sessions_map(run_id=active_run_id)
                    if (not active_sessions) and active_run_id:
                        # Se nenhum registro estiver ativo no run, restaura a ultima
                        # sessao por pivo antes de processar novas mensagens.
                        active_sessions = self.persistence.activate_latest_sessions_for_run(
                            active_run_id,
                            now_ts=time.time(),
                        )
                    self._active_session_by_pivot = active_sessions
            else:
                with self._lock:
                    self._active_session_by_pivot = {}
                    self._active_run_id = None

            if self.history_mode == "merge":
                self._load_runtime_state()
                db_probe_settings = self.persistence.load_probe_settings()
                if isinstance(db_probe_settings, dict) and db_probe_settings:
                    with self._lock:
                        self._probe_settings.update(db_probe_settings)
            else:
                self._clear_dashboard_data_files()

            now = time.time()
            with self._lock:
                self._sync_probe_settings_to_runtime_pivots_locked()
                self._monitoring_mode = "live"
                self._ensure_active_run_locked(now, source="runtime_start")
                for pivot_id, pivot in self.pivots.items():
                    discovered_ts = _safe_float(pivot.get("discovered_at_ts"), now)
                    session_id = self._ensure_active_session_locked(pivot_id, discovered_ts, source="runtime")
                    pivot["session_id"] = session_id
                    pivot["run_id"] = self._active_run_id
                    self._backfill_pivot_session_locked(pivot)
                    baseline_summary = self._load_pivot_baseline_from_persistence_locked(pivot_id)
                    if isinstance(baseline_summary, dict):
                        self._apply_baseline_snapshot_locked(pivot, baseline_summary, now=now)
                    # Em restart, evita sobrescrever snapshot valido ja persistido
                    # com estado parcial carregado do runtime_store.
                    has_persisted_snapshot = False
                    if self.history_mode == "merge":
                        try:
                            has_persisted_snapshot = self.persistence.has_snapshot(pivot_id, session_id)
                        except RuntimeError:
                            has_persisted_snapshot = False
                    self._refresh_status_locked(pivot, now)
                    if not has_persisted_snapshot:
                        self._persist_pivot_snapshot_locked(pivot, now)

                self._dirty = True
                self._invalidate_api_caches_locked()
            self.write()

        if self.enable_background_worker and (not self._started):
            self._started = True
            self._worker.start()

    def stop(self):
        self._stop_event.set()
        if self._started:
            self._worker.join(timeout=2.5)
        self.write()
        self.persistence.stop()

    def set_probe_sender(self, sender_fn):
        self._probe_sender = sender_fn

    def set_modem_reset_sender(self, sender_fn):
        self._modem_reset_sender = sender_fn

    def set_pivot_config_sender(self, sender_fn):
        self._pivot_config_sender = sender_fn

    def _api_cache_key(self, run_id):
        normalized_run = str(run_id or "").strip()
        return normalized_run or "__default__"

    def _invalidate_api_caches_locked(self):
        self._api_cache_generation += 1
        self._state_snapshot_cache.clear()
        self._quality_cards_cache.clear()
        self._cloud2_filter_options_cache.clear()

    def _get_cached_api_payload_locked(self, cache, cache_key, now_ts):
        entry = cache.get(cache_key)
        if not isinstance(entry, dict):
            return None

        generation = _safe_int(entry.get("generation"), None)
        if generation is None or generation != self._api_cache_generation:
            cache.pop(cache_key, None)
            return None

        expires_at_ts = _safe_float(entry.get("expires_at_ts"), None)
        if expires_at_ts is None or expires_at_ts <= now_ts:
            cache.pop(cache_key, None)
            return None

        payload = entry.get("payload")
        if not isinstance(payload, dict):
            cache.pop(cache_key, None)
            return None
        return copy.deepcopy(payload)

    def _set_cached_api_payload_locked(
        self,
        cache,
        cache_key,
        payload,
        ttl_sec,
        now_ts,
        expected_generation=None,
    ):
        ttl_value = _safe_float(ttl_sec, 0.0)
        if ttl_value is None or ttl_value <= 0:
            return
        if expected_generation is not None and int(expected_generation) != int(self._api_cache_generation):
            return
        if not isinstance(payload, dict):
            return

        cache[cache_key] = {
            "generation": int(self._api_cache_generation),
            "expires_at_ts": float(now_ts) + float(ttl_value),
            "payload": copy.deepcopy(payload),
        }

    def process_message(self, topic, payload, ts=None):
        ts = float(ts if ts is not None else time.time())
        topic = str(topic or "").strip()
        payload_text = str(payload or "").strip()

        if not topic:
            return {"accepted": False, "reason": "topic vazio"}

        write_expected_queue_after = False
        with self._lock:
            if self._monitoring_mode != "live":
                return {
                    "accepted": False,
                    "reason": "monitoramento aguardando aplicacao",
                    "mode": self._monitoring_mode,
                }

            modem_reset_ack = self._record_modem_reset_ack_if_applicable_locked(topic, payload_text, ts)
            if modem_reset_ack is not None:
                return modem_reset_ack

            if topic not in self.monitor_topics:
                self.log.warning("Mensagem em topico nao monitorado descartada: topic=%s", topic)
                return {"accepted": False, "reason": "topic nao monitorado"}

            if self._is_duplicate_locked(topic, payload_text, ts):
                self.duplicate_count += 1
                return {"accepted": False, "reason": "duplicada"}

            parsed, parse_error = parse_device_payload(payload_text)
            if parse_error:
                self._record_malformed_locked(topic, payload_text, parse_error, ts)
                self._invalidate_api_caches_locked()
                return {"accepted": False, "reason": parse_error}

            pivot_id = parsed["pivot_id"]

            if topic == TOPIC_CLOUDV2:
                pivot = self.pivots.get(pivot_id)
                known_pivot = pivot is not None or self._pivot_exists_locked(pivot_id)
                pending_expected = self.pending_expected_pivots.get(pivot_id)
                if (not known_pivot) and pending_expected is None:
                    self.log.info(
                        "Mensagem cloudv2 descartada para pivot nao autorizado: pivot_id=%s",
                        pivot_id,
                    )
                    return {
                        "accepted": False,
                        "reason": "pivot nao autorizado",
                        "pivot_id": pivot_id,
                    }

                pivot = self._get_or_create_pivot_locked(pivot_id, ts)
                if pending_expected is not None:
                    self.pending_expected_pivots.pop(pivot_id, None)
                    self._persist_pending_expected_pivots_locked()
                    write_expected_queue_after = True
                self._record_message_common_locked(pivot, topic, ts)
                self._record_cloudv2_locked(pivot, parsed, topic, ts, raw_payload=payload_text)
                self._refresh_status_locked(pivot, ts)
                self._prune_pivot_locked(pivot, ts)
                self._persist_pivot_snapshot_locked(pivot, ts)
                self._dirty = True
                self._invalidate_api_caches_locked()
                result = {
                    "accepted": True,
                    "pivot_id": pivot_id,
                    "event": "cloudv2",
                    "session_id": pivot.get("session_id"),
                }
                # Persiste imediatamente a fila de descoberta quando um pivot autorizado
                # eh encontrado, para nao reaparecer apos restart.
                if write_expected_queue_after:
                    pass
                else:
                    return result
            else:
                result = None

            if topic != TOPIC_CLOUDV2:
                pivot = self.pivots.get(pivot_id)
                if pivot is None:
                    self.log.info(
                        "Mensagem descartada para pivot nao autorizado: topic=%s pivot_id=%s",
                        topic,
                        pivot_id,
                    )
                    return {
                        "accepted": False,
                        "reason": "pivot nao autorizado",
                        "pivot_id": pivot_id,
                    }

                self._record_message_common_locked(pivot, topic, ts)

                if topic == TOPIC_PING:
                    self._record_ping_locked(pivot, parsed, topic, ts, raw_payload=payload_text)
                elif topic == TOPIC_CLOUD2:
                    self._record_cloud2_locked(pivot, parsed, topic, ts, raw_payload=payload_text)
                elif topic in PROBE_RESPONSE_TOPICS:
                    self._record_probe_response_locked(pivot, parsed, topic, ts, raw_payload=payload_text)
                elif topic == TOPIC_CONFIG:
                    self._record_pivot_config_locked(pivot, parsed, topic, ts, raw_payload=payload_text)
                elif topic in EVENT_ONLY_TOPICS:
                    self._record_generic_topic_locked(pivot, parsed, topic, ts, raw_payload=payload_text)

                self._refresh_status_locked(pivot, ts)
                self._prune_pivot_locked(pivot, ts)
                self._persist_pivot_snapshot_locked(pivot, ts)
                self._dirty = True
                self._invalidate_api_caches_locked()
                return {
                    "accepted": True,
                    "pivot_id": pivot_id,
                    "event": topic,
                    "session_id": pivot.get("session_id"),
                }

        if write_expected_queue_after:
            self.write()
        return result

    def _record_modem_reset_ack_if_applicable_locked(self, topic, payload_text, ts):
        if not topic or not payload_text:
            return None
        if not payload_text.startswith("#92-") or not payload_text.endswith("-reset_system$"):
            return None

        parsed, parse_error = parse_device_payload(payload_text)
        if parse_error or not isinstance(parsed, dict):
            return None

        if str(parsed.get("idp") or "").strip() != "92":
            return None

        parts = parsed.get("parts")
        if not isinstance(parts, list) or len(parts) < 3:
            return None
        if str(parts[2] or "").strip().lower() != "reset_system":
            return None

        pivot_id = str(parsed.get("pivot_id") or "").strip()
        if not pivot_id:
            return None
        if topic != pivot_id:
            self.log.warning(
                "ACK de reset descartado por topico divergente: topic=%s pivot_id=%s payload=%s",
                topic,
                pivot_id,
                payload_text,
            )
            return {"accepted": False, "reason": "ack reset em topico divergente", "pivot_id": pivot_id}

        pivot = self.pivots.get(pivot_id)
        if pivot is None:
            self.log.info(
                "ACK de reset descartado para pivot nao descoberto: pivot_id=%s topic=%s",
                pivot_id,
                topic,
            )
            return {"accepted": False, "reason": "ack reset para pivot nao descoberto", "pivot_id": pivot_id}

        modem_reset = pivot.get("modem_reset")
        if not isinstance(modem_reset, dict):
            modem_reset = {}
            pivot["modem_reset"] = modem_reset

        modem_reset["last_ack_ts"] = ts
        modem_reset["last_ack_topic"] = topic
        modem_reset["last_ack_payload"] = payload_text
        modem_reset["last_ack_idp"] = str(parsed.get("idp") or "")
        modem_reset["ack_count"] = int(modem_reset.get("ack_count") or 0) + 1

        self._persist_pivot_snapshot_locked(pivot, ts)
        self._dirty = True
        self._invalidate_api_caches_locked()

        self.log.info("ACK de reset registrado: pivot_id=%s topic=%s payload=%s", pivot_id, topic, payload_text)
        return {
            "accepted": True,
            "pivot_id": pivot_id,
            "event": "modem_reset_ack",
            "session_id": pivot.get("session_id"),
        }

    def tick(self, now=None):
        now = float(now if now is not None else time.time())
        send_candidates = []
        changed = False

        with self._lock:
            if self._monitoring_mode != "live":
                return False
            for pivot in self.pivots.values():
                pivot_changed = False
                timed_out = self._check_probe_timeout_locked(pivot, now)
                if timed_out:
                    pivot_changed = True
                    changed = True
                if self._refresh_status_locked(pivot, now):
                    pivot_changed = True
                    changed = True
                if self._prune_pivot_locked(pivot, now):
                    pivot_changed = True
                    changed = True
                if pivot_changed:
                    self._persist_pivot_snapshot_locked(pivot, now)
                if (not timed_out) and self._probe_should_send_locked(pivot, now):
                    send_candidates.append(pivot["pivot_id"])

            self._cleanup_pending_ping_locked(now)
            self._cleanup_dedupe_locked(now)

        for pivot_id in send_candidates:
            if pivot_id in MONITOR_TOPICS:
                self.log.error(
                    "Bloqueio de seguranca: tentativa de publicar probe em topico fixo monitorado (%s)",
                    pivot_id,
                )
                continue

            sender = self._probe_sender
            if sender is None:
                continue

            ok = False
            try:
                ok = bool(sender(pivot_id, "#11$"))
            except Exception as exc:
                self.log.exception("Erro ao enviar probe #11$ para %s: %s", pivot_id, exc)

            if ok:
                with self._lock:
                    pivot = self.pivots.get(pivot_id)
                    if pivot is not None:
                        self._record_probe_sent_locked(pivot, now)
                        self._refresh_status_locked(pivot, now)
                        self._persist_pivot_snapshot_locked(pivot, now)
                        changed = True
            else:
                self.log.warning("Falha ao publicar probe #11$ para pivot %s", pivot_id)

        if changed:
            with self._lock:
                self._dirty = True
                self._invalidate_api_caches_locked()
        return changed

    def write(self):
        now = time.time()
        with self._lock:
            state_payload = self._build_state_snapshot_locked(now)
            pivot_payloads = {
                pivot_id: self._build_pivot_snapshot_locked(pivot, now)
                for pivot_id, pivot in self.pivots.items()
            }
            mapping = [
                {
                    "pivot_id": pivot_id,
                    "slug": pivot["pivot_slug"],
                    "file": f"pivot_{pivot['pivot_slug']}.json",
                }
                for pivot_id, pivot in sorted(self.pivots.items(), key=lambda item: item[0].lower())
            ]
            runtime_payload = self._build_runtime_payload_locked(now)

            self._dirty = False
            self._last_write_ts = now

        try:
            self.persistence.record_summary_cards_hourly(
                now,
                self._build_summary_cards_counts_from_state_payload(state_payload),
            )
        except RuntimeError:
            pass

        write_json_atomic(os.path.join(DATA_DIR, "state.json"), state_payload)
        write_json_atomic(os.path.join(DATA_DIR, "pivots.json"), mapping)

        for pivot_id, payload in pivot_payloads.items():
            slug = slugify(pivot_id)
            write_json_atomic(os.path.join(DATA_DIR, f"pivot_{slug}.json"), payload)

        write_json_atomic(self.runtime_path, runtime_payload)

    def _build_summary_cards_counts_from_state_payload(self, state_payload):
        counts = {
            "total_count": 0,
            "connected_count": 0,
            "disconnected_count": 0,
            "initial_count": 0,
            "quality_green_count": 0,
            "quality_calculating_count": 0,
            "quality_yellow_count": 0,
            "quality_critical_count": 0,
        }
        for item in state_payload.get("pivots", []) or []:
            if not isinstance(item, dict):
                continue
            counts["total_count"] += 1
            status = item.get("status") if isinstance(item.get("status"), dict) else {}
            status_code = str(status.get("code") or "").strip().lower()
            if status_code == "green":
                counts["connected_count"] += 1
            elif status_code == "red":
                counts["disconnected_count"] += 1
            elif status_code == "gray":
                counts["initial_count"] += 1

            quality = item.get("quality") if isinstance(item.get("quality"), dict) else {}
            quality_code = str(quality.get("code") or "").strip().lower()
            if quality_code == "green":
                counts["quality_green_count"] += 1
            elif quality_code == "calculating":
                counts["quality_calculating_count"] += 1
            elif quality_code == "yellow":
                counts["quality_yellow_count"] += 1
            elif quality_code == "critical":
                counts["quality_critical_count"] += 1
        return counts

    def get_summary_cards_history_snapshot(self, now=None):
        safe_now = float(now if now is not None else time.time())
        try:
            points = self.persistence.fetch_summary_cards_hourly_history(
                now=safe_now,
                window_sec=SUMMARY_CARDS_HISTORY_WINDOW_SEC,
                limit=int(SUMMARY_CARDS_HISTORY_WINDOW_SEC // SUMMARY_CARDS_HISTORY_BUCKET_SEC),
            )
        except RuntimeError:
            points = []

        latest = points[-1] if points else {}
        return {
            "updated_at_ts": safe_now,
            "updated_at": _ts_to_str(safe_now),
            "bucket_sec": SUMMARY_CARDS_HISTORY_BUCKET_SEC,
            "window_sec": SUMMARY_CARDS_HISTORY_WINDOW_SEC,
            "source": "live",
            "points": points,
            "latest": {
                "ts": latest.get("ts"),
                "at": latest.get("at"),
                "total_count": int(latest.get("total_count") or 0),
                "connected_count": int(latest.get("connected_count") or 0),
                "disconnected_count": int(latest.get("disconnected_count") or 0),
                "initial_count": int(latest.get("initial_count") or 0),
                "quality_green_count": int(latest.get("quality_green_count") or 0),
                "quality_calculating_count": int(latest.get("quality_calculating_count") or 0),
                "quality_yellow_count": int(latest.get("quality_yellow_count") or 0),
                "quality_critical_count": int(latest.get("quality_critical_count") or 0),
            },
        }

    def get_connected_pivots_history_snapshot(self, now=None):
        return self.get_summary_cards_history_snapshot(now=now)

    def get_state_snapshot(self, now=None, run_id=None):
        now = float(now if now is not None else time.time())
        normalized_run = str(run_id or "").strip()
        cache_key = self._api_cache_key(normalized_run if normalized_run else None)
        cache_ttl_sec = self.api_state_cache_ttl_sec
        if normalized_run:
            with self._lock:
                cached_payload = self._get_cached_api_payload_locked(self._state_snapshot_cache, cache_key, now)
                if cached_payload is not None:
                    return cached_payload
                cache_generation = int(self._api_cache_generation)
                settings = self._build_state_settings_locked()
                expected_pivots_pending = self._build_expected_pivots_pending_locked()

            try:
                persisted = self.persistence.get_run_state_payload(
                    run_id=normalized_run,
                    connectivity_settings={
                        "ping_expected_sec": self.ping_expected_sec,
                        "tolerance_factor": self.tolerance_factor,
                    },
                )
            except RuntimeError:
                persisted = None
            if persisted is None:
                payload = {
                    "updated_at": _ts_to_str(now),
                    "updated_at_ts": now,
                    "run_id": normalized_run,
                    "run": None,
                    "settings": settings,
                    "counts": {
                        "pivots": 0,
                        "pending_ping_unknown": 0,
                        "expected_pivots_pending": len(expected_pivots_pending),
                        "malformed_messages": 0,
                        "duplicate_drops": 0,
                    },
                    "pivots": [],
                    "pending_ping": [],
                    "expected_pivots_pending": expected_pivots_pending,
                    "malformed_recent": [],
                    "mode": "history",
                }
                with self._lock:
                    self._set_cached_api_payload_locked(
                        self._state_snapshot_cache,
                        cache_key,
                        payload,
                        ttl_sec=cache_ttl_sec,
                        now_ts=now,
                        expected_generation=cache_generation,
                    )
                return payload

            pivot_items = persisted.get("pivots") if isinstance(persisted.get("pivots"), list) else []
            run_info = persisted.get("run") if isinstance(persisted.get("run"), dict) else None
            mode = "live" if bool((run_info or {}).get("is_active")) else "history"
            payload = {
                "updated_at": persisted.get("updated_at") or _ts_to_str(now),
                "updated_at_ts": _safe_float(persisted.get("updated_at_ts"), now),
                "run_id": str(persisted.get("run_id") or normalized_run),
                "run": run_info,
                "settings": settings,
                "counts": {
                    "pivots": len(pivot_items),
                    "pending_ping_unknown": 0,
                    "expected_pivots_pending": len(expected_pivots_pending),
                    "malformed_messages": 0,
                    "duplicate_drops": 0,
                },
                "pivots": pivot_items,
                "pending_ping": [],
                "expected_pivots_pending": expected_pivots_pending,
                "malformed_recent": [],
                "mode": mode,
            }
            with self._lock:
                self._set_cached_api_payload_locked(
                    self._state_snapshot_cache,
                    cache_key,
                    payload,
                    ttl_sec=cache_ttl_sec,
                    now_ts=now,
                    expected_generation=cache_generation,
                )
            return payload

        with self._lock:
            cached_payload = self._get_cached_api_payload_locked(self._state_snapshot_cache, cache_key, now)
            if cached_payload is not None:
                return cached_payload

            payload = self._build_state_snapshot_locked(now)
            self._set_cached_api_payload_locked(
                self._state_snapshot_cache,
                cache_key,
                payload,
                ttl_sec=cache_ttl_sec,
                now_ts=now,
                expected_generation=self._api_cache_generation,
            )
            return payload

    def get_pivot_snapshot(self, pivot_id, now=None, session_id=None, run_id=None):
        explicit_now = now is not None
        now = float(now if now is not None else time.time())
        normalized = str(pivot_id or "").strip()
        normalized_run = str(run_id or "").strip() or None
        if not normalized:
            return None

        # Mantem compatibilidade para simuladores/tests que passam "now" explicitamente.
        if explicit_now and session_id is None and normalized_run is None:
            with self._lock:
                pivot = self.pivots.get(normalized)
                if pivot is not None:
                    return self._build_pivot_snapshot_locked(pivot, now)

        try:
            panel = self.persistence.get_panel_payload(
                normalized,
                session_id=session_id,
                run_id=normalized_run,
            )
        except RuntimeError:
            panel = None
        if panel is not None:
            return panel
        if normalized_run is not None:
            return None

        with self._lock:
            pivot = self.pivots.get(normalized)
            if pivot is None:
                return None
            return self._build_pivot_snapshot_locked(pivot, now)

    def get_complete_panel(self, pivot_id, session_id=None, run_id=None, now=None):
        return self.get_pivot_snapshot(pivot_id, now=now, session_id=session_id, run_id=run_id)

    def get_quality_cards_snapshot(self, run_id=None, pivot_ids=None):
        normalized_run = str(run_id or "").strip() or None
        normalized_pivot_ids = []
        seen_pivot_ids = set()
        for raw_pivot_id in pivot_ids or []:
            pivot_id = str(raw_pivot_id or "").strip()
            if not pivot_id or pivot_id in seen_pivot_ids:
                continue
            seen_pivot_ids.add(pivot_id)
            normalized_pivot_ids.append(pivot_id)
        now = time.time()
        quality_scope = ",".join(normalized_pivot_ids)
        cache_key = self._api_cache_key(
            f"{normalized_run or '__default__'}|quality:{quality_scope or '__all__'}"
        )
        cache_ttl_sec = self.api_quality_cache_ttl_sec
        with self._lock:
            cached_payload = self._get_cached_api_payload_locked(self._quality_cards_cache, cache_key, now)
            if cached_payload is not None:
                return cached_payload
            cache_generation = int(self._api_cache_generation)

        try:
            payload = self.persistence.get_quality_cards_payload(
                run_id=normalized_run,
                timeline_limit=self.max_events_per_pivot_list,
                pivot_ids=normalized_pivot_ids,
            )
        except RuntimeError:
            payload = None
        if payload is not None:
            with self._lock:
                self._set_cached_api_payload_locked(
                    self._quality_cards_cache,
                    cache_key,
                    payload,
                    ttl_sec=cache_ttl_sec,
                    now_ts=now,
                    expected_generation=cache_generation,
                )
            return payload

        now = time.time()
        with self._lock:
            fallback_pivots = []
            for pivot in self.pivots.values():
                pivot_payload = self._build_pivot_snapshot_locked(pivot, now)
                fallback_pivots.append(
                    {
                        "pivot_id": str(pivot_payload.get("pivot_id") or ""),
                        "pivot_slug": str(pivot_payload.get("pivot_slug") or ""),
                        "session_id": str(pivot_payload.get("session_id") or ""),
                        "run_id": str(pivot_payload.get("run_id") or ""),
                        "updated_at_ts": _safe_float(pivot_payload.get("updated_at_ts"), now),
                        "updated_at": str(pivot_payload.get("updated_at") or _ts_to_str(now)),
                        "summary": pivot_payload.get("summary")
                        if isinstance(pivot_payload.get("summary"), dict)
                        else {},
                        "timeline": (
                            [
                                {
                                    "ts": _safe_float(event.get("ts"), None),
                                    "topic": str(event.get("topic") or ""),
                                }
                                for event in (pivot_payload.get("timeline") or [])
                                if isinstance(event, dict)
                            ][-self.max_events_per_pivot_panel :]
                        ),
                    }
                )

            run_payload = self._build_runtime_payload_locked(now).get("active_run")
            payload = {
                "run_id": str((run_payload or {}).get("run_id") or normalized_run or ""),
                "run": run_payload,
                "updated_at_ts": now,
                "updated_at": _ts_to_str(now),
                "pivots": fallback_pivots,
            }
            self._set_cached_api_payload_locked(
                self._quality_cards_cache,
                cache_key,
                payload,
                ttl_sec=cache_ttl_sec,
                now_ts=now,
                expected_generation=cache_generation,
            )
            return payload

    def list_monitoring_runs(self, limit=200):
        try:
            return self.persistence.list_runs(limit=limit)
        except RuntimeError:
            return []

    def get_cloud2_filter_options(self, run_id=None):
        normalized_run = str(run_id or "").strip() or None
        now = time.time()
        cache_key = self._api_cache_key(f"{normalized_run or '__default__'}|cloud2-filter-options")
        cache_ttl_sec = self.api_state_cache_ttl_sec
        with self._lock:
            cached_payload = self._get_cached_api_payload_locked(
                self._cloud2_filter_options_cache,
                cache_key,
                now,
            )
            if cached_payload is not None:
                return cached_payload
            cache_generation = int(self._api_cache_generation)
        try:
            payload = self.persistence.get_cloud2_filter_options(run_id=normalized_run)
            normalized_payload = {
                "run_id": str(payload.get("run_id") or "").strip() or None,
                "technologies": list(payload.get("technologies") or []),
                "firmwares": list(payload.get("firmwares") or []),
            }
            with self._lock:
                self._set_cached_api_payload_locked(
                    self._cloud2_filter_options_cache,
                    cache_key,
                    normalized_payload,
                    ttl_sec=cache_ttl_sec,
                    now_ts=now,
                    expected_generation=cache_generation,
                )
            return normalized_payload
        except RuntimeError:
            pass

        with self._lock:
            by_technology = {}
            by_firmware = {}
            for pivot in self.pivots.values():
                cloud2 = pivot.get("last_cloud2") if isinstance(pivot.get("last_cloud2"), dict) else {}
                is_concentrator = bool(pivot.get("is_concentrator"))
                technology = CONCENTRATOR_TECH_LABEL if is_concentrator else str(cloud2.get("technology") or "").strip()
                firmware = str(cloud2.get("firmware") or "").strip()
                if technology:
                    key = technology.lower()
                    if key not in by_technology:
                        by_technology[key] = technology
                if firmware:
                    key = firmware.lower()
                    if key not in by_firmware:
                        by_firmware[key] = firmware

            technologies = sorted(by_technology.values(), key=lambda value: value.lower())
            firmwares = sorted(by_firmware.values(), key=lambda value: value.lower())
            payload = {
                "run_id": normalized_run,
                "technologies": technologies,
                "firmwares": firmwares,
            }
            self._set_cached_api_payload_locked(
                self._cloud2_filter_options_cache,
                cache_key,
                payload,
                ttl_sec=cache_ttl_sec,
                now_ts=now,
                expected_generation=self._api_cache_generation,
            )
            return payload

    def purge_database_records(self, password, now=None, source="ui"):
        expected_password = get_db_purge_password()
        if not expected_password:
            raise ValueError(
                "senha de limpeza nao configurada; defina DB_PURGE_PASSWORD em cloudv2_local_secrets.py"
            )

        provided_password = str(password or "")
        if provided_password != expected_password:
            raise ValueError("senha invalida")

        current_ts = float(now if now is not None else time.time())
        with self._lock:
            self.persistence.purge_all_data()
            self._active_session_by_pivot = {}
            self._active_run_id = None
            self._monitoring_mode = "idle"
            self.pivots = {}
            self.pending_ping_unknown = {}
            self.malformed_messages = []
            self.duplicate_count = 0
            self._dedupe_cache = {}
            self._event_seq = 0
            self._dirty = True
            self._invalidate_api_caches_locked()

            result = {
                "ok": True,
                "mode": self._monitoring_mode,
                "purged_at_ts": current_ts,
                "purged_at": _ts_to_str(current_ts),
                "source": str(source or "ui"),
            }

        self._clear_dashboard_data_files()
        self.write()
        return result

    def _pivot_exists_locked(self, pivot_id):
        normalized = str(pivot_id or "").strip()
        if not normalized:
            return False
        if normalized in self.pivots:
            return True
        try:
            return bool(self.persistence.pivot_exists(normalized))
        except RuntimeError:
            return False

    def _ensure_known_pivot_for_command_locked(self, pivot_id, ts):
        pivot = self.pivots.get(pivot_id)
        if pivot is not None:
            return pivot
        if not self._pivot_exists_locked(pivot_id):
            return None

        try:
            panel = self.persistence.get_panel_payload(pivot_id)
        except RuntimeError:
            panel = None
        if panel is not None:
            pivot = self._restore_pivot_from_panel_locked(
                panel,
                fallback_run_id=self._active_run_id,
                now=ts,
            )
            if pivot is not None:
                self.pivots[pivot_id] = pivot
                return pivot

        return self._get_or_create_pivot_locked(pivot_id, ts)

    def _cleanup_expected_pivots_locked(self):
        keep = {}
        changed = False
        for pivot_id, raw_entry in self.pending_expected_pivots.items():
            normalized = str(pivot_id or "").strip()
            if not normalized or (not validate_pivot_id(normalized)):
                changed = True
                continue
            if self._pivot_exists_locked(normalized):
                changed = True
                continue
            entry = raw_entry if isinstance(raw_entry, dict) else {}
            next_entry = {
                "pivot_id": normalized,
                "added_at_ts": _safe_float(entry.get("added_at_ts"), None),
                "source": str(entry.get("source") or "ui").strip() or "ui",
            }
            keep[normalized] = next_entry
            if raw_entry != next_entry or pivot_id != normalized:
                changed = True
        self.pending_expected_pivots = keep
        return changed

    def _build_expected_pivots_pending_locked(self):
        items = []
        for pivot_id, entry in sorted(self.pending_expected_pivots.items(), key=lambda item: item[0].lower()):
            items.append(
                {
                    "pivot_id": pivot_id,
                    "added_at_ts": _safe_float(entry.get("added_at_ts"), None),
                    "added_at": _ts_to_str(entry.get("added_at_ts")),
                    "source": str(entry.get("source") or "ui").strip() or "ui",
                }
            )
        return items

    def queue_expected_pivots(self, pivot_ids, now=None, source="ui"):
        if not isinstance(pivot_ids, list):
            raise ValueError("pivot_ids obrigatorio")

        normalized_ids = []
        seen = set()
        for raw_pivot_id in pivot_ids:
            pivot_id = str(raw_pivot_id or "").strip()
            if not pivot_id or pivot_id in seen:
                continue
            seen.add(pivot_id)
            normalized_ids.append(pivot_id)

        if not normalized_ids:
            raise ValueError("pivot_ids obrigatorio")

        current_ts = float(now if now is not None else time.time())
        results = []
        added_count = 0
        should_write = False
        with self._lock:
            cleaned = self._cleanup_expected_pivots_locked()
            if cleaned:
                should_write = True
            for pivot_id in normalized_ids:
                if not validate_pivot_id(pivot_id):
                    results.append(
                        {
                            "ok": False,
                            "pivot_id": pivot_id,
                            "status": "invalid",
                            "message": "pivot_id invalido",
                        }
                    )
                    continue
                if self._pivot_exists_locked(pivot_id):
                    results.append(
                        {
                            "ok": False,
                            "pivot_id": pivot_id,
                            "status": "already_exists",
                            "message": "pivot_id ja existe",
                        }
                    )
                    continue
                if pivot_id in self.pending_expected_pivots:
                    results.append(
                        {
                            "ok": False,
                            "pivot_id": pivot_id,
                            "status": "already_pending",
                            "message": "pivot_id ja aguardando descoberta",
                        }
                    )
                    continue

                self.pending_expected_pivots[pivot_id] = {
                    "pivot_id": pivot_id,
                    "added_at_ts": current_ts,
                    "source": str(source or "ui").strip() or "ui",
                }
                added_count += 1
                should_write = True
                results.append(
                    {
                        "ok": True,
                        "pivot_id": pivot_id,
                        "status": "added",
                        "message": "pivot_id adicionado para descoberta",
                    }
                )

            if added_count:
                self._dirty = True
                self._invalidate_api_caches_locked()
            if cleaned or added_count:
                self._persist_pending_expected_pivots_locked()
            response = {
                "ok": added_count == len(results) and bool(results),
                "partial": 0 < added_count < len(results),
                "added_count": added_count,
                "results": results,
                "pending": self._build_expected_pivots_pending_locked(),
            }
        if should_write:
            self.write()
        return response

    def remove_expected_pivot(self, pivot_id, now=None, source="ui"):
        normalized = str(pivot_id or "").strip()
        if not normalized:
            raise ValueError("pivot_id obrigatorio")
        if not validate_pivot_id(normalized):
            raise ValueError("pivot_id invalido")

        current_ts = float(now if now is not None else time.time())
        should_write = False
        with self._lock:
            removed = self.pending_expected_pivots.pop(normalized, None)
            if removed is None:
                return {
                    "ok": False,
                    "pivot_id": normalized,
                    "status": "not_found",
                    "message": "pivot_id nao estava aguardando descoberta",
                }

            self._dirty = True
            self._invalidate_api_caches_locked()
            self._persist_pending_expected_pivots_locked()
            should_write = True
            response = {
                "ok": True,
                "pivot_id": normalized,
                "status": "removed",
                "removed_at_ts": current_ts,
                "removed_at": _ts_to_str(current_ts),
                "source": str(source or "ui").strip() or "ui",
            }
        if should_write:
            self.write()
        return response

    def delete_pivot(self, pivot_id, now=None, source="ui"):
        normalized = str(pivot_id or "").strip()
        if not normalized:
            raise ValueError("pivot_id obrigatorio")

        current_ts = float(now if now is not None else time.time())
        with self._lock:
            removed_runtime = False
            if normalized in self.pivots:
                del self.pivots[normalized]
                removed_runtime = True
            if normalized in self._active_session_by_pivot:
                del self._active_session_by_pivot[normalized]
            if normalized in self.pending_ping_unknown:
                del self.pending_ping_unknown[normalized]
            if normalized in self._probe_settings:
                del self._probe_settings[normalized]

            removed_db = self.persistence.delete_pivot(normalized)
            self._dirty = True
            self._invalidate_api_caches_locked()

            result = {
                "ok": True,
                "pivot_id": normalized,
                "removed_runtime": removed_runtime,
                "removed_db": bool(removed_db),
                "mode": self._monitoring_mode,
                "source": str(source or "ui"),
                "deleted_at_ts": current_ts,
                "deleted_at": _ts_to_str(current_ts),
            }

        self.write()
        return result

    def _build_drop_events_from_cloud2_locked(self, cloud2_events):
        drops = []
        for event in cloud2_events:
            if not isinstance(event, dict):
                continue
            ts_value = _safe_float(event.get("ts"), None)
            duration_sec = _safe_float(event.get("drop_duration_sec"), None)
            if ts_value is None or duration_sec is None or duration_sec <= 0:
                continue
            drops.append(
                {
                    "ts": ts_value,
                    "at": _ts_to_str(ts_value),
                    "duration_sec": duration_sec,
                    "technology": event.get("technology"),
                    "rssi": event.get("rssi"),
                }
            )
        if len(drops) > self.max_events_per_pivot:
            return drops[-self.max_events_per_pivot :]
        return drops

    def _restore_pivot_from_panel_locked(self, panel, fallback_run_id, now):
        if not isinstance(panel, dict):
            return None

        pivot_id = str(panel.get("pivot_id") or "").strip()
        if not pivot_id:
            return None

        summary = panel.get("summary") if isinstance(panel.get("summary"), dict) else {}
        session_meta = panel.get("session") if isinstance(panel.get("session"), dict) else {}
        probe_summary = summary.get("probe") if isinstance(summary.get("probe"), dict) else {}
        panel_is_concentrator = panel.get("is_concentrator")
        if panel_is_concentrator is None:
            panel_is_concentrator = summary.get("is_concentrator")
        resolved_is_concentrator = False
        if isinstance(panel_is_concentrator, bool):
            resolved_is_concentrator = panel_is_concentrator
        elif isinstance(panel_is_concentrator, (int, float)):
            resolved_is_concentrator = bool(panel_is_concentrator)
        elif isinstance(panel_is_concentrator, str):
            normalized_flag = panel_is_concentrator.strip().lower()
            if normalized_flag in ("1", "true", "yes", "sim"):
                resolved_is_concentrator = True
            elif normalized_flag in ("0", "false", "no", "nao", "não"):
                resolved_is_concentrator = False

        run_id = str(
            panel.get("run_id")
            or summary.get("run_id")
            or fallback_run_id
            or ""
        ).strip() or None
        session_id = str(
            panel.get("session_id")
            or session_meta.get("session_id")
            or summary.get("session_id")
            or ""
        ).strip() or None

        timeline_events = [item for item in (panel.get("timeline") or []) if isinstance(item, dict)]
        timeline_events.sort(key=lambda item: _safe_float(item.get("ts"), 0))
        if len(timeline_events) > self.max_events_per_pivot:
            timeline_events = timeline_events[-self.max_events_per_pivot :]

        cloud2_events = [item for item in (panel.get("cloud2_events") or []) if isinstance(item, dict)]
        cloud2_events.sort(key=lambda item: _safe_float(item.get("ts"), 0))
        if len(cloud2_events) > self.max_events_per_pivot:
            cloud2_events = cloud2_events[-self.max_events_per_pivot :]

        probe_events = [item for item in (panel.get("probe_events") or []) if isinstance(item, dict)]
        probe_events.sort(key=lambda item: _safe_float(item.get("ts"), 0))
        if len(probe_events) > self.max_events_per_pivot:
            probe_events = probe_events[-self.max_events_per_pivot :]

        rssi_series = [item for item in (panel.get("rssiSeries") or []) if isinstance(item, dict)]
        rssi_series.sort(key=lambda item: _safe_float(item.get("ts"), 0))
        if len(rssi_series) > self.max_events_per_pivot:
            rssi_series = rssi_series[-self.max_events_per_pivot :]

        first_timeline_ts = None
        for event in timeline_events:
            event_ts = _safe_float(event.get("ts"), None)
            if event_ts is not None and event_ts > 0:
                first_timeline_ts = event_ts
                break

        discovered_ts = (
            _safe_float(summary.get("discovered_at_ts"), None)
            or _safe_float(summary.get("last_activity_ts"), None)
            or _safe_float(summary.get("last_cloudv2_ts"), None)
            or _safe_float(summary.get("last_ping_ts"), None)
            or first_timeline_ts
            or _safe_float(panel.get("updated_at_ts"), None)
            or now
        )

        pivot = self._new_pivot_state(
            pivot_id,
            discovered_ts,
            session_id=session_id,
            run_id=run_id,
            is_concentrator=resolved_is_concentrator,
        )

        topic_counters = {topic: 0 for topic in self.monitor_topics}
        topic_last_ts = {topic: None for topic in CONNECTIVITY_TOPICS}
        topic_intervals = {topic: [] for topic in CONNECTIVITY_TOPICS}
        last_seen_candidates = []

        for event in timeline_events:
            event_ts = _safe_float(event.get("ts"), None)
            if event_ts is None:
                continue
            last_seen_candidates.append(event_ts)
            topic = str(event.get("topic") or "").strip()
            if topic in topic_counters:
                topic_counters[topic] = int(topic_counters.get(topic, 0)) + 1
            if topic not in CONNECTIVITY_TOPICS:
                continue

            previous_ts = _safe_float(topic_last_ts.get(topic), None)
            if previous_ts is not None and event_ts > previous_ts:
                intervals = topic_intervals.get(topic, [])
                intervals.append(event_ts - previous_ts)
                if len(intervals) > self.cloudv2_window:
                    intervals = intervals[-self.cloudv2_window :]
                topic_intervals[topic] = intervals
            topic_last_ts[topic] = event_ts

        summary_counters = summary.get("topic_counters")
        if isinstance(summary_counters, dict):
            for topic in self.monitor_topics:
                counter_value = _safe_int(summary_counters.get(topic), None)
                if counter_value is not None and counter_value >= 0:
                    topic_counters[topic] = counter_value

        last_ping_ts = _safe_float(summary.get("last_ping_ts"), topic_last_ts.get(TOPIC_PING))
        last_cloudv2_ts = _safe_float(summary.get("last_cloudv2_ts"), topic_last_ts.get(TOPIC_CLOUDV2))
        last_cloud2 = summary.get("last_cloud2") if isinstance(summary.get("last_cloud2"), dict) else None
        if last_cloud2 is None and cloud2_events:
            last_cloud2 = dict(cloud2_events[-1])
        last_cloud2_ts = _safe_float((last_cloud2 or {}).get("ts"), None)
        if last_cloud2_ts is None and cloud2_events:
            last_cloud2_ts = _safe_float(cloud2_events[-1].get("ts"), None)

        if last_ping_ts is not None:
            topic_last_ts[TOPIC_PING] = last_ping_ts
        if last_cloudv2_ts is not None:
            topic_last_ts[TOPIC_CLOUDV2] = last_cloudv2_ts

        for value in (last_ping_ts, last_cloudv2_ts, last_cloud2_ts):
            if value is not None:
                last_seen_candidates.append(value)
        last_seen_ts = max(last_seen_candidates) if last_seen_candidates else discovered_ts

        probe = pivot["probe"]
        probe["enabled"] = bool(probe_summary.get("enabled", probe.get("enabled")))
        probe_interval = _safe_int(probe_summary.get("interval_sec"), probe.get("interval_sec"))
        if probe_interval is None:
            probe_interval = self.probe_default_interval_sec
        if probe_interval < self.probe_min_interval_sec:
            probe_interval = self.probe_min_interval_sec
        probe["interval_sec"] = probe_interval
        probe["events"] = probe_events
        probe["last_sent_ts"] = _safe_float(probe_summary.get("last_sent_ts"), None)
        probe["last_response_ts"] = _safe_float(probe_summary.get("last_response_ts"), None)

        pending = bool(probe_summary.get("pending"))
        pending_deadline_ts = _safe_float(probe_summary.get("pending_deadline_ts"), None)
        pending_sent_ts = probe["last_sent_ts"] if pending else None
        if pending and pending_deadline_ts is None and pending_sent_ts is not None:
            pending_deadline_ts = pending_sent_ts + (probe_interval * self.probe_timeout_factor)
        probe["pending_sent_ts"] = pending_sent_ts
        probe["pending_deadline_ts"] = pending_deadline_ts
        probe["timeout_streak"] = max(0, _safe_int(probe_summary.get("timeout_streak"), 0) or 0)
        probe_last_result = probe_summary.get("last_result")
        probe["last_result"] = str(probe_last_result).strip().lower() if probe_last_result not in (None, "") else None

        pivot["run_id"] = run_id
        pivot["session_id"] = session_id
        pivot["last_seen_ts"] = last_seen_ts
        pivot["last_ping_ts"] = last_ping_ts
        pivot["last_cloudv2_ts"] = last_cloudv2_ts
        pivot["last_cloud2_ts"] = last_cloud2_ts
        pivot["topic_counters"] = topic_counters
        pivot["topic_last_ts"] = topic_last_ts
        pivot["topic_intervals_sec"] = topic_intervals
        pivot["cloudv2_intervals_sec"] = list(topic_intervals.get(TOPIC_CLOUDV2) or [])
        pivot["last_cloud2"] = last_cloud2
        pivot["timeline"] = timeline_events
        pivot["cloud2_events"] = cloud2_events
        pivot["ping_rssi_points"] = rssi_series
        pivot["drop_events"] = self._build_drop_events_from_cloud2_locked(cloud2_events)
        pivot["pivot_config"] = _normalize_pivot_config_state(summary.get("pivot_config"))
        pivot["network_config"] = _normalize_network_config_state(summary.get("network_config"))
        pivot["rush_config"] = _normalize_rush_config_state(summary.get("rush_config"))
        pivot["sector_config"] = _normalize_sector_config_state(summary.get("sector_config"))
        pivot["physical_barrier_config"] = _normalize_physical_barrier_config_state(summary.get("physical_barrier_config"))

        status_summary = summary.get("status") if isinstance(summary.get("status"), dict) else {}
        quality_summary = summary.get("quality") if isinstance(summary.get("quality"), dict) else {}
        pivot["status_cache"] = {
            "code": str(status_summary.get("code") or "gray"),
            "reason": str(status_summary.get("reason") or "Aguardando amostras iniciais de cloudv2."),
            "quality_code": str(quality_summary.get("code") or "green"),
            "quality_reason": str(quality_summary.get("reason") or "Qualidade aguardando dados da janela."),
            "changed_at_ts": _safe_float(summary.get("last_activity_ts"), now),
        }

        return pivot

    def activate_history_run(self, run_id, now=None, source="ui"):
        self._ensure_manual_session_rotation_allowed("activate_history_run")
        normalized_run = str(run_id or "").strip()
        if not normalized_run:
            raise ValueError("run_id obrigatorio")

        current_ts = float(now if now is not None else time.time())
        with self._lock:
            run = self.persistence.activate_existing_run(normalized_run, now_ts=current_ts)
            if not run:
                raise ValueError("historico nao encontrado")

            active_sessions = self.persistence.activate_latest_sessions_for_run(normalized_run, now_ts=current_ts)
            try:
                persisted = self.persistence.get_run_state_payload(run_id=normalized_run)
            except RuntimeError:
                persisted = None
            persisted_pivots = persisted.get("pivots") if isinstance((persisted or {}).get("pivots"), list) else []

            self._active_run_id = normalized_run
            self._monitoring_mode = "live"
            self._active_session_by_pivot = dict(active_sessions)

            restored_pivots = {}
            for item in persisted_pivots:
                if not isinstance(item, dict):
                    continue

                pivot_id = str(item.get("pivot_id") or "").strip()
                if not pivot_id:
                    continue

                preferred_session_id = str(
                    self._active_session_by_pivot.get(pivot_id)
                    or item.get("session_id")
                    or ""
                ).strip() or None

                try:
                    panel = self.persistence.get_panel_payload(
                        pivot_id,
                        session_id=preferred_session_id,
                        run_id=normalized_run,
                    )
                except RuntimeError:
                    panel = None

                if panel is None:
                    panel = {
                        "pivot_id": pivot_id,
                        "run_id": normalized_run,
                        "session_id": preferred_session_id,
                        "summary": item,
                        "timeline": [],
                        "probe_events": [],
                        "cloud2_events": [],
                    }

                pivot = self._restore_pivot_from_panel_locked(
                    panel,
                    fallback_run_id=normalized_run,
                    now=current_ts,
                )
                if pivot is None:
                    continue

                active_session_id = str(self._active_session_by_pivot.get(pivot_id) or "").strip()
                if active_session_id:
                    pivot["session_id"] = active_session_id
                elif not pivot.get("session_id"):
                    pivot["session_id"] = self._ensure_active_session_locked(
                        pivot_id,
                        current_ts,
                        source="history_resume",
                    )

                pivot["run_id"] = normalized_run
                self._refresh_status_locked(pivot, current_ts)
                self._prune_pivot_locked(pivot, current_ts)
                self._persist_pivot_snapshot_locked(pivot, current_ts)
                restored_pivots[pivot_id] = pivot

            self.pivots = restored_pivots
            restored_session_map = {
                pivot_id: str(pivot.get("session_id") or "").strip()
                for pivot_id, pivot in restored_pivots.items()
                if str(pivot.get("session_id") or "").strip()
            }
            self._active_session_by_pivot = {
                pivot_id: session_id
                for pivot_id, session_id in {**active_sessions, **restored_session_map}.items()
                if str(session_id or "").strip()
            }
            self.pending_ping_unknown = {}
            self.malformed_messages = []
            self.duplicate_count = 0
            self._dedupe_cache = {}
            self._dirty = True
            self._invalidate_api_caches_locked()

            result = {
                "run_id": normalized_run,
                "run": run,
                "mode": self._monitoring_mode,
                "pivot_count": len(restored_pivots),
                "active_sessions": len(self._active_session_by_pivot),
                "source": str(source or "ui"),
                "applied_at_ts": current_ts,
                "applied_at": _ts_to_str(current_ts),
            }
        self.write()
        return result

    def start_new_monitoring_run(self, now=None, source="ui"):
        self._ensure_manual_session_rotation_allowed("start_new_monitoring_run")
        current_ts = float(now if now is not None else time.time())
        with self._lock:
            self.persistence.deactivate_all_active_sessions(now_ts=current_ts)
            created_run = self.persistence.create_new_run(
                now_ts=current_ts,
                source=source,
                label="Monitoramento global",
                metadata={"trigger": "ui_global_action"},
            )
            run_id = str((created_run or {}).get("run_id") or "").strip()
            if not run_id:
                raise ValueError("nao foi possivel criar monitoramento global")

            previous_pivots = dict(self.pivots)
            previous_pivot_ids = sorted(previous_pivots.keys(), key=lambda item: item.lower())
            self._active_run_id = run_id
            self._monitoring_mode = "live"
            self._active_session_by_pivot = {}
            self.pending_ping_unknown = {}
            self.malformed_messages = []
            self.duplicate_count = 0

            rebuilt = {}
            for pivot_id in previous_pivot_ids:
                session_id = self._ensure_active_session_locked(pivot_id, current_ts, source="new_monitoring_run")
                pivot = self._new_pivot_state(
                    pivot_id,
                    current_ts,
                    session_id=session_id,
                    run_id=run_id,
                )
                previous_pivot = previous_pivots.get(pivot_id)
                if isinstance(previous_pivot, dict):
                    baseline_summary = self._build_pivot_summary_locked(previous_pivot, current_ts)
                else:
                    baseline_summary = self._load_pivot_baseline_from_persistence_locked(pivot_id)
                if isinstance(baseline_summary, dict):
                    self._apply_baseline_snapshot_locked(pivot, baseline_summary, now=current_ts)
                rebuilt[pivot_id] = pivot
                self._record_timeline_locked(
                    pivot,
                    event_type="session_started",
                    topic="monitoring_run",
                    ts=current_ts,
                    summary="Novo monitoramento global iniciado.",
                    details={
                        "run_id": run_id,
                        "session_id": session_id,
                        "source": source,
                    },
                )
                self._refresh_status_locked(pivot, current_ts)
                self._persist_pivot_snapshot_locked(pivot, current_ts)

            self.pivots = rebuilt
            self._dirty = True
            self._invalidate_api_caches_locked()

            result = {
                "run_id": run_id,
                "run": created_run,
                "pivot_count": len(rebuilt),
                "mode": self._monitoring_mode,
            }
        self.write()
        return result

    def list_monitoring_sessions(self, pivot_id, limit=200, run_id=None):
        normalized = str(pivot_id or "").strip()
        if not normalized:
            return []
        normalized_run = str(run_id or "").strip() or None
        try:
            return self.persistence.list_sessions(normalized, limit=limit, run_id=normalized_run)
        except RuntimeError:
            return []

    def start_new_monitoring_session(self, pivot_id, now=None, source="ui"):
        self._ensure_manual_session_rotation_allowed("start_new_monitoring_session")
        normalized = str(pivot_id or "").strip()
        if not normalized:
            raise ValueError("pivot_id obrigatorio")
        if not validate_pivot_id(normalized):
            raise ValueError("pivot_id invalido")

        current_ts = float(now if now is not None else time.time())
        with self._lock:
            if not self._pivot_exists_locked(normalized):
                raise ValueError("pivot ainda nao foi autorizado para descoberta via cloudv2")
            self._monitoring_mode = "live"
            active_run_id = self._ensure_active_run_locked(current_ts, source=source)
            if not active_run_id:
                raise ValueError("nao foi possivel resolver monitoramento global ativo")
            created = self.persistence.create_new_session(
                normalized,
                pivot_slug=slugify(normalized),
                now_ts=current_ts,
                source=source,
                run_id=active_run_id,
            )
            if not created:
                raise ValueError("nao foi possivel criar sessao")

            session_id = str(created.get("session_id") or "").strip()
            self._active_session_by_pivot[normalized] = session_id

            pivot = self._new_pivot_state(
                normalized,
                current_ts,
                session_id=session_id,
                run_id=active_run_id,
            )
            previous_pivot = self.pivots.get(normalized)
            if isinstance(previous_pivot, dict):
                baseline_summary = self._build_pivot_summary_locked(previous_pivot, current_ts)
            else:
                baseline_summary = self._load_pivot_baseline_from_persistence_locked(normalized)
            if isinstance(baseline_summary, dict):
                self._apply_baseline_snapshot_locked(pivot, baseline_summary, now=current_ts)
            self.pivots[normalized] = pivot

            self._record_timeline_locked(
                pivot,
                event_type="session_started",
                topic="monitoring_session",
                ts=current_ts,
                summary="Nova sessao de monitoramento iniciada manualmente.",
                details={
                    "run_id": active_run_id,
                    "session_id": session_id,
                    "source": source,
                },
            )
            self._refresh_status_locked(pivot, current_ts)
            self._persist_pivot_snapshot_locked(pivot, current_ts)
            self._dirty = True
            self._invalidate_api_caches_locked()

            return {
                "pivot_id": normalized,
                "run_id": active_run_id,
                "session_id": session_id,
                "session": created,
            }

    def _ensure_active_run_locked(self, ts, source="runtime"):
        cached_run_id = str(self._active_run_id or "").strip()
        if cached_run_id:
            run = self.persistence.resolve_run(run_id=cached_run_id)
            if run and bool(run.get("is_active")):
                return cached_run_id

        if self.history_mode == "fresh":
            created = self.persistence.create_new_run(now_ts=ts, source=source)
        else:
            created = self.persistence.get_or_create_active_run(now_ts=ts, source=source)

        run_id = str((created or {}).get("run_id") or "").strip()
        self._active_run_id = run_id or None
        return self._active_run_id

    def _ensure_active_session_locked(self, pivot_id, ts, source="runtime"):
        normalized = str(pivot_id or "").strip()
        if not normalized:
            return None

        active_run_id = self._ensure_active_run_locked(ts, source=source)
        if not active_run_id:
            return None

        cached_session_id = str(self._active_session_by_pivot.get(normalized) or "").strip()
        if cached_session_id:
            session = self.persistence.resolve_session(
                normalized,
                session_id=cached_session_id,
                run_id=active_run_id,
            )
            if session and bool(session.get("is_active")):
                return cached_session_id

        created = self.persistence.get_or_create_active_session(
            normalized,
            pivot_slug=slugify(normalized),
            now_ts=ts,
            source=source,
            run_id=active_run_id,
        )

        session_id = str((created or {}).get("session_id") or "").strip()
        if session_id:
            self._active_session_by_pivot[normalized] = session_id
            return session_id
        return None

    def _persist_pivot_snapshot_locked(self, pivot, now):
        if not isinstance(pivot, dict):
            return None

        pivot_id = str(pivot.get("pivot_id") or "").strip()
        if not pivot_id:
            return None

        session_id = str(pivot.get("session_id") or "").strip()
        if not session_id:
            session_id = self._ensure_active_session_locked(pivot_id, now, source="runtime")
            pivot["session_id"] = session_id
        if not session_id:
            return None

        if not pivot.get("run_id"):
            pivot["run_id"] = self._active_run_id

        self.persistence.ensure_pivot(pivot_id, pivot_slug=slugify(pivot_id), seen_ts=now)
        snapshot = self._build_pivot_snapshot_locked(pivot, now)
        self.persistence.upsert_snapshot(pivot_id, session_id, snapshot, updated_at_ts=now)
        return snapshot

    def _backfill_pivot_session_locked(self, pivot):
        if not isinstance(pivot, dict):
            return

        pivot_id = str(pivot.get("pivot_id") or "").strip()
        session_id = str(pivot.get("session_id") or "").strip()
        if not pivot_id or not session_id:
            return

        if self.persistence.session_has_events(pivot_id, session_id):
            return

        timeline = sorted(list(pivot.get("timeline", [])), key=lambda item: _safe_float(item.get("ts"), 0))
        for event in timeline:
            if not isinstance(event, dict):
                continue
            details = event.get("details") if isinstance(event.get("details"), dict) else {}
            parsed_payload = details.get("parsed_payload") if isinstance(details.get("parsed_payload"), dict) else None
            raw_payload = details.get("raw_payload")
            source_topic = details.get("source_topic") or event.get("topic")
            self.persistence.insert_connectivity_event(
                pivot_id,
                session_id,
                event,
                source_topic=source_topic,
                raw_payload=raw_payload,
                parsed_payload=parsed_payload,
            )

        cloud2_events = sorted(list(pivot.get("cloud2_events", [])), key=lambda item: _safe_float(item.get("ts"), 0))
        for event in cloud2_events:
            if not isinstance(event, dict):
                continue
            self.persistence.insert_cloud2_event(pivot_id, session_id, event)

        ping_rssi_points = sorted(
            list(pivot.get("ping_rssi_points", [])),
            key=lambda item: _safe_float(item.get("ts"), 0),
        )
        for point in ping_rssi_points:
            if not isinstance(point, dict):
                continue
            self.persistence.insert_ping_rssi_point(
                pivot_id,
                session_id,
                ts=point.get("ts"),
                rssi=point.get("rssi"),
            )

        drop_events = sorted(list(pivot.get("drop_events", [])), key=lambda item: _safe_float(item.get("ts"), 0))
        for event in drop_events:
            if not isinstance(event, dict):
                continue
            self.persistence.insert_drop_event(pivot_id, session_id, event)

        probe = pivot.get("probe") if isinstance(pivot.get("probe"), dict) else {}
        probe_events = sorted(list(probe.get("events", [])), key=lambda item: _safe_float(item.get("ts"), 0))
        for event in probe_events:
            if not isinstance(event, dict):
                continue
            self.persistence.insert_probe_event(pivot_id, session_id, event)

        probe_delay_points = self._build_probe_delay_points_locked(probe_events)
        for point in probe_delay_points:
            self.persistence.insert_probe_delay_point(
                pivot_id,
                session_id,
                ts=point.get("ts"),
                latency_sec=point.get("latency_sec"),
                avg_latency_sec=point.get("avg_latency_sec"),
                median_latency_sec=point.get("median_latency_sec"),
                sample_count=point.get("sample_count"),
            )

    def get_probe_config_snapshot(self):
        with self._lock:
            items = []
            for pivot_id, setting in sorted(self._probe_settings.items(), key=lambda item: item[0].lower()):
                items.append(
                    {
                        "pivot_id": pivot_id,
                        "enabled": bool(setting.get("enabled")),
                        "interval_sec": int(setting.get("interval_sec", self.probe_default_interval_sec)),
                    }
                )
            return {
                "default_interval_sec": self.probe_default_interval_sec,
                "min_interval_sec": self.probe_min_interval_sec,
                "timeout_factor": self.probe_timeout_factor,
                "items": items,
            }

    def update_probe_setting(self, pivot_id, enabled, interval_sec):
        normalized_pivot = str(pivot_id or "").strip()
        if not normalized_pivot:
            raise ValueError("pivot_id obrigatorio")
        if not validate_pivot_id(normalized_pivot):
            raise ValueError("pivot_id invalido")

        normalized_enabled = bool(enabled)
        normalized_interval = _safe_int(interval_sec, self.probe_default_interval_sec)
        if normalized_interval is None:
            normalized_interval = self.probe_default_interval_sec
        if normalized_interval < self.probe_min_interval_sec:
            normalized_interval = self.probe_min_interval_sec

        with self._lock:
            self._probe_settings[normalized_pivot] = {
                "enabled": normalized_enabled,
                "interval_sec": normalized_interval,
            }
            self.persistence.upsert_probe_setting(normalized_pivot, normalized_enabled, normalized_interval)

            pivot = self.pivots.get(normalized_pivot)
            if pivot is not None:
                self._apply_probe_setting_to_pivot_locked(pivot, self._probe_settings[normalized_pivot])
                now_ts = time.time()
                self._refresh_status_locked(pivot, now_ts)
                self._persist_pivot_snapshot_locked(pivot, now_ts)

            self._dirty = True
            self._invalidate_api_caches_locked()

        self.log.info(
            "Configuracao de probe atualizada: pivot_id=%s enabled=%s interval_sec=%s",
            normalized_pivot,
            normalized_enabled,
            normalized_interval,
        )

        return {
            "pivot_id": normalized_pivot,
            "enabled": normalized_enabled,
            "interval_sec": normalized_interval,
        }

    def _apply_probe_setting_to_pivot_locked(self, pivot, setting):
        if not isinstance(pivot, dict):
            return

        probe = pivot.get("probe")
        if not isinstance(probe, dict):
            probe = {}
            pivot["probe"] = probe

        normalized_enabled = bool((setting or {}).get("enabled", False))
        normalized_interval = _safe_int(
            (setting or {}).get("interval_sec"),
            self.probe_default_interval_sec,
        )
        if normalized_interval is None:
            normalized_interval = self.probe_default_interval_sec
        if normalized_interval < self.probe_min_interval_sec:
            normalized_interval = self.probe_min_interval_sec

        probe["enabled"] = normalized_enabled
        probe["interval_sec"] = normalized_interval
        if not normalized_enabled:
            probe["pending_sent_ts"] = None
            probe["pending_deadline_ts"] = None

    def _sync_probe_settings_to_runtime_pivots_locked(self):
        for pivot_id, pivot in self.pivots.items():
            self._apply_probe_setting_to_pivot_locked(
                pivot,
                self._probe_settings.get(pivot_id, {}),
            )

    def update_pivot_concentrator(self, pivot_id, is_concentrator):
        normalized_pivot = str(pivot_id or "").strip()
        if not normalized_pivot:
            raise ValueError("pivot_id obrigatorio")
        if not validate_pivot_id(normalized_pivot):
            raise ValueError("pivot_id invalido")

        normalized_flag = bool(is_concentrator)
        with self._lock:
            persisted_flag = self.persistence.set_pivot_is_concentrator(
                normalized_pivot,
                normalized_flag,
                pivot_slug=slugify(normalized_pivot),
                seen_ts=time.time(),
            )
            pivot = self.pivots.get(normalized_pivot)
            if pivot is not None:
                pivot["is_concentrator"] = bool(persisted_flag)
                now_ts = time.time()
                self._refresh_status_locked(pivot, now_ts)
                self._persist_pivot_snapshot_locked(pivot, now_ts)

            self._dirty = True
            self._invalidate_api_caches_locked()

        self.log.info(
            "Tecnologia concentrador atualizada: pivot_id=%s is_concentrator=%s",
            normalized_pivot,
            persisted_flag,
        )
        return {
            "pivot_id": normalized_pivot,
            "is_concentrator": bool(persisted_flag),
        }

    def update_pivot_coordinates(self, pivot_id, latitude, longitude):
        normalized_pivot = str(pivot_id or "").strip()
        if not normalized_pivot:
            raise ValueError("pivot_id obrigatorio")
        if not validate_pivot_id(normalized_pivot):
            raise ValueError("pivot_id invalido")

        normalized_lat = _safe_float(latitude, None)
        normalized_lon = _safe_float(longitude, None)
        if normalized_lat is None or normalized_lon is None:
            raise ValueError("latitude e longitude obrigatorias")
        if normalized_lat < -90 or normalized_lat > 90:
            raise ValueError("latitude invalida")
        if normalized_lon < -180 or normalized_lon > 180:
            raise ValueError("longitude invalida")

        with self._lock:
            persisted = self.persistence.set_pivot_coordinates(
                normalized_pivot,
                normalized_lat,
                normalized_lon,
                pivot_slug=slugify(normalized_pivot),
                seen_ts=time.time(),
            )
            pivot = self.pivots.get(normalized_pivot)
            if pivot is not None:
                pivot["latitude"] = _safe_float(persisted.get("latitude"), None)
                pivot["longitude"] = _safe_float(persisted.get("longitude"), None)
                now_ts = time.time()
                self._refresh_status_locked(pivot, now_ts)
                self._persist_pivot_snapshot_locked(pivot, now_ts)

            self._dirty = True
            self._invalidate_api_caches_locked()

        self.log.info(
            "Coordenadas atualizadas: pivot_id=%s latitude=%s longitude=%s",
            normalized_pivot,
            normalized_lat,
            normalized_lon,
        )
        return {
            "pivot_id": normalized_pivot,
            "latitude": _safe_float(persisted.get("latitude"), None),
            "longitude": _safe_float(persisted.get("longitude"), None),
        }

    def send_modem_reset_command(self, pivot_id):
        normalized_pivot = str(pivot_id or "").strip()
        if not normalized_pivot:
            raise ValueError("pivot_id obrigatorio")
        if not validate_pivot_id(normalized_pivot):
            raise ValueError("pivot_id invalido")

        sender = self._modem_reset_sender
        if sender is None:
            raise RuntimeError("envio de reset nao configurado")

        with self._lock:
            pivot = self.pivots.get(normalized_pivot)
            if pivot is None:
                raise ValueError("pivot nao encontrado")

        payload = "#92$"
        command_ts = time.time()
        sent_ok = bool(sender(normalized_pivot, payload))
        if not sent_ok:
            raise RuntimeError("falha ao enviar comando de reset")
        with self._lock:
            pivot = self.pivots.get(normalized_pivot)
            if pivot is not None:
                modem_reset = pivot.get("modem_reset")
                if not isinstance(modem_reset, dict):
                    modem_reset = {}
                    pivot["modem_reset"] = modem_reset
                modem_reset["last_command_ts"] = command_ts
                modem_reset["last_command_topic"] = normalized_pivot
                modem_reset["last_command_payload"] = payload
                modem_reset["command_count"] = int(modem_reset.get("command_count") or 0) + 1
                self._record_timeline_locked(
                    pivot,
                    event_type="modem_reset_sent",
                    topic=normalized_pivot,
                    ts=command_ts,
                    summary="Comando de reset #92$ enviado no topico dinamico do pivot.",
                    details={"payload": payload},
                    source_topic=normalized_pivot,
                    raw_payload=payload,
                )
                self._persist_pivot_snapshot_locked(pivot, command_ts)
                self._dirty = True
                self._invalidate_api_caches_locked()

        self.log.info("Comando de reset #92$ enviado para pivot_id=%s", normalized_pivot)
        return {
            "pivot_id": normalized_pivot,
            "topic": normalized_pivot,
            "payload": payload,
            "command_ts": command_ts,
            "command_at": _ts_to_str(command_ts),
        }

    def send_pivot_config_request(self, pivot_id):
        return self.send_config_request(pivot_id, idp="03")

    def _build_config_update_payload(self, pivot_id, idp, values):
        safe_values = values if isinstance(values, dict) else {}
        if idp == "02":
            fields = [
                _validate_config_payload_field(safe_values.get("gprs_id"), "gprs_id"),
                _validate_config_payload_field(safe_values.get("modem_apn"), "modem_apn"),
                _validate_config_payload_field(safe_values.get("wifi_ssid"), "wifi_ssid"),
                _validate_config_payload_field(safe_values.get("wifi_pass"), "wifi_pass"),
            ]
        elif idp == "03":
            fields = [
                _validate_config_payload_field(safe_values.get("contactor"), "contactor"),
                _validate_config_payload_field(safe_values.get("pressure"), "pressure"),
                str(_validate_config_payload_int(safe_values.get("pressurization_time"), "pressurization_time", 0, 65535)),
                str(_validate_config_payload_int(safe_values.get("on_time"), "on_time", 0, 255)),
                str(_validate_config_payload_int(safe_values.get("off_time"), "off_time", 0, 255)),
                str(_validate_config_payload_int(safe_values.get("read_time"), "read_time", 0, 255)),
            ]
        elif idp == "04":
            fields = [
                _validate_config_payload_hhmm(safe_values.get("start_time_hhmm"), "start_time_hhmm"),
                _validate_config_payload_hhmm(safe_values.get("end_time_hhmm"), "end_time_hhmm"),
                _validate_config_payload_bool(safe_values.get("enabled"), "enabled"),
            ]
        elif idp == "05":
            sector_count = _validate_config_payload_sector_count(safe_values.get("sector_number"))
            fields = [str(sector_count)]
            for index in range(4):
                if index < sector_count:
                    fields.append(str(_validate_config_payload_int(_read_sector_angle_value(safe_values, index, "start_angle"), f"sector_{index + 1}_start_angle", 0, 65535)))
                    fields.append(str(_validate_config_payload_int(_read_sector_angle_value(safe_values, index, "end_angle"), f"sector_{index + 1}_end_angle", 0, 65535)))
                else:
                    fields.extend(["0", "0"])
        elif idp == "22":
            fields = [
                str(_validate_config_payload_int(safe_values.get("start_angle"), "start_angle", 0, 65535)),
                str(_validate_config_payload_int(safe_values.get("end_angle"), "end_angle", 0, 65535)),
                _validate_config_payload_bool(safe_values.get("automatic_return"), "automatic_return"),
                _validate_config_payload_bool(safe_values.get("water_return"), "water_return"),
                str(_validate_config_payload_int(safe_values.get("time_leaving_barrier"), "time_leaving_barrier", 0, 255)),
            ]
        else:
            raise ValueError("idp de configuracao nao suportado")
        return f"#{idp}-{pivot_id}-{'-'.join(fields)}$"

    def send_config_update(self, pivot_id, idp="03", values=None):
        normalized_pivot = str(pivot_id or "").strip()
        if not normalized_pivot:
            raise ValueError("pivot_id obrigatorio")
        if not validate_pivot_id(normalized_pivot):
            raise ValueError("pivot_id invalido")
        normalized_idp = str(idp or "").strip().zfill(2)
        if normalized_idp not in SUPPORTED_CONFIG_IDPS:
            raise ValueError("idp de configuracao nao suportado")

        sender = self._pivot_config_sender
        if sender is None:
            raise RuntimeError("envio de configuracao nao configurado")

        command_ts = time.time()
        with self._lock:
            pivot = self._ensure_known_pivot_for_command_locked(normalized_pivot, command_ts)
            if pivot is None:
                raise ValueError("pivot nao encontrado")

        payload = self._build_config_update_payload(normalized_pivot, normalized_idp, values)
        sent_ok = bool(sender(normalized_pivot, payload))
        if not sent_ok:
            raise RuntimeError("falha ao enviar configuracao")

        with self._lock:
            pivot = self._ensure_known_pivot_for_command_locked(normalized_pivot, command_ts)
            if pivot is not None:
                self._record_timeline_locked(
                    pivot,
                    event_type="config_update_sent",
                    topic=normalized_pivot,
                    ts=command_ts,
                    summary=f"Configuracao IDP {normalized_idp} enviada para salvar.",
                    details={"payload": payload, "idp": normalized_idp},
                    source_topic=normalized_pivot,
                    raw_payload=payload,
                )
                self._persist_pivot_snapshot_locked(pivot, command_ts)
                self._dirty = True
                self._invalidate_api_caches_locked()

        self.log.info("Configuracao IDP %s enviada para pivot_id=%s", normalized_idp, normalized_pivot)
        return {
            "pivot_id": normalized_pivot,
            "topic": normalized_pivot,
            "payload": payload,
            "idp": normalized_idp,
            "command_ts": command_ts,
            "command_at": _ts_to_str(command_ts),
        }

    def send_config_request(self, pivot_id, idp="03"):
        normalized_pivot = str(pivot_id or "").strip()
        if not normalized_pivot:
            raise ValueError("pivot_id obrigatorio")
        if not validate_pivot_id(normalized_pivot):
            raise ValueError("pivot_id invalido")
        normalized_idp = str(idp or "").strip().zfill(2)
        if normalized_idp not in SUPPORTED_CONFIG_IDPS:
            raise ValueError("idp de configuracao nao suportado")

        sender = self._pivot_config_sender
        if sender is None:
            raise RuntimeError("envio de configuracao nao configurado")

        request_ts = time.time()
        with self._lock:
            pivot = self._ensure_known_pivot_for_command_locked(normalized_pivot, request_ts)
            if pivot is None:
                raise ValueError("pivot nao encontrado")

        payload = f"#{normalized_idp}-{normalized_pivot}$"
        sent_ok = bool(sender(normalized_pivot, payload))
        if not sent_ok:
            raise RuntimeError("falha ao pedir configuracao")

        with self._lock:
            pivot = self._ensure_known_pivot_for_command_locked(normalized_pivot, request_ts)
            if pivot is not None:
                state_key, _, normalizer = _config_state_spec(normalized_idp)
                config_state = normalizer(pivot.get(state_key))
                config_state["last_request_ts"] = request_ts
                config_state["last_request_topic"] = normalized_pivot
                config_state["last_request_payload"] = payload
                pivot[state_key] = config_state
                self._record_timeline_locked(
                    pivot,
                    event_type="pivot_config_request",
                    topic=normalized_pivot,
                    ts=request_ts,
                    summary=f"Solicitacao de configuracao IDP {normalized_idp} enviada.",
                    details={"payload": payload, "idp": normalized_idp},
                    source_topic=normalized_pivot,
                    raw_payload=payload,
                )
                self._persist_pivot_snapshot_locked(pivot, request_ts)
                self._dirty = True
                self._invalidate_api_caches_locked()

        self.log.info("Solicitacao #%s-pivot_id$ enviada para pivot_id=%s", normalized_idp, normalized_pivot)
        return {
            "pivot_id": normalized_pivot,
            "topic": normalized_pivot,
            "payload": payload,
            "idp": normalized_idp,
            "request_ts": request_ts,
            "request_at": _ts_to_str(request_ts),
        }

    def _background_loop(self):
        while not self._stop_event.is_set():
            now = time.time()
            self.tick(now)

            if self._dirty and (now - self._last_write_ts) >= self.refresh_sec:
                self.write()

            self._stop_event.wait(1.0)

    def _is_duplicate_locked(self, topic, payload, ts):
        digest = hashlib.sha1(f"{topic}|{payload}".encode("utf-8", errors="ignore")).hexdigest()
        last_ts = self._dedupe_cache.get(digest)
        self._dedupe_cache[digest] = ts

        if last_ts is None:
            return False
        return (ts - last_ts) <= self.dedupe_window_sec

    def _cleanup_dedupe_locked(self, now):
        if len(self._dedupe_cache) < 5000:
            return
        threshold = now - (self.dedupe_window_sec * 4)
        keep = {}
        for digest, ts in self._dedupe_cache.items():
            if ts >= threshold:
                keep[digest] = ts
        self._dedupe_cache = keep

    def _normalize_probe_settings(self, probe_settings):
        normalized = {}
        if not isinstance(probe_settings, dict):
            return normalized

        for raw_pivot_id, raw_setting in probe_settings.items():
            pivot_id = str(raw_pivot_id or "").strip()
            if not pivot_id:
                continue

            if isinstance(raw_setting, dict):
                enabled = bool(raw_setting.get("enabled", False))
                interval_sec = _safe_int(raw_setting.get("interval_sec"), self.probe_default_interval_sec)
            else:
                enabled = bool(raw_setting)
                interval_sec = self.probe_default_interval_sec

            if interval_sec is None:
                interval_sec = self.probe_default_interval_sec
            if interval_sec < self.probe_min_interval_sec:
                interval_sec = self.probe_min_interval_sec

            normalized[pivot_id] = {
                "enabled": enabled,
                "interval_sec": interval_sec,
            }

        return normalized

    def _compute_cloudv2_interval_stats_locked(self, intervals):
        cleaned = []
        if isinstance(intervals, list):
            for item in intervals:
                parsed = _safe_float(item, None)
                if parsed is not None and parsed > 0:
                    cleaned.append(parsed)

        if not cleaned:
            return {
                "median_interval_sec": None,
                "sample_count": 0,
                "total_sample_count": 0,
            }

        buckets = []
        for index, interval_sec in enumerate(cleaned):
            matched_bucket = None
            matched_delta_ratio = None
            for bucket in buckets:
                reference = bucket["reference_sec"]
                if reference <= 0:
                    continue
                tolerance = reference * self.cloudv2_median_tolerance_pct
                delta = abs(interval_sec - reference)
                if delta > tolerance:
                    continue

                delta_ratio = delta / reference
                if matched_bucket is None or delta_ratio < matched_delta_ratio:
                    matched_bucket = bucket
                    matched_delta_ratio = delta_ratio

            if matched_bucket is None:
                buckets.append(
                    {
                        "reference_sec": interval_sec,
                        "samples": [interval_sec],
                        "last_index": index,
                    }
                )
                continue

            matched_bucket["samples"].append(interval_sec)
            matched_bucket["last_index"] = index
            matched_bucket["reference_sec"] = statistics.median(matched_bucket["samples"])

        best_bucket = max(buckets, key=lambda bucket: (len(bucket["samples"]), bucket["last_index"]))
        best_samples = best_bucket["samples"]
        return {
            "median_interval_sec": statistics.median(best_samples),
            "sample_count": len(best_samples),
            "total_sample_count": len(cleaned),
        }

    def _new_pivot_state(
        self,
        pivot_id,
        discovered_ts,
        session_id=None,
        run_id=None,
        is_concentrator=False,
        latitude=None,
        longitude=None,
    ):
        probe_setting = self._probe_settings.get(pivot_id, {})
        probe_enabled = bool(probe_setting.get("enabled", False))
        probe_interval = _safe_int(probe_setting.get("interval_sec"), self.probe_default_interval_sec)
        if probe_interval is None:
            probe_interval = self.probe_default_interval_sec
        if probe_interval < self.probe_min_interval_sec:
            probe_interval = self.probe_min_interval_sec

        return {
            "pivot_id": pivot_id,
            "pivot_slug": slugify(pivot_id),
            "session_id": session_id,
            "run_id": run_id,
            "is_concentrator": bool(is_concentrator),
            "latitude": _safe_float(latitude, None),
            "longitude": _safe_float(longitude, None),
            "discovered_at_ts": discovered_ts,
            "last_seen_ts": discovered_ts,
            "last_ping_ts": None,
            "last_cloudv2_ts": None,
            "last_cloud2_ts": None,
            "topic_counters": {topic: 0 for topic in self.monitor_topics},
            "cloudv2_intervals_sec": [],
            "median_latched_ready": False,
            "median_latched_interval_sec": None,
            "topic_last_ts": {topic: None for topic in CONNECTIVITY_TOPICS},
            "topic_intervals_sec": {topic: [] for topic in CONNECTIVITY_TOPICS},
            "last_cloud2": None,
            "cloud2_events": [],
            "ping_rssi_points": [],
            "drop_events": [],
            "timeline": [],
            "probe": {
                "enabled": probe_enabled,
                "interval_sec": probe_interval,
                "last_sent_ts": None,
                "last_response_ts": None,
                "last_response_info": None,
                "pending_sent_ts": None,
                "pending_deadline_ts": None,
                "timeout_streak": 0,
                "last_result": None,
                "events": [],
            },
            "modem_reset": {
                "last_command_ts": None,
                "last_command_topic": None,
                "last_command_payload": None,
                "last_ack_ts": None,
                "last_ack_topic": None,
                "last_ack_payload": None,
                "last_ack_idp": None,
                "command_count": 0,
                "ack_count": 0,
            },
            "pivot_config": _new_pivot_config_state(),
            "network_config": _new_network_config_state(),
            "rush_config": _new_rush_config_state(),
            "sector_config": _new_sector_config_state(),
            "physical_barrier_config": _new_physical_barrier_config_state(),
            "status_cache": {
                "code": "gray",
                "reason": "Aguardando amostras iniciais de cloudv2.",
                "quality_code": "green",
                "quality_reason": "Qualidade aguardando dados da janela.",
                "changed_at_ts": discovered_ts,
            },
        }

    def _apply_baseline_snapshot_locked(self, pivot, summary, now=None):
        if not isinstance(pivot, dict) or not isinstance(summary, dict):
            return False

        changed = False
        baseline_is_concentrator = summary.get("is_concentrator")
        if baseline_is_concentrator is not None:
            parsed_flag = None
            if isinstance(baseline_is_concentrator, bool):
                parsed_flag = baseline_is_concentrator
            elif isinstance(baseline_is_concentrator, (int, float)):
                parsed_flag = bool(baseline_is_concentrator)
            elif isinstance(baseline_is_concentrator, str):
                normalized_flag = baseline_is_concentrator.strip().lower()
                if normalized_flag in ("1", "true", "yes", "sim"):
                    parsed_flag = True
                elif normalized_flag in ("0", "false", "no", "nao", "não"):
                    parsed_flag = False
            if parsed_flag is not None and bool(pivot.get("is_concentrator")) != bool(parsed_flag):
                pivot["is_concentrator"] = bool(parsed_flag)
                changed = True

        baseline_latitude = _safe_float(summary.get("latitude"), None)
        baseline_longitude = _safe_float(summary.get("longitude"), None)
        if baseline_latitude is not None and _safe_float(pivot.get("latitude"), None) is None:
            pivot["latitude"] = baseline_latitude
            changed = True
        if baseline_longitude is not None and _safe_float(pivot.get("longitude"), None) is None:
            pivot["longitude"] = baseline_longitude
            changed = True

        baseline_last_ping_ts = _safe_float(summary.get("last_ping_ts"), None)
        baseline_last_cloudv2_ts = _safe_float(summary.get("last_cloudv2_ts"), None)
        baseline_last_activity_ts = _safe_float(summary.get("last_activity_ts"), None)

        current_last_ping_ts = _safe_float(pivot.get("last_ping_ts"), None)
        if baseline_last_ping_ts is not None and current_last_ping_ts is None:
            pivot["last_ping_ts"] = baseline_last_ping_ts
            pivot.setdefault("topic_last_ts", {})[TOPIC_PING] = baseline_last_ping_ts
            changed = True

        current_last_cloudv2_ts = _safe_float(pivot.get("last_cloudv2_ts"), None)
        if baseline_last_cloudv2_ts is not None and current_last_cloudv2_ts is None:
            pivot["last_cloudv2_ts"] = baseline_last_cloudv2_ts
            pivot.setdefault("topic_last_ts", {})[TOPIC_CLOUDV2] = baseline_last_cloudv2_ts
            changed = True

        if baseline_last_activity_ts is not None:
            current_last_seen_ts = _safe_float(pivot.get("last_seen_ts"), None)
            if current_last_seen_ts is None or baseline_last_activity_ts > current_last_seen_ts:
                pivot["last_seen_ts"] = baseline_last_activity_ts
                changed = True

        baseline_last_cloud2 = summary.get("last_cloud2")
        current_last_cloud2 = pivot.get("last_cloud2")
        baseline_last_cloud2_ts = None
        if (not isinstance(current_last_cloud2, dict) or not current_last_cloud2) and isinstance(
            baseline_last_cloud2, dict
        ) and baseline_last_cloud2:
            pivot["last_cloud2"] = dict(baseline_last_cloud2)
            baseline_last_cloud2_ts = _safe_float(baseline_last_cloud2.get("ts"), None)
            if baseline_last_cloud2_ts is not None:
                pivot["last_cloud2_ts"] = baseline_last_cloud2_ts
            changed = True

        baseline_pivot_config = summary.get("pivot_config")
        if isinstance(baseline_pivot_config, dict) and baseline_pivot_config:
            pivot["pivot_config"] = _normalize_pivot_config_state(baseline_pivot_config)
            changed = True

        baseline_network_config = summary.get("network_config")
        if isinstance(baseline_network_config, dict) and baseline_network_config:
            pivot["network_config"] = _normalize_network_config_state(baseline_network_config)
            changed = True

        baseline_rush_config = summary.get("rush_config")
        if isinstance(baseline_rush_config, dict) and baseline_rush_config:
            pivot["rush_config"] = _normalize_rush_config_state(baseline_rush_config)
            changed = True

        baseline_sector_config = summary.get("sector_config")
        if isinstance(baseline_sector_config, dict) and baseline_sector_config:
            pivot["sector_config"] = _normalize_sector_config_state(baseline_sector_config)
            changed = True

        baseline_physical_barrier_config = summary.get("physical_barrier_config")
        if isinstance(baseline_physical_barrier_config, dict) and baseline_physical_barrier_config:
            pivot["physical_barrier_config"] = _normalize_physical_barrier_config_state(baseline_physical_barrier_config)
            changed = True

        probe = pivot.get("probe")
        if isinstance(probe, dict) and not isinstance(probe.get("last_response_info"), dict):
            baseline_probe = summary.get("probe")
            baseline_probe_info = None
            if isinstance(baseline_probe, dict):
                baseline_probe_info = baseline_probe.get("last_response_info")
            normalized_probe_info = _normalize_probe_response_info(baseline_probe_info)
            if normalized_probe_info is not None:
                probe["last_response_info"] = normalized_probe_info
                changed = True

        current_intervals = (
            pivot.get("topic_intervals_sec", {}).get(TOPIC_CLOUDV2)
            if isinstance(pivot.get("topic_intervals_sec", {}).get(TOPIC_CLOUDV2), list)
            else pivot.get("cloudv2_intervals_sec", [])
        ) or []
        has_current_intervals = any((_safe_float(item, None) or 0) > 0 for item in current_intervals)

        median_interval = _safe_float(summary.get("median_cloudv2_interval_sec"), None)
        baseline_sample_count = _safe_int(summary.get("median_sample_count"), None)
        median_ready = bool(summary.get("median_ready"))
        if (
            not has_current_intervals
            and
            median_interval is not None
            and median_interval > 0
            and baseline_sample_count is not None
            and baseline_sample_count > 0
            and median_ready
        ):
            seed_size = min(
                self.cloudv2_window,
                max(self.cloudv2_min_samples, int(baseline_sample_count)),
            )
            seeded = [median_interval for _ in range(seed_size)]
            topic_intervals = pivot.setdefault("topic_intervals_sec", {})
            topic_intervals[TOPIC_CLOUDV2] = list(seeded)
            pivot["cloudv2_intervals_sec"] = list(seeded)
            pivot["median_latched_ready"] = True
            pivot["median_latched_interval_sec"] = median_interval
            changed = True

        status_info = summary.get("status")
        quality_info = summary.get("quality")
        if isinstance(status_info, dict) and isinstance(quality_info, dict):
            current_status_cache = pivot.get("status_cache") if isinstance(pivot.get("status_cache"), dict) else {}
            current_status_code = str(current_status_cache.get("code") or "").strip().lower()
            status_code = str(status_info.get("code") or "").strip()
            quality_code = str(quality_info.get("code") or "").strip()
            if (
                status_code in STATUS_LABELS
                and quality_code in QUALITY_LABELS
                and current_status_code in ("", "gray")
            ):
                pivot["status_cache"] = {
                    "code": status_code,
                    "reason": str(status_info.get("reason") or ""),
                    "quality_code": quality_code,
                    "quality_reason": str(quality_info.get("reason") or ""),
                    "changed_at_ts": float(now if now is not None else time.time()),
                }
                changed = True

        return changed

    def _load_pivot_baseline_from_persistence_locked(self, pivot_id):
        normalized_pivot_id = str(pivot_id or "").strip()
        if not normalized_pivot_id:
            return None

        try:
            sessions = self.persistence.list_sessions(normalized_pivot_id, limit=500, run_id=None)
        except RuntimeError:
            sessions = []

        best_summary = None
        best_rank = None
        for session in sessions:
            session_id = str((session or {}).get("session_id") or "").strip()
            if not session_id:
                continue
            try:
                has_snapshot = self.persistence.has_snapshot(normalized_pivot_id, session_id)
            except RuntimeError:
                has_snapshot = False
            if not has_snapshot:
                continue
            try:
                payload = self.persistence.get_panel_payload(
                    normalized_pivot_id,
                    session_id=session_id,
                    run_id=None,
                )
            except RuntimeError:
                payload = None
            if not isinstance(payload, dict):
                continue
            summary = payload.get("summary")
            if isinstance(summary, dict):
                sample_count = _safe_int(summary.get("median_sample_count"), 0)
                if sample_count is None or sample_count < 0:
                    sample_count = 0
                median_ready = bool(summary.get("median_ready"))
                status_code = str(((summary.get("status") or {}).get("code") or "")).strip().lower()
                summary_last_activity_ts = _safe_float(summary.get("last_activity_ts"), None)
                session_updated_ts = _safe_float((session or {}).get("updated_at_ts"), None)
                rank = (
                    1 if median_ready else 0,
                    1 if status_code in ("green", "red") else 0,
                    int(sample_count),
                    summary_last_activity_ts if summary_last_activity_ts is not None else -1.0,
                    session_updated_ts if session_updated_ts is not None else -1.0,
                )
                if best_rank is None or rank > best_rank:
                    best_rank = rank
                    best_summary = summary

        if isinstance(best_summary, dict):
            return best_summary

        try:
            payload = self.persistence.get_panel_payload(normalized_pivot_id)
        except RuntimeError:
            return None
        if not isinstance(payload, dict):
            return None
        summary = payload.get("summary")
        if not isinstance(summary, dict):
            return None
        return summary

    def _load_pivot_concentrator_flag_locked(self, pivot_id):
        normalized_pivot_id = str(pivot_id or "").strip()
        if not normalized_pivot_id:
            return False
        try:
            return bool(self.persistence.get_pivot_is_concentrator(normalized_pivot_id))
        except RuntimeError:
            return False

    def _load_pivot_coordinates_locked(self, pivot_id):
        normalized_pivot_id = str(pivot_id or "").strip()
        if not normalized_pivot_id:
            return {"latitude": None, "longitude": None}
        try:
            coordinates = self.persistence.get_pivot_coordinates(normalized_pivot_id)
        except RuntimeError:
            coordinates = None
        if not isinstance(coordinates, dict):
            return {"latitude": None, "longitude": None}
        return {
            "latitude": _safe_float(coordinates.get("latitude"), None),
            "longitude": _safe_float(coordinates.get("longitude"), None),
        }

    def _get_or_create_pivot_locked(self, pivot_id, ts):
        pivot = self.pivots.get(pivot_id)
        if pivot is not None:
            if not pivot.get("session_id"):
                pivot["session_id"] = self._ensure_active_session_locked(pivot_id, ts, source="runtime")
            if not pivot.get("run_id"):
                pivot["run_id"] = self._active_run_id
            if "is_concentrator" not in pivot:
                pivot["is_concentrator"] = self._load_pivot_concentrator_flag_locked(pivot_id)
            missing_latitude = "latitude" not in pivot or _safe_float(pivot.get("latitude"), None) is None
            missing_longitude = "longitude" not in pivot or _safe_float(pivot.get("longitude"), None) is None
            if missing_latitude or missing_longitude:
                coordinates = self._load_pivot_coordinates_locked(pivot_id)
                if missing_latitude:
                    pivot["latitude"] = coordinates["latitude"]
                if missing_longitude:
                    pivot["longitude"] = coordinates["longitude"]
            return pivot

        session_id = self._ensure_active_session_locked(pivot_id, ts, source="cloudv2_discovery")
        is_concentrator = self._load_pivot_concentrator_flag_locked(pivot_id)
        coordinates = self._load_pivot_coordinates_locked(pivot_id)
        pivot = self._new_pivot_state(
            pivot_id,
            ts,
            session_id=session_id,
            run_id=self._active_run_id,
            is_concentrator=is_concentrator,
            latitude=coordinates["latitude"],
            longitude=coordinates["longitude"],
        )
        baseline_summary = self._load_pivot_baseline_from_persistence_locked(pivot_id)
        if isinstance(baseline_summary, dict):
            self._apply_baseline_snapshot_locked(pivot, baseline_summary, now=ts)
        self.pivots[pivot_id] = pivot
        self.persistence.ensure_pivot(pivot_id, pivot_slug=pivot["pivot_slug"], seen_ts=ts)

        self._record_timeline_locked(
            pivot,
            event_type="pivot_discovered",
            topic=TOPIC_CLOUDV2,
            ts=ts,
            summary="Pivot descoberto via cloudv2 apos autorizacao.",
            details={"pivot_id": pivot_id},
        )
        self.log.info("Pivot autorizado descoberto via cloudv2: pivot_id=%s", pivot_id)
        return pivot

    def _record_message_common_locked(self, pivot, topic, ts):
        pivot["last_seen_ts"] = ts
        pivot_id = pivot.get("pivot_id")
        if pivot_id:
            self.persistence.touch_pivot_seen(pivot_id, ts)
        counters = pivot["topic_counters"]
        counters[topic] = counters.get(topic, 0) + 1

        if topic in CONNECTIVITY_TOPICS:
            topic_last = pivot.setdefault("topic_last_ts", {item: None for item in CONNECTIVITY_TOPICS})
            topic_intervals = pivot.setdefault("topic_intervals_sec", {item: [] for item in CONNECTIVITY_TOPICS})

            previous_ts = _safe_float(topic_last.get(topic), None)
            if previous_ts is not None and ts > previous_ts:
                intervals = topic_intervals.setdefault(topic, [])
                intervals.append(ts - previous_ts)
                if len(intervals) > self.cloudv2_window:
                    topic_intervals[topic] = intervals[-self.cloudv2_window :]
            topic_last[topic] = ts

    def _record_cloudv2_locked(self, pivot, parsed, topic, ts, raw_payload=None):
        intervals = (
            pivot.get("topic_intervals_sec", {}).get(TOPIC_CLOUDV2)
            or pivot.get("cloudv2_intervals_sec", [])
        )
        interval_sec = intervals[-1] if intervals else None
        if intervals:
            pivot["cloudv2_intervals_sec"] = intervals[-self.cloudv2_window :]
            interval_stats = self._compute_cloudv2_interval_stats_locked(pivot["cloudv2_intervals_sec"])
            median_value = interval_stats["median_interval_sec"]
            if median_value is not None:
                self.log.debug(
                    "Mediana cloudv2 atualizada: pivot_id=%s mediana=%.2fs amostras_validas=%s total=%s",
                    pivot["pivot_id"],
                    median_value,
                    interval_stats["sample_count"],
                    interval_stats["total_sample_count"],
                )

        pivot["last_cloudv2_ts"] = ts

        details = {
            "idp": parsed.get("idp"),
            "field_count": len(parsed.get("parts", [])),
        }
        if interval_sec is not None:
            details["interval_sec"] = interval_sec

        self._record_timeline_locked(
            pivot,
            event_type="cloudv2",
            topic=topic,
            ts=ts,
            summary="Pacote cloudv2 recebido.",
            details=details,
            source_topic=topic,
            raw_payload=raw_payload,
            parsed_payload=parsed,
        )

    def _record_ping_locked(self, pivot, parsed, topic, ts, raw_payload=None):
        pivot["last_ping_ts"] = ts
        rssi_value = parse_ping_rssi(parsed)
        if rssi_value is not None:
            rssi_point = {
                "ts": ts,
                "at": _ts_to_str(ts),
                "rssi": rssi_value,
            }
            self.persistence.insert_ping_rssi_point(
                pivot.get("pivot_id"),
                pivot.get("session_id"),
                ts=ts,
                rssi=rssi_value,
            )
            pivot["ping_rssi_points"].append(rssi_point)
            if len(pivot["ping_rssi_points"]) > self.max_events_per_pivot:
                pivot["ping_rssi_points"] = pivot["ping_rssi_points"][-self.max_events_per_pivot :]

        details = {"idp": parsed.get("idp")}
        if rssi_value is not None:
            details["rssi"] = rssi_value
        self._record_timeline_locked(
            pivot,
            event_type="ping",
            topic=topic,
            ts=ts,
            summary="Ping passivo recebido em cloudv2-ping.",
            details=details,
            source_topic=topic,
            raw_payload=raw_payload,
            parsed_payload=parsed,
        )

    def _record_cloud2_locked(self, pivot, parsed, topic, ts, raw_payload=None):
        parts = parsed.get("parts", [])

        # cloud2 pode trazer RSSI negativo no formato "...-<pivot>--67-..."
        # e data final com hifens; por isso parseamos o "tail" com cuidado.
        rssi = None
        technology = None
        drop_duration_raw = None
        firmware = None
        event_date = None

        payload_source = raw_payload if raw_payload is not None else parsed.get("raw", "")
        core = payload_source[1:-1] if payload_source.startswith("#") and payload_source.endswith("$") else ""
        core_parts = core.split("-", 2)
        tail = core_parts[2] if len(core_parts) >= 3 else ""
        tail_tokens = tail.split("-") if tail else []

        idx = 0
        if tail_tokens:
            first = tail_tokens[0]
            if first == "" and len(tail_tokens) > 1 and re.fullmatch(r"\d+", tail_tokens[1] or ""):
                rssi = f"-{tail_tokens[1]}"
                idx = 2
            elif re.fullmatch(r"-?\d+", first or ""):
                rssi = first
                idx = 1

        if len(tail_tokens) > idx:
            technology = tail_tokens[idx] or None
        if len(tail_tokens) > idx + 1:
            drop_duration_raw = tail_tokens[idx + 1] or None
        if len(tail_tokens) > idx + 2:
            firmware = tail_tokens[idx + 2] or None
        if len(tail_tokens) > idx + 3:
            event_date = "-".join(tail_tokens[idx + 3 :]) or None

        # Fallback para formatos legados/curtos.
        if rssi is None and len(parts) > 2:
            rssi = parts[2] or None
        if technology is None and len(parts) > 3:
            technology = parts[3] or None
        if drop_duration_raw is None and len(parts) > 4:
            drop_duration_raw = parts[4] or None
        if firmware is None and len(parts) > 5:
            firmware = parts[5] or None
        if event_date is None and len(parts) > 6:
            event_date = "-".join(parts[6:]) or None

        drop_duration_sec = _parse_duration_seconds(drop_duration_raw)

        cloud2_event = {
            "ts": ts,
            "at": _ts_to_str(ts),
            "idp": parsed.get("idp"),
            "rssi": rssi,
            "technology": technology,
            "drop_duration_raw": drop_duration_raw,
            "drop_duration_sec": drop_duration_sec,
            "firmware": firmware,
            "event_date": event_date,
            "raw": parsed.get("raw"),
        }
        self.persistence.insert_cloud2_event(pivot.get("pivot_id"), pivot.get("session_id"), cloud2_event)

        pivot["last_cloud2_ts"] = ts
        pivot["last_cloud2"] = cloud2_event
        pivot["cloud2_events"].append(cloud2_event)
        if len(pivot["cloud2_events"]) > self.max_events_per_pivot:
            pivot["cloud2_events"] = pivot["cloud2_events"][-self.max_events_per_pivot :]

        drop_event = None
        if drop_duration_sec is not None and drop_duration_sec > 0:
            drop_event = {
                "ts": ts,
                "at": _ts_to_str(ts),
                "duration_sec": drop_duration_sec,
                "technology": technology,
                "rssi": rssi,
            }
            self.persistence.insert_drop_event(pivot.get("pivot_id"), pivot.get("session_id"), drop_event)
            pivot["drop_events"].append(drop_event)
            if len(pivot["drop_events"]) > self.max_events_per_pivot:
                pivot["drop_events"] = pivot["drop_events"][-self.max_events_per_pivot :]

        self._record_timeline_locked(
            pivot,
            event_type="cloud2",
            topic=topic,
            ts=ts,
            summary="Evento cloud2 (conexao/reconexao) recebido.",
            details={
                "rssi": rssi,
                "technology": technology,
                "drop_duration_raw": drop_duration_raw,
                "drop_duration_sec": drop_duration_sec,
                "firmware": firmware,
                "event_date": event_date,
            },
            source_topic=topic,
            raw_payload=payload_source,
            parsed_payload=parsed,
        )

    def _record_generic_topic_locked(self, pivot, parsed, topic, ts, raw_payload=None):
        normalized_topic = str(topic or "").strip()
        event_type = re.sub(r"[^a-zA-Z0-9_]+", "_", normalized_topic).strip("_") or "topic_event"
        self._record_timeline_locked(
            pivot,
            event_type=event_type,
            topic=normalized_topic,
            ts=ts,
            summary=f"Mensagem recebida em {normalized_topic}.",
            details={
                "idp": parsed.get("idp"),
                "field_count": len(parsed.get("parts", [])),
            },
            source_topic=normalized_topic,
            raw_payload=raw_payload,
            parsed_payload=parsed,
        )

    def _record_pivot_config_locked(self, pivot, parsed, topic, ts, raw_payload=None):
        normalized_topic = str(topic or "").strip()
        raw_idp = str((parsed or {}).get("idp") or "").strip().zfill(2)
        state_key, parser, normalizer = _config_state_spec(raw_idp)
        if not state_key or parser is None or normalizer is None:
            self._record_generic_topic_locked(pivot, parsed, normalized_topic, ts, raw_payload=raw_payload)
            return False
        parsed_config = parser(parsed)
        config_state = normalizer(pivot.get(state_key))
        pivot[state_key] = config_state

        if parsed_config is None:
            self._record_generic_topic_locked(pivot, parsed, normalized_topic, ts, raw_payload=raw_payload)
            return False

        last_request_ts = _safe_float(config_state.get("last_request_ts"), None)
        last_response_ts = _safe_float(config_state.get("last_response_ts"), None)
        request_pending = last_request_ts is not None and (
            last_response_ts is None or last_response_ts < last_request_ts
        )
        if not request_pending or ts < last_request_ts:
            self._record_timeline_locked(
                pivot,
                event_type="pivot_config_unrequested",
                topic=normalized_topic,
                ts=ts,
                summary=f"Configuracao IDP {raw_idp} recebida sem solicitacao ativa.",
                details={
                    "idp": raw_idp,
                    "field_count": len(parsed.get("parts", [])),
                    "ignored_for_modal": True,
                },
                source_topic=normalized_topic,
                raw_payload=raw_payload,
                parsed_payload=parsed,
            )
            return False

        config_state.update(parsed_config)
        config_state["last_response_ts"] = ts
        config_state["last_response_topic"] = normalized_topic
        config_state["last_response_payload"] = str(raw_payload or parsed.get("raw") or "").strip() or None
        pivot[state_key] = config_state

        self._record_timeline_locked(
            pivot,
            event_type="pivot_config_response",
            topic=normalized_topic,
            ts=ts,
            summary=f"Configuracao IDP {raw_idp} recebida.",
            details={
                "idp": raw_idp,
                "field_count": len(parsed.get("parts", [])),
                "config": dict(parsed_config),
            },
            source_topic=normalized_topic,
            raw_payload=raw_payload,
            parsed_payload=parsed,
        )
        return True

    def _record_probe_response_locked(self, pivot, parsed, topic, ts, raw_payload=None):
        probe = pivot["probe"]
        pending_sent_ts = probe.get("pending_sent_ts")
        pending_deadline_ts = probe.get("pending_deadline_ts")
        probe_response_info = parse_probe_status_payload(parsed)
        if probe_response_info is not None:
            probe["last_response_info"] = probe_response_info

        if (
            pending_sent_ts is not None
            and pending_deadline_ts is not None
            and pending_sent_ts <= ts <= pending_deadline_ts
        ):
            latency = ts - pending_sent_ts
            probe["pending_sent_ts"] = None
            probe["pending_deadline_ts"] = None
            probe["last_response_ts"] = ts
            probe["timeout_streak"] = 0
            probe["last_result"] = "response"
            response_event = {
                "type": "response",
                "ts": ts,
                "at": _ts_to_str(ts),
                "topic": topic,
                "latency_sec": latency,
            }
            if raw_payload is not None:
                response_event["payload"] = str(raw_payload)
            if probe_response_info is not None:
                response_event["details"] = {
                    "response_info": probe_response_info,
                }
            probe["events"].append(response_event)
            if len(probe["events"]) > self.max_events_per_pivot:
                probe["events"] = probe["events"][-self.max_events_per_pivot :]
            self.persistence.insert_probe_event(pivot.get("pivot_id"), pivot.get("session_id"), response_event)

            probe_stats = self._summarize_probe_stats_locked(probe)
            self.persistence.insert_probe_delay_point(
                pivot.get("pivot_id"),
                pivot.get("session_id"),
                ts=ts,
                latency_sec=latency,
                avg_latency_sec=probe_stats.get("latency_avg_sec"),
                median_latency_sec=probe_stats.get("latency_median_sec"),
                sample_count=probe_stats.get("latency_sample_count"),
            )

            self._record_timeline_locked(
                pivot,
                event_type="probe_response",
                topic=topic,
                ts=ts,
                summary="Resposta de probe #11$ recebida dentro da janela esperada.",
                details={
                    "latency_sec": latency,
                    "source_topic": topic,
                    "response_info": probe_response_info,
                },
                source_topic=topic,
                raw_payload=raw_payload,
                parsed_payload=parsed,
            )
            self.log.info(
                "Probe respondido: pivot_id=%s topic=%s latency=%.2fs",
                pivot["pivot_id"],
                topic,
                latency,
            )
            return

        unmatched_event = {
            "type": "response_unmatched",
            "ts": ts,
            "at": _ts_to_str(ts),
            "topic": topic,
        }
        if raw_payload is not None:
            unmatched_event["payload"] = str(raw_payload)
        if probe_response_info is not None:
            unmatched_event["details"] = {
                "response_info": probe_response_info,
            }
        self.persistence.insert_probe_event(pivot.get("pivot_id"), pivot.get("session_id"), unmatched_event)
        self._record_timeline_locked(
            pivot,
            event_type="probe_response_unmatched",
            topic=topic,
            ts=ts,
            summary="Resposta recebida sem probe pendente na janela temporal.",
            details={
                "source_topic": topic,
                "response_info": probe_response_info,
            },
            source_topic=topic,
            raw_payload=raw_payload,
            parsed_payload=parsed,
        )

    def _probe_should_send_locked(self, pivot, now):
        probe = pivot["probe"]
        if not probe.get("enabled"):
            return False

        if probe.get("pending_sent_ts") is not None:
            return False

        interval_sec = _safe_int(probe.get("interval_sec"), self.probe_default_interval_sec)
        if interval_sec is None:
            interval_sec = self.probe_default_interval_sec
        if interval_sec < self.probe_min_interval_sec:
            interval_sec = self.probe_min_interval_sec
        probe["interval_sec"] = interval_sec

        last_sent_ts = probe.get("last_sent_ts")
        if last_sent_ts is None:
            return True

        return (now - last_sent_ts) >= interval_sec

    def _record_probe_sent_locked(self, pivot, ts):
        probe = pivot["probe"]
        interval_sec = _safe_int(probe.get("interval_sec"), self.probe_default_interval_sec)
        if interval_sec is None:
            interval_sec = self.probe_default_interval_sec
        if interval_sec < self.probe_min_interval_sec:
            interval_sec = self.probe_min_interval_sec

        deadline_ts = ts + (interval_sec * self.probe_timeout_factor)
        probe["last_sent_ts"] = ts
        probe["pending_sent_ts"] = ts
        probe["pending_deadline_ts"] = deadline_ts
        probe["last_result"] = "sent"

        sent_event = {
            "type": "sent",
            "ts": ts,
            "at": _ts_to_str(ts),
            "topic": pivot["pivot_id"],
            "payload": "#11$",
            "deadline_ts": deadline_ts,
            "deadline_at": _ts_to_str(deadline_ts),
        }
        probe["events"].append(sent_event)
        if len(probe["events"]) > self.max_events_per_pivot:
            probe["events"] = probe["events"][-self.max_events_per_pivot :]
        self.persistence.insert_probe_event(pivot.get("pivot_id"), pivot.get("session_id"), sent_event)

        self._record_timeline_locked(
            pivot,
            event_type="probe_sent",
            topic=pivot["pivot_id"],
            ts=ts,
            summary="Probe #11$ enviado no topico dinamico do pivot.",
            details={
                "payload": "#11$",
                "deadline_ts": deadline_ts,
                "deadline_at": _ts_to_str(deadline_ts),
            },
            source_topic=pivot["pivot_id"],
            raw_payload="#11$",
        )
        self.log.info("Probe #11$ enviado: pivot_id=%s", pivot["pivot_id"])

    def _check_probe_timeout_locked(self, pivot, now):
        probe = pivot["probe"]
        pending_sent_ts = probe.get("pending_sent_ts")
        pending_deadline_ts = probe.get("pending_deadline_ts")
        if pending_sent_ts is None or pending_deadline_ts is None:
            return False

        if now <= pending_deadline_ts:
            return False

        probe["pending_sent_ts"] = None
        probe["pending_deadline_ts"] = None
        probe["timeout_streak"] = int(probe.get("timeout_streak", 0)) + 1
        probe["last_result"] = "timeout"

        timeout_event = {
            "type": "timeout",
            "ts": now,
            "at": _ts_to_str(now),
            "sent_ts": pending_sent_ts,
            "sent_at": _ts_to_str(pending_sent_ts),
        }
        probe["events"].append(timeout_event)
        if len(probe["events"]) > self.max_events_per_pivot:
            probe["events"] = probe["events"][-self.max_events_per_pivot :]
        self.persistence.insert_probe_event(pivot.get("pivot_id"), pivot.get("session_id"), timeout_event)

        self._record_timeline_locked(
            pivot,
            event_type="probe_timeout",
            topic=pivot["pivot_id"],
            ts=now,
            summary="Probe #11$ sem resposta dentro da janela esperada.",
            details={
                "sent_ts": pending_sent_ts,
                "deadline_ts": pending_deadline_ts,
            },
            source_topic=pivot["pivot_id"],
            raw_payload="#11$",
        )

        self.log.warning(
            "Probe com timeout: pivot_id=%s streak=%s",
            pivot["pivot_id"],
            probe["timeout_streak"],
        )
        return True

    def _record_timeline_locked(
        self,
        pivot,
        event_type,
        topic,
        ts,
        summary,
        details=None,
        source_topic=None,
        raw_payload=None,
        parsed_payload=None,
    ):
        event_details = dict(details or {})
        normalized_source_topic = str(source_topic or "").strip()
        if normalized_source_topic and "source_topic" not in event_details:
            event_details["source_topic"] = normalized_source_topic
        if raw_payload not in (None, "") and "raw_payload" not in event_details:
            event_details["raw_payload"] = str(raw_payload)
        if isinstance(parsed_payload, dict) and parsed_payload and "parsed_payload" not in event_details:
            event_details["parsed_payload"] = parsed_payload

        self._event_seq += 1
        event = {
            "id": self._event_seq,
            "ts": ts,
            "at": _ts_to_str(ts),
            "type": event_type,
            "topic": topic,
            "summary": summary,
            "details": event_details,
        }
        pivot["timeline"].append(event)
        if len(pivot["timeline"]) > self.max_events_per_pivot:
            pivot["timeline"] = pivot["timeline"][-self.max_events_per_pivot :]

        self.persistence.insert_connectivity_event(
            pivot.get("pivot_id"),
            pivot.get("session_id"),
            event,
            source_topic=source_topic,
            raw_payload=raw_payload,
            parsed_payload=parsed_payload,
        )

    def _record_malformed_locked(self, topic, payload, reason, ts):
        excerpt = payload[:240]
        info = {
            "ts": ts,
            "at": _ts_to_str(ts),
            "topic": topic,
            "reason": reason,
            "payload_excerpt": excerpt,
        }
        self.malformed_messages.append(info)
        if len(self.malformed_messages) > 500:
            self.malformed_messages = self.malformed_messages[-500:]

        self.log.warning(
            "Payload malformado descartado: topic=%s reason=%s payload=%s",
            topic,
            reason,
            excerpt,
        )

    def _record_pending_ping_locked(self, pivot_id, ts, payload):
        entry = self.pending_ping_unknown.get(pivot_id)
        if entry is None:
            entry = {
                "pivot_id": pivot_id,
                "first_seen_ts": ts,
                "last_seen_ts": ts,
                "count": 0,
                "last_payload_excerpt": "",
            }
            self.pending_ping_unknown[pivot_id] = entry

        entry["count"] += 1
        entry["last_seen_ts"] = ts
        entry["last_payload_excerpt"] = payload[:160]

        self.log.info(
            "Ping recebido para pivot ainda nao descoberto via cloudv2: pivot_id=%s count=%s",
            pivot_id,
            entry["count"],
        )

    def _cleanup_pending_ping_locked(self, now):
        cutoff = now - self.retention_sec
        keep = {}
        for pivot_id, entry in self.pending_ping_unknown.items():
            last_seen_ts = _safe_float(entry.get("last_seen_ts"), 0)
            if last_seen_ts >= cutoff:
                keep[pivot_id] = entry
        self.pending_ping_unknown = keep

    def _prune_pivot_locked(self, pivot, now):
        cutoff = now - self.retention_sec
        changed = False

        timeline = [event for event in pivot["timeline"] if _safe_float(event.get("ts"), 0) >= cutoff]
        if len(timeline) != len(pivot["timeline"]):
            changed = True
        pivot["timeline"] = timeline

        cloud2_events = [event for event in pivot["cloud2_events"] if _safe_float(event.get("ts"), 0) >= cutoff]
        if len(cloud2_events) != len(pivot["cloud2_events"]):
            changed = True
        pivot["cloud2_events"] = cloud2_events

        ping_rssi_points = [event for event in pivot["ping_rssi_points"] if _safe_float(event.get("ts"), 0) >= cutoff]
        if len(ping_rssi_points) != len(pivot["ping_rssi_points"]):
            changed = True
        pivot["ping_rssi_points"] = ping_rssi_points

        drop_events = [event for event in pivot["drop_events"] if _safe_float(event.get("ts"), 0) >= cutoff]
        if len(drop_events) != len(pivot["drop_events"]):
            changed = True
        pivot["drop_events"] = drop_events

        probe = pivot["probe"]
        probe_events = [event for event in probe["events"] if _safe_float(event.get("ts"), 0) >= cutoff]
        if len(probe_events) != len(probe["events"]):
            changed = True
        probe["events"] = probe_events
        return changed

    def _compute_disconnected_pct_locked(self, pivot, now, disconnect_threshold_sec):
        if disconnect_threshold_sec is None or disconnect_threshold_sec <= 0:
            return None

        window_sec = min(self.attention_disconnected_window_sec, self.retention_sec)
        if window_sec <= 0:
            return None

        start_ts = now - window_sec
        min_relevant_ts = start_ts - disconnect_threshold_sec

        message_ts = []
        for event in pivot.get("timeline", []):
            topic = str(event.get("topic") or "")
            if topic not in CONNECTIVITY_TOPICS:
                continue
            ts = _safe_float(event.get("ts"), None)
            if ts is None:
                continue
            if ts > now or ts < min_relevant_ts:
                continue
            message_ts.append(ts)

        if not message_ts:
            return 100.0

        message_ts = sorted(set(message_ts))

        connected_sec = 0.0
        current_start = None
        current_end = None

        for ts in message_ts:
            seg_start = max(start_ts, ts)
            seg_end = min(now, ts + disconnect_threshold_sec)
            if seg_end <= seg_start:
                continue

            if current_start is None:
                current_start = seg_start
                current_end = seg_end
                continue

            if seg_start <= current_end:
                if seg_end > current_end:
                    current_end = seg_end
                continue

            connected_sec += current_end - current_start
            current_start = seg_start
            current_end = seg_end

        if current_start is not None:
            connected_sec += current_end - current_start

        connected_sec = max(0.0, min(float(window_sec), connected_sec))
        disconnected_sec = max(0.0, float(window_sec) - connected_sec)
        return (disconnected_sec / float(window_sec)) * 100.0

    def _build_timeline_mini_segments_locked(self, pivot, now, disconnect_threshold_sec, window_sec):
        threshold_sec = _safe_float(disconnect_threshold_sec, None)
        safe_window_sec = _safe_float(window_sec, TIMELINE_MINI_WINDOW_SEC)
        if threshold_sec is None or threshold_sec <= 0 or safe_window_sec is None or safe_window_sec <= 0:
            return []

        end_ts = _safe_float(now, None)
        if end_ts is None:
            return []

        min_timeline_ts = None
        for event in pivot.get("timeline", []):
            if not isinstance(event, dict):
                continue
            event_ts = _safe_float(event.get("ts"), None)
            if event_ts is None or event_ts <= 0 or event_ts > end_ts:
                continue
            if min_timeline_ts is None or event_ts < min_timeline_ts:
                min_timeline_ts = event_ts

        if min_timeline_ts is None:
            min_timeline_ts = max(0.0, end_ts - TIMELINE_MINI_EMPTY_FALLBACK_SEC)

        start_ts = end_ts - safe_window_sec
        if start_ts < min_timeline_ts:
            start_ts = min_timeline_ts
        if start_ts >= end_ts:
            start_ts = max(min_timeline_ts, end_ts - TIMELINE_MINI_EMPTY_FALLBACK_SEC)
        if start_ts >= end_ts:
            return []

        min_relevant_ts = start_ts - threshold_sec
        message_ts = []
        for event in pivot.get("timeline", []):
            if not isinstance(event, dict):
                continue
            topic = str(event.get("topic") or "")
            if topic not in CONNECTIVITY_TOPICS:
                continue
            ts = _safe_float(event.get("ts"), None)
            if ts is None:
                continue
            if ts < min_relevant_ts or ts > end_ts:
                continue
            message_ts.append(ts)

        total_duration = end_ts - start_ts
        if total_duration <= 0:
            return []

        if not message_ts:
            return [{"state": "offline", "ratio": 1.0}]

        message_ts = sorted(set(message_ts))
        online_intervals = []
        current_start = None
        current_end = None
        for ts in message_ts:
            interval_start = max(start_ts, ts)
            interval_end = min(end_ts, ts + threshold_sec)
            if interval_end <= interval_start:
                continue
            if current_start is None:
                current_start = interval_start
                current_end = interval_end
                continue
            if interval_start <= current_end:
                current_end = max(current_end, interval_end)
                continue
            online_intervals.append((current_start, current_end))
            current_start = interval_start
            current_end = interval_end
        if current_start is not None:
            online_intervals.append((current_start, current_end))

        if not online_intervals:
            return [{"state": "offline", "ratio": 1.0}]

        raw_segments = []
        cursor = start_ts
        for interval_start, interval_end in online_intervals:
            if interval_start > cursor:
                raw_segments.append({"state": "offline", "start": cursor, "end": interval_start})
            raw_segments.append({"state": "online", "start": interval_start, "end": interval_end})
            cursor = interval_end
            if cursor >= end_ts:
                break
        if cursor < end_ts:
            raw_segments.append({"state": "offline", "start": cursor, "end": end_ts})
        raw_segments = [segment for segment in raw_segments if segment["end"] > segment["start"]]
        if not raw_segments:
            return [{"state": "offline", "ratio": 1.0}]

        def build_ratio_segments(segments):
            ratio_segments = []
            for segment in segments:
                ratio = (segment["end"] - segment["start"]) / total_duration
                if ratio <= 0:
                    continue
                state = segment["state"]
                if ratio_segments and ratio_segments[-1]["state"] == state:
                    ratio_segments[-1]["ratio"] += ratio
                else:
                    ratio_segments.append({"state": state, "ratio": ratio})
            if not ratio_segments:
                return []
            total_ratio = sum(item["ratio"] for item in ratio_segments)
            if total_ratio <= 0:
                return []
            for item in ratio_segments:
                item["ratio"] = round(item["ratio"] / total_ratio, 6)
            return ratio_segments

        if len(raw_segments) <= TIMELINE_MINI_BINS:
            direct = build_ratio_segments(raw_segments)
            if direct:
                return direct
            return [{"state": "offline", "ratio": 1.0}]

        bin_size = total_duration / float(TIMELINE_MINI_BINS)
        states = []
        segment_idx = 0
        for bin_idx in range(TIMELINE_MINI_BINS):
            bin_start = start_ts + (bin_idx * bin_size)
            bin_end = bin_start + bin_size
            online_overlap = 0.0
            offline_overlap = 0.0

            while segment_idx < len(raw_segments) and raw_segments[segment_idx]["end"] <= bin_start:
                segment_idx += 1

            probe_idx = segment_idx
            while probe_idx < len(raw_segments):
                segment = raw_segments[probe_idx]
                if segment["start"] >= bin_end:
                    break
                overlap_start = max(bin_start, segment["start"])
                overlap_end = min(bin_end, segment["end"])
                overlap = overlap_end - overlap_start
                if overlap > 0:
                    if segment["state"] == "online":
                        online_overlap += overlap
                    else:
                        offline_overlap += overlap
                probe_idx += 1

            states.append("online" if online_overlap >= offline_overlap else "offline")

        compressed = []
        current_state = states[0]
        current_count = 1
        for state_value in states[1:]:
            if state_value == current_state:
                current_count += 1
                continue
            compressed.append({"state": current_state, "ratio": current_count / float(TIMELINE_MINI_BINS)})
            current_state = state_value
            current_count = 1
        compressed.append({"state": current_state, "ratio": current_count / float(TIMELINE_MINI_BINS)})

        for item in compressed:
            item["ratio"] = round(item["ratio"], 6)
        return compressed

    def _compute_status_locked(self, pivot, now):
        topic_last_ts = pivot.get("topic_last_ts") or {}
        topic_intervals = pivot.get("topic_intervals_sec") or {}

        last_ping_ts = _safe_float(pivot.get("last_ping_ts"), None)
        last_cloudv2_ts = _safe_float(pivot.get("last_cloudv2_ts"), None)

        ping_window = self.ping_expected_sec * self.tolerance_factor
        ping_ok = False
        ping_age_sec = None
        if last_ping_ts is not None:
            ping_age_sec = now - last_ping_ts
            ping_ok = ping_age_sec <= ping_window

        cloudv2_intervals = (
            topic_intervals.get(TOPIC_CLOUDV2)
            if isinstance(topic_intervals.get(TOPIC_CLOUDV2), list)
            else pivot.get("cloudv2_intervals_sec", [])
        ) or []
        cloudv2_interval_stats = self._compute_cloudv2_interval_stats_locked(cloudv2_intervals)
        sample_count = int(cloudv2_interval_stats["sample_count"] or 0)
        median_interval_sec = cloudv2_interval_stats["median_interval_sec"]
        latched_ready = bool(pivot.get("median_latched_ready"))
        latched_interval = _safe_float(pivot.get("median_latched_interval_sec"), None)
        if median_interval_sec is not None and sample_count >= self.cloudv2_min_samples:
            pivot["median_latched_ready"] = True
            pivot["median_latched_interval_sec"] = median_interval_sec
            latched_ready = True
            latched_interval = median_interval_sec
        elif (
            sample_count < self.cloudv2_min_samples
            and latched_ready
            and latched_interval is not None
            and latched_interval > 0
        ):
            median_interval_sec = latched_interval
            sample_count = self.cloudv2_min_samples
        cloudv2_window = None
        cloudv2_ok = False
        cloudv2_age_sec = None
        if median_interval_sec is not None and last_cloudv2_ts is not None:
            cloudv2_window = median_interval_sec * self.tolerance_factor
            cloudv2_age_sec = now - last_cloudv2_ts
            cloudv2_ok = cloudv2_age_sec <= cloudv2_window

        median_ready = sample_count >= self.cloudv2_min_samples

        expected_by_topic = {}
        for topic in CONNECTIVITY_TOPICS:
            intervals = topic_intervals.get(topic)
            topic_expected = None
            if topic == TOPIC_CLOUDV2:
                topic_expected = median_interval_sec
            elif isinstance(intervals, list) and intervals:
                if len(intervals) >= self.cloudv2_min_samples or topic == TOPIC_PING:
                    topic_expected = statistics.median(intervals)
            if topic_expected is None and topic == TOPIC_PING:
                topic_expected = float(self.ping_expected_sec)
            expected_by_topic[topic] = topic_expected

        expected_candidates = [value for value in expected_by_topic.values() if value is not None and value > 0]
        max_expected_interval_sec = max(expected_candidates) if expected_candidates else None
        disconnect_threshold_sec = (
            max_expected_interval_sec * self.tolerance_factor if max_expected_interval_sec is not None else None
        )

        monitored_last_values = [
            _safe_float(topic_last_ts.get(topic), None)
            for topic in CONNECTIVITY_TOPICS
        ]
        monitored_last_values = [value for value in monitored_last_values if value is not None]
        last_monitored_message_ts = max(monitored_last_values) if monitored_last_values else None
        monitored_age_sec = (
            now - last_monitored_message_ts if last_monitored_message_ts is not None else None
        )

        disconnected_by_inactivity = False
        if disconnect_threshold_sec is not None and monitored_age_sec is not None:
            disconnected_by_inactivity = monitored_age_sec > disconnect_threshold_sec

        attention_window_sec = min(self.attention_disconnected_window_sec, self.retention_sec)
        attention_disconnected_pct = self._compute_disconnected_pct_locked(
            pivot,
            now,
            disconnect_threshold_sec,
        )
        connected_pct = None
        if attention_disconnected_pct is not None:
            connected_pct = max(0.0, min(100.0, 100.0 - attention_disconnected_pct))

        attention_by_disconnected_pct = (
            attention_disconnected_pct is not None
            and attention_disconnected_pct > self.attention_disconnected_pct_threshold
        )
        critical_by_disconnected_pct = (
            attention_disconnected_pct is not None
            and attention_disconnected_pct > self.critical_disconnected_pct_threshold
        )

        has_principal_payload_in_window = False
        has_aux_payload_in_window = False
        if attention_window_sec > 0:
            start_ts = now - attention_window_sec
            for event in pivot.get("timeline", []):
                topic = str(event.get("topic") or "")
                if topic not in CONNECTIVITY_TOPICS:
                    continue
                ts = _safe_float(event.get("ts"), None)
                if ts is None or ts < start_ts or ts > now:
                    continue
                if topic == TOPIC_CLOUDV2:
                    has_principal_payload_in_window = True
                elif topic in (TOPIC_PING, TOPIC_NETWORK, TOPIC_INFO):
                    has_aux_payload_in_window = True

        attention_by_only_aux_topics = (not has_principal_payload_in_window) and has_aux_payload_in_window

        if disconnected_by_inactivity:
            code = "red"
            reason = (
                "Sem mensagens recentes nos topicos monitorados "
                "(cloudv2/cloudv2-ping/cloudv2-info/cloudv2-network) "
                "alem da janela esperada."
            )
        elif not median_ready:
            code = "gray"
            reason = (
                "Aguardando amostras de cloudv2 para estimar mediana "
                f"({sample_count}/{self.cloudv2_min_samples})."
            )
        elif ping_ok and cloudv2_ok:
            code = "green"
            reason = "Mensagens monitoradas dentro da janela esperada."
        elif ping_ok and not cloudv2_ok:
            code = "green"
            reason = "Online por atividade recente; cloudv2 fora da janela estimada."
        else:
            code = "green"
            reason = "Online por atividade recente em topicos monitorados."

        if critical_by_disconnected_pct:
            quality_code = "critical"
            quality_reason = (
                "Percentual desconectado na janela de monitoramento "
                f"({attention_disconnected_pct:.1f}%) acima do limite critico "
                f"({self.critical_disconnected_pct_threshold:.1f}%)."
            )
        elif attention_by_disconnected_pct:
            quality_code = "yellow"
            quality_reason = (
                "Percentual desconectado na janela de monitoramento "
                f"({attention_disconnected_pct:.1f}%) acima do limite de atencao "
                f"({self.attention_disconnected_pct_threshold:.1f}%)."
            )
        elif attention_by_only_aux_topics:
            quality_code = "yellow"
            quality_reason = (
                "Sem payload no topico principal cloudv2 na janela atual; "
                "recebendo apenas ping/network/info."
            )
        else:
            quality_code = "green"
            quality_reason = "Percentuais e topicos monitorados dentro da faixa saudavel na janela atual."

        return {
            "code": code,
            "label": STATUS_LABELS[code],
            "rank": STATUS_RANK[code],
            "reason": reason,
            "online": not disconnected_by_inactivity,
            "offline": disconnected_by_inactivity,
            "quality_code": quality_code,
            "quality_label": QUALITY_LABELS[quality_code],
            "quality_rank": QUALITY_RANK[quality_code],
            "quality_reason": quality_reason,
            "ping_ok": ping_ok,
            "ping_age_sec": ping_age_sec,
            "ping_window_sec": ping_window,
            "cloudv2_ok": cloudv2_ok,
            "cloudv2_age_sec": cloudv2_age_sec,
            "cloudv2_window_sec": cloudv2_window,
            "median_ready": median_ready,
            "median_interval_sec": median_interval_sec,
            "sample_count": sample_count,
            "expected_by_topic_sec": expected_by_topic,
            "max_expected_interval_sec": max_expected_interval_sec,
            "disconnect_threshold_sec": disconnect_threshold_sec,
            "last_monitored_message_ts": last_monitored_message_ts,
            "last_monitored_message_age_sec": monitored_age_sec,
            "disconnected_by_inactivity": disconnected_by_inactivity,
            "connected_pct": connected_pct,
            "attention_disconnected_pct": attention_disconnected_pct,
            "attention_disconnected_pct_threshold": self.attention_disconnected_pct_threshold,
            "critical_disconnected_pct_threshold": self.critical_disconnected_pct_threshold,
            "attention_window_sec": attention_window_sec,
            "has_principal_payload_in_window": has_principal_payload_in_window,
            "has_aux_payload_in_window": has_aux_payload_in_window,
            "attention_by_only_aux_topics": attention_by_only_aux_topics,
            "critical_by_disconnected_pct": critical_by_disconnected_pct,
            "attention_by_disconnected_pct": attention_by_disconnected_pct,
        }

    def _refresh_status_locked(self, pivot, now):
        computed = self._compute_status_locked(pivot, now)
        cached = pivot.get("status_cache", {})
        previous_code = cached.get("code")
        previous_reason = cached.get("reason")
        previous_quality_code = cached.get("quality_code")
        previous_quality_reason = cached.get("quality_reason")

        changed = (
            (computed["code"] != previous_code)
            or (computed["reason"] != previous_reason)
            or (computed["quality_code"] != previous_quality_code)
            or (computed["quality_reason"] != previous_quality_reason)
        )
        if not changed:
            return False

        pivot["status_cache"] = {
            "code": computed["code"],
            "reason": computed["reason"],
            "quality_code": computed["quality_code"],
            "quality_reason": computed["quality_reason"],
            "changed_at_ts": now,
        }

        if previous_code != computed["code"]:
            self.log.info(
                "Mudanca de estado: pivot_id=%s %s -> %s (%s)",
                pivot["pivot_id"],
                previous_code or "-",
                computed["code"],
                computed["reason"],
            )

        if previous_quality_code != computed["quality_code"]:
            self.log.info(
                "Mudanca de qualidade: pivot_id=%s %s -> %s (%s)",
                pivot["pivot_id"],
                previous_quality_code or "-",
                computed["quality_code"],
                computed["quality_reason"],
            )

        return True

    def _build_state_settings_locked(self):
        return {
            "monitor_topics": list(self.monitor_topics),
            "connectivity_topics": list(CONNECTIVITY_TOPICS),
            "tolerance_factor": self.tolerance_factor,
            "ping_expected_sec": self.ping_expected_sec,
            "attention_disconnected_pct_threshold": self.attention_disconnected_pct_threshold,
            "critical_disconnected_pct_threshold": self.critical_disconnected_pct_threshold,
            "attention_disconnected_window_hours": self.attention_disconnected_window_hours,
            "cloudv2_median_window": self.cloudv2_window,
            "cloudv2_min_samples": self.cloudv2_min_samples,
            "show_pending_ping_pivots": self.show_pending_ping_pivots,
            "probe_timeout_streak_alert": self.probe_timeout_streak_alert,
            "max_events_per_pivot_panel": self.max_events_per_pivot_panel,
            "max_events_per_pivot_list": self.max_events_per_pivot_list,
        }

    def _build_state_snapshot_locked(self, now):
        pivots = [
            self._build_pivot_summary_locked(pivot, now)
            for _, pivot in sorted(self.pivots.items(), key=lambda item: item[0].lower())
        ]

        pending_ping = []
        for pivot_id, entry in sorted(self.pending_ping_unknown.items(), key=lambda item: item[0].lower()):
            pending_ping.append(
                {
                    "pivot_id": pivot_id,
                    "count": int(entry.get("count", 0)),
                    "first_seen_ts": entry.get("first_seen_ts"),
                    "first_seen_at": _ts_to_str(entry.get("first_seen_ts")),
                    "last_seen_ts": entry.get("last_seen_ts"),
                    "last_seen_at": _ts_to_str(entry.get("last_seen_ts")),
                    "last_payload_excerpt": entry.get("last_payload_excerpt", ""),
                }
            )

        expected_pivots_pending = self._build_expected_pivots_pending_locked()

        malformed_recent = [
            {
                "ts": item.get("ts"),
                "at": item.get("at"),
                "topic": item.get("topic"),
                "reason": item.get("reason"),
                "payload_excerpt": item.get("payload_excerpt"),
            }
            for item in self.malformed_messages[-50:]
        ]

        run_info = None
        active_run_id = str(self._active_run_id or "").strip() or None
        if active_run_id:
            try:
                run_info = self.persistence.resolve_run(run_id=active_run_id)
            except RuntimeError:
                run_info = None

        return {
            "updated_at": _ts_to_str(now),
            "updated_at_ts": now,
            "mode": str(self._monitoring_mode or "idle"),
            "run_id": active_run_id,
            "run": run_info,
            "require_apply_to_start": bool(self.require_apply_to_start),
            "settings": self._build_state_settings_locked(),
            "counts": {
                "pivots": len(pivots),
                "pending_ping_unknown": len(pending_ping),
                "expected_pivots_pending": len(expected_pivots_pending),
                "malformed_messages": len(self.malformed_messages),
                "duplicate_drops": self.duplicate_count,
            },
            "pivots": pivots,
            "pending_ping": pending_ping,
            "expected_pivots_pending": expected_pivots_pending,
            "malformed_recent": malformed_recent,
        }

    def _summarize_probe_stats_locked(self, probe):
        events = probe.get("events")
        if not isinstance(events, list):
            events = []

        sent_count = 0
        response_count = 0
        timeout_count = 0
        latency_samples = []

        for event in events:
            if not isinstance(event, dict):
                continue

            event_type = str(event.get("type") or "").strip().lower()
            if event_type == "sent":
                sent_count += 1
                continue
            if event_type == "timeout":
                timeout_count += 1
                continue
            if event_type != "response":
                continue

            response_count += 1
            latency = _safe_float(event.get("latency_sec"), None)
            if latency is not None and latency >= 0:
                latency_samples.append(latency)

        return {
            "sent_count": sent_count,
            "response_count": response_count,
            "timeout_count": timeout_count,
            "response_ratio_pct": ((response_count / sent_count) * 100.0) if sent_count > 0 else None,
            "latency_sample_count": len(latency_samples),
            "latency_last_sec": latency_samples[-1] if latency_samples else None,
            "latency_avg_sec": (sum(latency_samples) / len(latency_samples)) if latency_samples else None,
            "latency_median_sec": statistics.median(latency_samples) if latency_samples else None,
            "latency_min_sec": min(latency_samples) if latency_samples else None,
            "latency_max_sec": max(latency_samples) if latency_samples else None,
        }

    def _build_probe_delay_points_locked(self, probe_events):
        points = []
        if not isinstance(probe_events, list):
            return points

        responses = []
        for event in probe_events:
            if not isinstance(event, dict):
                continue
            if str(event.get("type") or "").strip().lower() != "response":
                continue
            ts_value = _safe_float(event.get("ts"), None)
            latency = _safe_float(event.get("latency_sec"), None)
            if ts_value is None or latency is None or latency < 0:
                continue
            responses.append((ts_value, latency))

        responses.sort(key=lambda item: item[0])
        running_total = 0.0
        sample_count = 0
        for ts_value, latency in responses:
            sample_count += 1
            running_total += latency
            points.append(
                {
                    "ts": ts_value,
                    "at": _ts_to_str(ts_value),
                    "latency_sec": latency,
                    "avg_latency_sec": running_total / sample_count,
                    "median_latency_sec": None,
                    "sample_count": sample_count,
                }
            )
        return points

    def _build_pivot_summary_locked(self, pivot, now):
        status = self._compute_status_locked(pivot, now)
        probe = pivot["probe"]
        probe_stats = self._summarize_probe_stats_locked(probe)
        modem_reset = pivot.get("modem_reset")
        if not isinstance(modem_reset, dict):
            modem_reset = {}
        is_concentrator = bool(pivot.get("is_concentrator"))
        source_last_cloud2 = pivot.get("last_cloud2")
        last_cloud2 = dict(source_last_cloud2) if isinstance(source_last_cloud2, dict) else {}
        _refresh_timestamp_field(last_cloud2)
        if is_concentrator:
            last_cloud2["technology"] = CONCENTRATOR_TECH_LABEL
        signal = _normalize_text(last_cloud2.get("rssi"))
        technology = _normalize_text(last_cloud2.get("technology"))
        combined_signal_technology = _normalize_text(pivot.get("signal_technology"))
        if combined_signal_technology:
            parsed_signal, parsed_technology = _parse_signal_technology_combined(combined_signal_technology)
            if not signal and parsed_signal:
                signal = parsed_signal
            if not technology and parsed_technology:
                technology = parsed_technology
        if signal and not _normalize_text(last_cloud2.get("rssi")):
            last_cloud2["rssi"] = signal
        if technology and not _normalize_text(last_cloud2.get("technology")):
            last_cloud2["technology"] = technology
        signal_technology = f"{signal or '-'} / {technology or '-'}"
        last_activity_ts = max(
            [
                _safe_float(pivot.get("last_seen_ts"), 0) or 0,
                _safe_float(pivot.get("last_ping_ts"), 0) or 0,
                _safe_float(pivot.get("last_cloudv2_ts"), 0) or 0,
                _safe_float(pivot.get("last_cloud2_ts"), 0) or 0,
                _safe_float(probe.get("last_sent_ts"), 0) or 0,
                _safe_float(probe.get("last_response_ts"), 0) or 0,
            ]
        )

        probe_alert = int(probe.get("timeout_streak", 0)) >= self.probe_timeout_streak_alert
        timeline_mini = self._build_timeline_mini_segments_locked(
            pivot,
            now,
            disconnect_threshold_sec=status["disconnect_threshold_sec"],
            window_sec=TIMELINE_MINI_WINDOW_SEC,
        )
        pivot_config = _normalize_pivot_config_state(pivot.get("pivot_config"))
        pivot_config_summary = dict(pivot_config)
        pivot_config_summary["last_request_at"] = _ts_to_str(pivot_config.get("last_request_ts"))
        pivot_config_summary["last_response_at"] = _ts_to_str(pivot_config.get("last_response_ts"))
        last_request_ts = _safe_float(pivot_config.get("last_request_ts"), None)
        last_response_ts = _safe_float(pivot_config.get("last_response_ts"), None)
        pivot_config_summary["pending"] = last_request_ts is not None and (
            last_response_ts is None or last_response_ts < last_request_ts
        )
        network_config = _normalize_network_config_state(pivot.get("network_config"))
        network_config_summary = dict(network_config)
        network_config_summary["last_request_at"] = _ts_to_str(network_config.get("last_request_ts"))
        network_config_summary["last_response_at"] = _ts_to_str(network_config.get("last_response_ts"))
        net_last_request_ts = _safe_float(network_config.get("last_request_ts"), None)
        net_last_response_ts = _safe_float(network_config.get("last_response_ts"), None)
        network_config_summary["pending"] = net_last_request_ts is not None and (
            net_last_response_ts is None or net_last_response_ts < net_last_request_ts
        )
        rush_config = _normalize_rush_config_state(pivot.get("rush_config"))
        rush_config_summary = dict(rush_config)
        rush_config_summary["last_request_at"] = _ts_to_str(rush_config.get("last_request_ts"))
        rush_config_summary["last_response_at"] = _ts_to_str(rush_config.get("last_response_ts"))
        rush_last_request_ts = _safe_float(rush_config.get("last_request_ts"), None)
        rush_last_response_ts = _safe_float(rush_config.get("last_response_ts"), None)
        rush_config_summary["pending"] = rush_last_request_ts is not None and (
            rush_last_response_ts is None or rush_last_response_ts < rush_last_request_ts
        )
        sector_config = _normalize_sector_config_state(pivot.get("sector_config"))
        sector_config_summary = dict(sector_config)
        sector_config_summary["last_request_at"] = _ts_to_str(sector_config.get("last_request_ts"))
        sector_config_summary["last_response_at"] = _ts_to_str(sector_config.get("last_response_ts"))
        sector_last_request_ts = _safe_float(sector_config.get("last_request_ts"), None)
        sector_last_response_ts = _safe_float(sector_config.get("last_response_ts"), None)
        sector_config_summary["pending"] = sector_last_request_ts is not None and (
            sector_last_response_ts is None or sector_last_response_ts < sector_last_request_ts
        )
        physical_barrier_config = _normalize_physical_barrier_config_state(pivot.get("physical_barrier_config"))
        physical_barrier_config_summary = dict(physical_barrier_config)
        physical_barrier_config_summary["last_request_at"] = _ts_to_str(physical_barrier_config.get("last_request_ts"))
        physical_barrier_config_summary["last_response_at"] = _ts_to_str(physical_barrier_config.get("last_response_ts"))
        barrier_last_request_ts = _safe_float(physical_barrier_config.get("last_request_ts"), None)
        barrier_last_response_ts = _safe_float(physical_barrier_config.get("last_response_ts"), None)
        physical_barrier_config_summary["pending"] = barrier_last_request_ts is not None and (
            barrier_last_response_ts is None or barrier_last_response_ts < barrier_last_request_ts
        )

        return {
            "pivot_id": pivot["pivot_id"],
            "pivot_slug": pivot["pivot_slug"],
            "session_id": pivot.get("session_id"),
            "run_id": pivot.get("run_id"),
            "is_concentrator": is_concentrator,
            "latitude": _safe_float(pivot.get("latitude"), None),
            "longitude": _safe_float(pivot.get("longitude"), None),
            "flags": {
                "online": status["online"],
                "offline": status["offline"],
            },
            "status": {
                "code": status["code"],
                "label": status["label"],
                "rank": status["rank"],
                "reason": status["reason"],
            },
            "quality": {
                "code": status["quality_code"],
                "label": status["quality_label"],
                "rank": status["quality_rank"],
                "reason": status["quality_reason"],
            },
            "ping_ok": status["ping_ok"],
            "cloudv2_ok": status["cloudv2_ok"],
            "median_ready": status["median_ready"],
            "median_cloudv2_interval_sec": status["median_interval_sec"],
            "median_sample_count": min(max(0, int(status["sample_count"] or 0)), self.cloudv2_min_samples),
            "max_expected_interval_sec": status["max_expected_interval_sec"],
            "disconnect_threshold_sec": status["disconnect_threshold_sec"],
            "disconnected_by_inactivity": status["disconnected_by_inactivity"],
            "connected_pct": status["connected_pct"],
            "disconnected_pct": status["attention_disconnected_pct"],
            "attention_disconnected_pct": status["attention_disconnected_pct"],
            "attention_disconnected_pct_threshold": status["attention_disconnected_pct_threshold"],
            "critical_disconnected_pct_threshold": status["critical_disconnected_pct_threshold"],
            "attention_window_sec": status["attention_window_sec"],
            "has_principal_payload_in_window": status["has_principal_payload_in_window"],
            "has_aux_payload_in_window": status["has_aux_payload_in_window"],
            "attention_by_only_aux_topics": status["attention_by_only_aux_topics"],
            "critical_by_disconnected_pct": status["critical_by_disconnected_pct"],
            "attention_by_disconnected_pct": status["attention_by_disconnected_pct"],
            "last_monitored_message_ts": status["last_monitored_message_ts"],
            "last_monitored_message_at": _ts_to_str(status["last_monitored_message_ts"]),
            "last_monitored_message_age_sec": status["last_monitored_message_age_sec"],
            "expected_by_topic_sec": status["expected_by_topic_sec"],
            "last_ping_ts": pivot.get("last_ping_ts"),
            "last_ping_at": _ts_to_str(pivot.get("last_ping_ts")),
            "last_cloudv2_ts": pivot.get("last_cloudv2_ts"),
            "last_cloudv2_at": _ts_to_str(pivot.get("last_cloudv2_ts")),
            "last_cloud2": last_cloud2,
            "signal": signal,
            "technology": technology,
            "signal_technology": signal_technology,
            "timeline_mini": timeline_mini,
            "last_activity_ts": last_activity_ts if last_activity_ts > 0 else None,
            "last_activity_at": _ts_to_str(last_activity_ts) if last_activity_ts > 0 else "-",
            "last_activity_ago": _format_ago(now - last_activity_ts) if last_activity_ts > 0 else "-",
            "topic_counters": dict(pivot.get("topic_counters", {})),
            "probe": {
                "enabled": bool(probe.get("enabled")),
                "interval_sec": int(probe.get("interval_sec", self.probe_default_interval_sec)),
                "last_sent_ts": probe.get("last_sent_ts"),
                "last_sent_at": _ts_to_str(probe.get("last_sent_ts")),
                "last_response_ts": probe.get("last_response_ts"),
                "last_response_at": _ts_to_str(probe.get("last_response_ts")),
                "last_response_info": _normalize_probe_response_info(probe.get("last_response_info")),
                "pending": probe.get("pending_sent_ts") is not None,
                "pending_deadline_ts": probe.get("pending_deadline_ts"),
                "pending_deadline_at": _ts_to_str(probe.get("pending_deadline_ts")),
                "timeout_streak": int(probe.get("timeout_streak", 0)),
                "last_result": probe.get("last_result"),
                "alert": probe_alert,
                "sent_count": int(probe_stats["sent_count"]),
                "response_count": int(probe_stats["response_count"]),
                "timeout_count": int(probe_stats["timeout_count"]),
                "response_ratio_pct": probe_stats["response_ratio_pct"],
                "latency_sample_count": int(probe_stats["latency_sample_count"]),
                "latency_last_sec": probe_stats["latency_last_sec"],
                "latency_avg_sec": probe_stats["latency_avg_sec"],
                "latency_median_sec": probe_stats["latency_median_sec"],
                "latency_min_sec": probe_stats["latency_min_sec"],
                "latency_max_sec": probe_stats["latency_max_sec"],
            },
            "modem_reset": {
                "last_command_ts": _safe_float(modem_reset.get("last_command_ts"), None),
                "last_command_at": _ts_to_str(modem_reset.get("last_command_ts")),
                "last_command_topic": _normalize_text(modem_reset.get("last_command_topic")),
                "last_command_payload": _normalize_text(modem_reset.get("last_command_payload")),
                "last_ack_ts": _safe_float(modem_reset.get("last_ack_ts"), None),
                "last_ack_at": _ts_to_str(modem_reset.get("last_ack_ts")),
                "last_ack_topic": _normalize_text(modem_reset.get("last_ack_topic")),
                "last_ack_payload": _normalize_text(modem_reset.get("last_ack_payload")),
                "command_count": int(modem_reset.get("command_count") or 0),
                "ack_count": int(modem_reset.get("ack_count") or 0),
            },
            "pivot_config": pivot_config_summary,
            "network_config": network_config_summary,
            "rush_config": rush_config_summary,
            "sector_config": sector_config_summary,
            "physical_barrier_config": physical_barrier_config_summary,
        }

    def _build_pivot_snapshot_locked(self, pivot, now):
        summary = self._build_pivot_summary_locked(pivot, now)

        drop_events = _clone_timestamped_items(pivot.get("drop_events", []))
        drops_24h = [item for item in drop_events if _safe_float(item.get("ts"), 0) >= (now - 86400)]
        drops_7d = [item for item in drop_events if _safe_float(item.get("ts"), 0) >= (now - 604800)]

        last_drop = drop_events[-1] if drop_events else None
        summary_last_cloud2 = summary.get("last_cloud2") if isinstance(summary.get("last_cloud2"), dict) else None
        last_cloud2 = dict(summary_last_cloud2) if isinstance(summary_last_cloud2, dict) else (pivot.get("last_cloud2") or {})
        if not isinstance(last_cloud2, dict):
            last_cloud2 = {}
        _refresh_timestamp_field(last_cloud2)

        timeline = sorted(
            _clone_timestamped_items(pivot.get("timeline", [])),
            key=lambda item: _safe_float(item.get("ts"), 0),
            reverse=True,
        )

        probe_events = sorted(
            _clone_timestamped_items(
                pivot.get("probe", {}).get("events", []),
                field_pairs=(("ts", "at"), ("sent_ts", "sent_at"), ("deadline_ts", "deadline_at")),
            ),
            key=lambda item: _safe_float(item.get("ts"), 0),
            reverse=True,
        )
        probe_delay_points = self._build_probe_delay_points_locked(probe_events)
        rssi_series = sorted(
            _clone_timestamped_items(pivot.get("ping_rssi_points", [])),
            key=lambda item: _safe_float(item.get("ts"), 0),
        )
        session_info = self.persistence.resolve_session(
            pivot["pivot_id"],
            session_id=pivot.get("session_id"),
            run_id=pivot.get("run_id"),
        )
        run_id = str(
            pivot.get("run_id")
            or (session_info or {}).get("run_id")
            or self._active_run_id
            or ""
        ).strip() or None
        run_info = self.persistence.resolve_run(run_id=run_id) if run_id else None

        return {
            "pivot_id": pivot["pivot_id"],
            "pivot_slug": pivot["pivot_slug"],
            "session_id": pivot.get("session_id"),
            "run_id": run_id,
            "run": run_info,
            "session": session_info,
            "updated_at": _ts_to_str(now),
            "updated_at_ts": now,
            "summary": summary,
            "metrics": {
                "drops_24h": len(drops_24h),
                "drops_7d": len(drops_7d),
                "last_drop_duration_sec": (last_drop or {}).get("duration_sec"),
                "last_drop_at": (last_drop or {}).get("at", "-"),
                "last_rssi": last_cloud2.get("rssi"),
                "last_technology": last_cloud2.get("technology"),
                "last_firmware": last_cloud2.get("firmware"),
                "last_cloud2_event_date": last_cloud2.get("event_date"),
                "last_cloud2_at": last_cloud2.get("at", "-"),
            },
            "timeline": timeline,
            "probe_events": probe_events,
            "probe_delay_points": probe_delay_points,
            "hasRssi": bool(rssi_series),
            "rssiSeries": rssi_series,
            "cloud2_events": sorted(
                _clone_timestamped_items(pivot.get("cloud2_events", [])),
                key=lambda item: _safe_float(item.get("ts"), 0),
                reverse=True,
            ),
        }

    def _build_runtime_payload_locked(self, now):
        payload = {
            "version": 5,
            "updated_at": _ts_to_str(now),
            "updated_at_ts": now,
            "event_seq": self._event_seq,
            "active_run_id": self._active_run_id,
            "monitoring_mode": self._monitoring_mode,
            "probe_settings": self._probe_settings,
            "pending_ping_unknown": self.pending_ping_unknown,
            "pending_expected_pivots": self.pending_expected_pivots,
            "malformed_messages": self.malformed_messages,
            "duplicate_count": self.duplicate_count,
            "pivots": self.pivots,
        }

        # Garante serializacao JSON sem referencias compartilhadas mutaveis.
        return json.loads(json.dumps(payload, ensure_ascii=False))

    def _load_runtime_state(self):
        if not os.path.exists(self.runtime_path):
            return

        try:
            with open(self.runtime_path, "r", encoding="utf-8") as file:
                loaded = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            self.log.warning("Nao foi possivel restaurar runtime_store.json: %s", exc)
            return

        if not isinstance(loaded, dict):
            return

        with self._lock:
            loaded_run_id = str(loaded.get("active_run_id") or "").strip()
            if loaded_run_id:
                self._active_run_id = loaded_run_id
            loaded_mode = str(loaded.get("monitoring_mode") or "").strip().lower()
            if loaded_mode in ("idle", "live", "history"):
                self._monitoring_mode = loaded_mode

            loaded_probe = loaded.get("probe_settings")
            if isinstance(loaded_probe, dict):
                self._probe_settings = self._normalize_probe_settings(loaded_probe)

            loaded_pivots = loaded.get("pivots", {})
            if isinstance(loaded_pivots, dict):
                restored = {}
                for pivot_id, raw_pivot in loaded_pivots.items():
                    normalized_pivot_id = str(pivot_id or "").strip()
                    if not normalized_pivot_id or not isinstance(raw_pivot, dict):
                        continue

                    discovered_ts = _safe_float(raw_pivot.get("discovered_at_ts"), time.time())
                    raw_session_id = str(raw_pivot.get("session_id") or "").strip()
                    raw_run_id = str(raw_pivot.get("run_id") or "").strip()
                    pivot = self._new_pivot_state(
                        normalized_pivot_id,
                        discovered_ts,
                        session_id=(raw_session_id or None),
                        run_id=(raw_run_id or self._active_run_id),
                    )
                    raw_is_concentrator = raw_pivot.get("is_concentrator")
                    if isinstance(raw_is_concentrator, bool):
                        pivot["is_concentrator"] = raw_is_concentrator
                    elif isinstance(raw_is_concentrator, (int, float)):
                        pivot["is_concentrator"] = bool(raw_is_concentrator)
                    elif isinstance(raw_is_concentrator, str):
                        normalized_flag = raw_is_concentrator.strip().lower()
                        if normalized_flag in ("1", "true", "yes", "sim"):
                            pivot["is_concentrator"] = True
                        elif normalized_flag in ("0", "false", "no", "nao", "não"):
                            pivot["is_concentrator"] = False

                    raw_latitude = _safe_float(raw_pivot.get("latitude"), None)
                    raw_longitude = _safe_float(raw_pivot.get("longitude"), None)
                    if raw_latitude is not None:
                        pivot["latitude"] = raw_latitude
                    if raw_longitude is not None:
                        pivot["longitude"] = raw_longitude
                    if raw_latitude is None or raw_longitude is None:
                        persisted_coordinates = self._load_pivot_coordinates_locked(normalized_pivot_id)
                        if raw_latitude is None:
                            pivot["latitude"] = persisted_coordinates["latitude"]
                        if raw_longitude is None:
                            pivot["longitude"] = persisted_coordinates["longitude"]

                    for field in (
                        "last_seen_ts",
                        "last_ping_ts",
                        "last_cloudv2_ts",
                        "last_cloud2_ts",
                        "discovered_at_ts",
                    ):
                        value = _safe_float(raw_pivot.get(field), None)
                        pivot[field] = value

                    topic_counters = raw_pivot.get("topic_counters")
                    if isinstance(topic_counters, dict):
                        for topic in self.monitor_topics:
                            pivot["topic_counters"][topic] = int(topic_counters.get(topic, 0))

                    raw_topic_last = raw_pivot.get("topic_last_ts")
                    if isinstance(raw_topic_last, dict):
                        for topic in CONNECTIVITY_TOPICS:
                            pivot["topic_last_ts"][topic] = _safe_float(raw_topic_last.get(topic), None)

                    raw_topic_intervals = raw_pivot.get("topic_intervals_sec")
                    if isinstance(raw_topic_intervals, dict):
                        for topic in CONNECTIVITY_TOPICS:
                            values = raw_topic_intervals.get(topic)
                            if not isinstance(values, list):
                                continue
                            cleaned = []
                            for item in values:
                                parsed_interval = _safe_float(item, None)
                                if parsed_interval is not None and parsed_interval > 0:
                                    cleaned.append(parsed_interval)
                            pivot["topic_intervals_sec"][topic] = cleaned[-self.cloudv2_window :]

                    intervals = raw_pivot.get("cloudv2_intervals_sec")
                    if isinstance(intervals, list):
                        cleaned_intervals = []
                        for item in intervals:
                            parsed = _safe_float(item, None)
                            if parsed is not None and parsed > 0:
                                cleaned_intervals.append(parsed)
                        pivot["cloudv2_intervals_sec"] = cleaned_intervals[-self.cloudv2_window :]

                    # Compatibilidade entre estado legado e estrutura por topico.
                    if not pivot["topic_intervals_sec"].get(TOPIC_CLOUDV2):
                        pivot["topic_intervals_sec"][TOPIC_CLOUDV2] = list(pivot["cloudv2_intervals_sec"])
                    else:
                        pivot["cloudv2_intervals_sec"] = list(pivot["topic_intervals_sec"][TOPIC_CLOUDV2])
                    if pivot.get("last_cloudv2_ts") is not None:
                        pivot["topic_last_ts"][TOPIC_CLOUDV2] = _safe_float(pivot.get("last_cloudv2_ts"), None)
                    if pivot.get("last_ping_ts") is not None:
                        pivot["topic_last_ts"][TOPIC_PING] = _safe_float(pivot.get("last_ping_ts"), None)

                    last_cloud2 = raw_pivot.get("last_cloud2")
                    if isinstance(last_cloud2, dict):
                        pivot["last_cloud2"] = last_cloud2

                    for list_field in ("cloud2_events", "ping_rssi_points", "drop_events", "timeline"):
                        raw_list = raw_pivot.get(list_field)
                        if isinstance(raw_list, list):
                            pivot[list_field] = raw_list[-self.max_events_per_pivot :]

                    raw_probe = raw_pivot.get("probe")
                    if isinstance(raw_probe, dict):
                        probe = pivot["probe"]
                        probe["enabled"] = bool(raw_probe.get("enabled", probe["enabled"]))
                        probe_interval = _safe_int(raw_probe.get("interval_sec"), probe["interval_sec"])
                        if probe_interval is not None and probe_interval >= self.probe_min_interval_sec:
                            probe["interval_sec"] = probe_interval
                        probe["last_sent_ts"] = _safe_float(raw_probe.get("last_sent_ts"), None)
                        probe["last_response_ts"] = _safe_float(raw_probe.get("last_response_ts"), None)
                        probe["last_response_info"] = _normalize_probe_response_info(raw_probe.get("last_response_info"))
                        probe["pending_sent_ts"] = _safe_float(raw_probe.get("pending_sent_ts"), None)
                        probe["pending_deadline_ts"] = _safe_float(raw_probe.get("pending_deadline_ts"), None)
                        probe["timeout_streak"] = _safe_int(raw_probe.get("timeout_streak"), 0) or 0
                        probe["last_result"] = raw_probe.get("last_result")
                        if isinstance(raw_probe.get("events"), list):
                            probe["events"] = raw_probe.get("events")[-self.max_events_per_pivot :]

                    raw_modem_reset = raw_pivot.get("modem_reset")
                    if isinstance(raw_modem_reset, dict):
                        modem_reset = pivot.get("modem_reset")
                        if not isinstance(modem_reset, dict):
                            modem_reset = {}
                            pivot["modem_reset"] = modem_reset
                        modem_reset["last_command_ts"] = _safe_float(raw_modem_reset.get("last_command_ts"), None)
                        modem_reset["last_command_topic"] = raw_modem_reset.get("last_command_topic")
                        modem_reset["last_command_payload"] = raw_modem_reset.get("last_command_payload")
                        modem_reset["last_ack_ts"] = _safe_float(raw_modem_reset.get("last_ack_ts"), None)
                        modem_reset["last_ack_topic"] = raw_modem_reset.get("last_ack_topic")
                        modem_reset["last_ack_payload"] = raw_modem_reset.get("last_ack_payload")
                        modem_reset["last_ack_idp"] = raw_modem_reset.get("last_ack_idp")
                        modem_reset["command_count"] = _safe_int(raw_modem_reset.get("command_count"), 0) or 0
                        modem_reset["ack_count"] = _safe_int(raw_modem_reset.get("ack_count"), 0) or 0

                    pivot["network_config"] = _normalize_network_config_state(raw_pivot.get("network_config"))
                    pivot["pivot_config"] = _normalize_pivot_config_state(raw_pivot.get("pivot_config"))
                    pivot["rush_config"] = _normalize_rush_config_state(raw_pivot.get("rush_config"))
                    pivot["sector_config"] = _normalize_sector_config_state(raw_pivot.get("sector_config"))
                    pivot["physical_barrier_config"] = _normalize_physical_barrier_config_state(raw_pivot.get("physical_barrier_config"))

                    raw_status = raw_pivot.get("status_cache")
                    if isinstance(raw_status, dict):
                        pivot["status_cache"] = {
                            "code": str(raw_status.get("code") or "gray"),
                            "reason": str(raw_status.get("reason") or ""),
                            "quality_code": str(raw_status.get("quality_code") or "green"),
                            "quality_reason": str(raw_status.get("quality_reason") or ""),
                            "changed_at_ts": _safe_float(raw_status.get("changed_at_ts"), discovered_ts),
                        }

                    restored[normalized_pivot_id] = pivot

                self.pivots = restored
                for normalized_pivot_id, pivot in self.pivots.items():
                    session_id = str(pivot.get("session_id") or "").strip()
                    if session_id:
                        pivot_run = str(pivot.get("run_id") or "").strip()
                        if self._active_run_id and pivot_run and pivot_run != self._active_run_id:
                            continue
                        self._active_session_by_pivot[normalized_pivot_id] = session_id

            pending_ping = loaded.get("pending_ping_unknown")
            if isinstance(pending_ping, dict):
                self.pending_ping_unknown = pending_ping

            malformed_messages = loaded.get("malformed_messages")
            if isinstance(malformed_messages, list):
                self.malformed_messages = malformed_messages[-500:]

            self.duplicate_count = _safe_int(loaded.get("duplicate_count"), 0) or 0
            self._event_seq = _safe_int(loaded.get("event_seq"), 0) or 0

            now = time.time()
            for pivot in self.pivots.values():
                self._prune_pivot_locked(pivot, now)
                self._refresh_status_locked(pivot, now)
            self._cleanup_pending_ping_locked(now)
            if self._cleanup_expected_pivots_locked():
                self._persist_pending_expected_pivots_locked()

        self.log.info("Estado de runtime restaurado com %s pivots.", len(self.pivots))

    def _clear_dashboard_data_files(self):
        if not os.path.isdir(DATA_DIR):
            return

        for name in os.listdir(DATA_DIR):
            if not name.endswith(".json"):
                continue
            path = os.path.join(DATA_DIR, name)
            try:
                os.remove(path)
            except OSError:
                pass
