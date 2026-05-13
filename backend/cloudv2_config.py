import json
import os

from backend.cloudv2_db import normalize_db_backend
from backend.cloudv2_paths import resolve_data_dir


CONFIG_FILE_ENV = "CONFIG_FILE"
DEFAULT_CONFIG_FILE = "cloudv2-config.json"

FIXED_MONITOR_TOPICS = [
    "cloudv2",
    "cloudv2-ping",
    "cloud2",
    "cloudv2-network",
    "cloudv2-info",
]

PROBE_RESPONSE_TOPICS = ["cloudv2-network", "cloudv2-info"]

DEFAULT_CONFIG = {
    "broker": "a19mijesri84u2-ats.iot.us-east-1.amazonaws.com",
    "port": 8883,
    "topics": list(FIXED_MONITOR_TOPICS),
    "monitor_topics": list(FIXED_MONITOR_TOPICS),
    "filter_names": [],
    "cmd_topics": [],
    "info_topics": list(PROBE_RESPONSE_TOPICS),
    "min_minutes": 1,
    "max_minutes": 10,
    "schedule_mode": "fixed",
    "fixed_minutes": 5,
    "response_timeout_sec": 10,
    "ping_interval_minutes": 3,
    "ping_topic": "cloudv2-ping",
    "dashboard_enabled": True,
    "dashboard_port": 8008,
    "dashboard_refresh_sec": 5,
    "api_state_cache_ttl_sec": 2.0,
    "api_quality_cache_ttl_sec": 2.0,
    "enable_background_worker": True,
    "require_apply_to_start": True,
    "continuous_monitoring_mode": True,
    "history_mode": "merge",
    "history_retention_hours": 24,
    "tolerance_factor": 3.0,
    "attention_disconnected_pct_threshold": 20.0,
    "critical_disconnected_pct_threshold": 50.0,
    "attention_disconnected_window_hours": 24,
    "cloudv2_median_window": 20,
    "cloudv2_min_samples": 5,
    "dedupe_window_sec": 8,
    "show_pending_ping_pivots": False,
    "probe_default_interval_sec": 300,
    "probe_min_interval_sec": 60,
    "probe_timeout_factor": 1.25,
    "probe_timeout_streak_alert": 2,
    "max_events_per_pivot": 5000,
    "max_events_per_pivot_panel": 5000,
    "max_events_per_pivot_list": 5000,
    "probe_settings": {},
    "db_backend": "sqlite",
    "sqlite_db_path": os.path.join(resolve_data_dir(), "telemetry.sqlite3"),
    "database_url": "",
}


def get_config_file_path():
    return os.environ.get(CONFIG_FILE_ENV, DEFAULT_CONFIG_FILE)


def _to_int(value, fallback, minimum=None):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = fallback
    if minimum is not None and parsed < minimum:
        return minimum
    return parsed


def _to_float(value, fallback, minimum=None):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = fallback
    if minimum is not None and parsed < minimum:
        return minimum
    return parsed


def _to_bool(value, fallback):
    if isinstance(value, bool):
        return value
    if value is None:
        return fallback
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "y", "on"):
        return True
    if text in ("0", "false", "no", "n", "off"):
        return False
    return fallback


def _normalize_string_list(values):
    if isinstance(values, str):
        values = values.replace("\n", ",").split(",")
    if not isinstance(values, list):
        return []

    normalized = []
    for item in values:
        as_text = str(item).strip()
        if as_text and as_text not in normalized:
            normalized.append(as_text)
    return normalized


def _normalize_history_mode(value):
    text = str(value or "").strip().lower()
    if text in ("fresh", "new", "zero", "reset", "novo", "zerar", "iniciar_do_zero"):
        return "fresh"
    return "merge"


def _normalize_schedule_mode(value):
    text = str(value or "").strip().lower()
    if text in ("fixed", "fixo", "periodic", "periodico"):
        return "fixed"
    return "random"


def _read_config_file(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as file:
            loaded = json.load(file)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Falha ao carregar configuracao de {path}: {exc}")
        return {}

    if not isinstance(loaded, dict):
        print(f"Arquivo de configuracao invalido (esperado objeto JSON): {path}")
        return {}
    return loaded


def _normalize_probe_settings(values, default_interval, minimum_interval):
    normalized = {}

    if isinstance(values, list):
        for pivot_id in _normalize_string_list(values):
            normalized[pivot_id] = {
                "enabled": True,
                "interval_sec": _to_int(default_interval, default_interval, minimum=minimum_interval),
            }
        return normalized

    if not isinstance(values, dict):
        return normalized

    for raw_pivot_id, raw_item in values.items():
        pivot_id = str(raw_pivot_id).strip()
        if not pivot_id:
            continue

        enabled = True
        interval_sec = default_interval

        if isinstance(raw_item, dict):
            enabled = _to_bool(raw_item.get("enabled"), True)
            interval_sec = _to_int(
                raw_item.get("interval_sec", raw_item.get("interval", default_interval)),
                default_interval,
                minimum=minimum_interval,
            )
        elif isinstance(raw_item, bool):
            enabled = raw_item
        elif raw_item is None:
            enabled = False
        else:
            enabled = _to_bool(raw_item, True)

        normalized[pivot_id] = {
            "enabled": enabled,
            "interval_sec": interval_sec,
        }

    return normalized


def _apply_env_overrides(config):
    render_value = str(os.environ.get("RENDER", "")).strip().lower()
    running_on_render = render_value in ("1", "true", "yes", "on") or bool(
        os.environ.get("RENDER_SERVICE_ID")
    )

    overrides = {
        "BROKER": "broker",
        "MQTT_PORT": "port",
        "MIN_MINUTES": "min_minutes",
        "MAX_MINUTES": "max_minutes",
        "SCHEDULE_MODE": "schedule_mode",
        "FIXED_MINUTES": "fixed_minutes",
        "RESPONSE_TIMEOUT_SEC": "response_timeout_sec",
        "PING_INTERVAL_MINUTES": "ping_interval_minutes",
        "DASHBOARD_ENABLED": "dashboard_enabled",
        "DASHBOARD_PORT": "dashboard_port",
        "DASHBOARD_REFRESH_SEC": "dashboard_refresh_sec",
        "API_STATE_CACHE_TTL_SEC": "api_state_cache_ttl_sec",
        "API_QUALITY_CACHE_TTL_SEC": "api_quality_cache_ttl_sec",
        "ENABLE_BACKGROUND_WORKER": "enable_background_worker",
        "REQUIRE_APPLY_TO_START": "require_apply_to_start",
        "CONTINUOUS_MONITORING_MODE": "continuous_monitoring_mode",
        "HISTORY_MODE": "history_mode",
        "HISTORY_RETENTION_HOURS": "history_retention_hours",
        "TOLERANCE_FACTOR": "tolerance_factor",
        "ATTENTION_DISCONNECTED_PCT_THRESHOLD": "attention_disconnected_pct_threshold",
        "CRITICAL_DISCONNECTED_PCT_THRESHOLD": "critical_disconnected_pct_threshold",
        "ATTENTION_DISCONNECTED_WINDOW_HOURS": "attention_disconnected_window_hours",
        "CLOUDV2_MEDIAN_WINDOW": "cloudv2_median_window",
        "CLOUDV2_MIN_SAMPLES": "cloudv2_min_samples",
        "DEDUPE_WINDOW_SEC": "dedupe_window_sec",
        "SHOW_PENDING_PING_PIVOTS": "show_pending_ping_pivots",
        "PROBE_DEFAULT_INTERVAL_SEC": "probe_default_interval_sec",
        "PROBE_MIN_INTERVAL_SEC": "probe_min_interval_sec",
        "PROBE_TIMEOUT_FACTOR": "probe_timeout_factor",
        "PROBE_TIMEOUT_STREAK_ALERT": "probe_timeout_streak_alert",
        "MAX_EVENTS_PER_PIVOT": "max_events_per_pivot",
        "MAX_EVENTS_PER_PIVOT_PANEL": "max_events_per_pivot_panel",
        "MAX_EVENTS_PER_PIVOT_LIST": "max_events_per_pivot_list",
        "DB_BACKEND": "db_backend",
        "SQLITE_DB_PATH": "sqlite_db_path",
        "DATABASE_URL": "database_url",
    }
    for env_name, config_key in overrides.items():
        env_value = os.environ.get(env_name)
        if env_value is not None and env_value != "":
            config[config_key] = env_value

    legacy_port_env = os.environ.get("PORT")
    mqtt_port_env = os.environ.get("MQTT_PORT")
    dashboard_port_env = os.environ.get("DASHBOARD_PORT")

    # Compatibilidade legada: fora do Render, PORT ainda pode definir a porta MQTT.
    if legacy_port_env and not mqtt_port_env and not running_on_render:
        config["port"] = legacy_port_env

    # No Render, PORT e a porta HTTP de entrada publicada pela plataforma.
    if running_on_render and legacy_port_env and not dashboard_port_env:
        config["dashboard_port"] = legacy_port_env

    # Em producao, aumenta o intervalo padrao do refresh do dashboard para reduzir carga.
    # Ainda permite sobrescrita explicita via arquivo/env.
    if running_on_render and not os.environ.get("DASHBOARD_REFRESH_SEC"):
        current_refresh = _to_int(
            config.get("dashboard_refresh_sec"),
            DEFAULT_CONFIG["dashboard_refresh_sec"],
            minimum=1,
        )
        if current_refresh < 10:
            config["dashboard_refresh_sec"] = 10

    cmd_topics_env = os.environ.get("CMD_TOPICS")
    if cmd_topics_env:
        config["cmd_topics"] = cmd_topics_env

    probe_settings_env = os.environ.get("PROBE_SETTINGS")
    if probe_settings_env:
        parsed = None
        try:
            parsed = json.loads(probe_settings_env)
        except json.JSONDecodeError:
            pass
        if parsed is None:
            parsed = [item.strip() for item in probe_settings_env.split(",") if item.strip()]
        config["probe_settings"] = parsed


def normalize_config(raw_config):
    base = dict(DEFAULT_CONFIG)
    if isinstance(raw_config, dict):
        for key in DEFAULT_CONFIG:
            if key in raw_config:
                base[key] = raw_config[key]

    base["broker"] = str(base.get("broker", DEFAULT_CONFIG["broker"])).strip() or DEFAULT_CONFIG["broker"]
    base["port"] = _to_int(base.get("port"), DEFAULT_CONFIG["port"], minimum=1)

    base["min_minutes"] = _to_int(base.get("min_minutes"), DEFAULT_CONFIG["min_minutes"], minimum=1)
    base["max_minutes"] = _to_int(base.get("max_minutes"), DEFAULT_CONFIG["max_minutes"], minimum=1)
    if base["min_minutes"] > base["max_minutes"]:
        base["min_minutes"], base["max_minutes"] = base["max_minutes"], base["min_minutes"]

    base["schedule_mode"] = _normalize_schedule_mode(base.get("schedule_mode", DEFAULT_CONFIG["schedule_mode"]))
    base["fixed_minutes"] = _to_int(base.get("fixed_minutes"), DEFAULT_CONFIG["fixed_minutes"], minimum=1)
    base["response_timeout_sec"] = _to_int(
        base.get("response_timeout_sec"),
        DEFAULT_CONFIG["response_timeout_sec"],
        minimum=1,
    )

    base["ping_interval_minutes"] = _to_int(
        base.get("ping_interval_minutes"),
        DEFAULT_CONFIG["ping_interval_minutes"],
        minimum=1,
    )
    base["dashboard_enabled"] = _to_bool(
        base.get("dashboard_enabled"),
        DEFAULT_CONFIG["dashboard_enabled"],
    )
    base["dashboard_port"] = _to_int(
        base.get("dashboard_port"),
        DEFAULT_CONFIG["dashboard_port"],
        minimum=1,
    )
    base["dashboard_refresh_sec"] = _to_int(
        base.get("dashboard_refresh_sec"),
        DEFAULT_CONFIG["dashboard_refresh_sec"],
        minimum=1,
    )
    base["api_state_cache_ttl_sec"] = _to_float(
        base.get("api_state_cache_ttl_sec"),
        DEFAULT_CONFIG["api_state_cache_ttl_sec"],
        minimum=0.0,
    )
    if base["api_state_cache_ttl_sec"] > 5.0:
        base["api_state_cache_ttl_sec"] = 5.0
    base["api_quality_cache_ttl_sec"] = _to_float(
        base.get("api_quality_cache_ttl_sec"),
        DEFAULT_CONFIG["api_quality_cache_ttl_sec"],
        minimum=0.0,
    )
    if base["api_quality_cache_ttl_sec"] > 5.0:
        base["api_quality_cache_ttl_sec"] = 5.0
    base["enable_background_worker"] = _to_bool(
        base.get("enable_background_worker"),
        DEFAULT_CONFIG["enable_background_worker"],
    )
    base["require_apply_to_start"] = _to_bool(
        base.get("require_apply_to_start"),
        DEFAULT_CONFIG["require_apply_to_start"],
    )
    base["continuous_monitoring_mode"] = _to_bool(
        base.get("continuous_monitoring_mode"),
        DEFAULT_CONFIG["continuous_monitoring_mode"],
    )

    base["history_mode"] = _normalize_history_mode(base.get("history_mode", DEFAULT_CONFIG["history_mode"]))
    base["history_retention_hours"] = _to_int(
        base.get("history_retention_hours"),
        DEFAULT_CONFIG["history_retention_hours"],
        minimum=24,
    )

    base["tolerance_factor"] = _to_float(
        base.get("tolerance_factor"),
        DEFAULT_CONFIG["tolerance_factor"],
        minimum=1.0,
    )
    base["attention_disconnected_pct_threshold"] = _to_float(
        base.get("attention_disconnected_pct_threshold"),
        DEFAULT_CONFIG["attention_disconnected_pct_threshold"],
        minimum=0.0,
    )
    if base["attention_disconnected_pct_threshold"] > 100.0:
        base["attention_disconnected_pct_threshold"] = 100.0
    base["critical_disconnected_pct_threshold"] = _to_float(
        base.get("critical_disconnected_pct_threshold"),
        DEFAULT_CONFIG["critical_disconnected_pct_threshold"],
        minimum=base["attention_disconnected_pct_threshold"],
    )
    if base["critical_disconnected_pct_threshold"] > 100.0:
        base["critical_disconnected_pct_threshold"] = 100.0
    base["attention_disconnected_window_hours"] = _to_int(
        base.get("attention_disconnected_window_hours"),
        DEFAULT_CONFIG["attention_disconnected_window_hours"],
        minimum=1,
    )
    base["cloudv2_median_window"] = _to_int(
        base.get("cloudv2_median_window"),
        DEFAULT_CONFIG["cloudv2_median_window"],
        minimum=3,
    )
    base["cloudv2_min_samples"] = _to_int(
        base.get("cloudv2_min_samples"),
        DEFAULT_CONFIG["cloudv2_min_samples"],
        minimum=2,
    )
    if base["cloudv2_min_samples"] > base["cloudv2_median_window"]:
        base["cloudv2_min_samples"] = base["cloudv2_median_window"]

    base["dedupe_window_sec"] = _to_int(
        base.get("dedupe_window_sec"),
        DEFAULT_CONFIG["dedupe_window_sec"],
        minimum=1,
    )
    base["show_pending_ping_pivots"] = _to_bool(
        base.get("show_pending_ping_pivots"),
        DEFAULT_CONFIG["show_pending_ping_pivots"],
    )

    base["probe_min_interval_sec"] = _to_int(
        base.get("probe_min_interval_sec"),
        DEFAULT_CONFIG["probe_min_interval_sec"],
        minimum=10,
    )
    base["probe_default_interval_sec"] = _to_int(
        base.get("probe_default_interval_sec"),
        DEFAULT_CONFIG["probe_default_interval_sec"],
        minimum=base["probe_min_interval_sec"],
    )
    base["probe_timeout_factor"] = _to_float(
        base.get("probe_timeout_factor"),
        DEFAULT_CONFIG["probe_timeout_factor"],
        minimum=1.0,
    )
    base["probe_timeout_streak_alert"] = _to_int(
        base.get("probe_timeout_streak_alert"),
        DEFAULT_CONFIG["probe_timeout_streak_alert"],
        minimum=1,
    )

    base["max_events_per_pivot"] = _to_int(
        base.get("max_events_per_pivot"),
        DEFAULT_CONFIG["max_events_per_pivot"],
        minimum=100,
    )
    base["max_events_per_pivot_panel"] = _to_int(
        base.get("max_events_per_pivot_panel", base["max_events_per_pivot"]),
        base["max_events_per_pivot"],
        minimum=100,
    )
    base["max_events_per_pivot_list"] = _to_int(
        base.get("max_events_per_pivot_list", base["max_events_per_pivot_panel"]),
        base["max_events_per_pivot_panel"],
        minimum=100,
    )
    if base["max_events_per_pivot_list"] > base["max_events_per_pivot_panel"]:
        base["max_events_per_pivot_list"] = base["max_events_per_pivot_panel"]
    # Compatibilidade com codigo legado que ainda consulta max_events_per_pivot.
    base["max_events_per_pivot"] = base["max_events_per_pivot_panel"]
    base["sqlite_db_path"] = (
        str(base.get("sqlite_db_path", DEFAULT_CONFIG["sqlite_db_path"])).strip()
        or DEFAULT_CONFIG["sqlite_db_path"]
    )
    base["db_backend"] = normalize_db_backend(base.get("db_backend", DEFAULT_CONFIG["db_backend"]))
    base["database_url"] = str(base.get("database_url", DEFAULT_CONFIG["database_url"])).strip()
    if base["db_backend"] == "postgres" and not base["database_url"]:
        raise ValueError("database_url obrigatorio quando db_backend=postgres")

    base["filter_names"] = _normalize_string_list(base.get("filter_names"))
    base["cmd_topics"] = _normalize_string_list(base.get("cmd_topics"))

    base["topics"] = list(FIXED_MONITOR_TOPICS)
    base["monitor_topics"] = list(FIXED_MONITOR_TOPICS)
    base["info_topics"] = list(PROBE_RESPONSE_TOPICS)
    base["ping_topic"] = "cloudv2-ping"

    raw_probe_settings = base.get("probe_settings")
    probe_settings = _normalize_probe_settings(
        raw_probe_settings,
        default_interval=base["probe_default_interval_sec"],
        minimum_interval=base["probe_min_interval_sec"],
    )

    if not probe_settings and base["cmd_topics"]:
        for pivot_id in base["cmd_topics"]:
            probe_settings[pivot_id] = {
                "enabled": True,
                "interval_sec": base["probe_default_interval_sec"],
            }

    base["probe_settings"] = probe_settings
    return base


def load_editable_config(config_path=None):
    path = config_path or get_config_file_path()
    loaded = _read_config_file(path)
    return normalize_config(loaded)


def load_runtime_config(config_path=None):
    path = config_path or get_config_file_path()
    loaded = _read_config_file(path)
    _apply_env_overrides(loaded)
    return normalize_config(loaded)


def save_config(config, config_path=None):
    path = config_path or get_config_file_path()
    normalized = normalize_config(config)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(normalized, file, indent=2, ensure_ascii=False)
    return normalized
