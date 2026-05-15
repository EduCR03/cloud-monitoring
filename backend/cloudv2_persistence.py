import json
import os
import re
import statistics
import threading
import time
import uuid

from backend.cloudv2_db import connect_database, resolve_database_settings
from backend.cloudv2_paths import resolve_data_dir
from backend.cloudv2_dashboard import slugify
from backend.cloudv2_time import ts_to_dashboard_str


DEFAULT_DB_PATH = os.path.join(resolve_data_dir(), "telemetry.sqlite3")
DEFAULT_MIGRATIONS_SQLITE_DIR = os.path.join(os.path.dirname(__file__), "migrations")
DEFAULT_MIGRATIONS_POSTGRES_DIR = os.path.join(os.path.dirname(__file__), "migrations_postgres")
PROBE_STATS_WINDOW_SEC = 30 * 24 * 3600
TIMELINE_MINI_BINS = 96
TIMELINE_MINI_DEFAULT_WINDOW_SEC = 30 * 24 * 3600
TIMELINE_MINI_EMPTY_FALLBACK_SEC = 24 * 3600
CONNECTIVITY_TOPICS = ("cloudv2", "cloudv2-ping", "cloudv2-info", "cloudv2-network")
SUMMARY_CARDS_WINDOW_SEC = 30 * 24 * 3600
SUMMARY_CARDS_BUCKET_SEC = 3600
CONNECTED_PIVOTS_WINDOW_SEC = SUMMARY_CARDS_WINDOW_SEC
CONNECTED_PIVOTS_BUCKET_SEC = SUMMARY_CARDS_BUCKET_SEC
SUMMARY_CARD_HISTORY_FIELDS = (
    "total_count",
    "connected_count",
    "disconnected_count",
    "initial_count",
    "quality_green_count",
    "quality_calculating_count",
    "quality_yellow_count",
    "quality_critical_count",
)


def _ts_to_str(ts):
    return ts_to_dashboard_str(ts)


def _safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _row_get(row, key, default=None):
    if row is None:
        return default
    try:
        if isinstance(row, dict):
            return row.get(key, default)
        return row[key]
    except Exception:
        try:
            return getattr(row, key)
        except Exception:
            return default


def _safe_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_bool(value, default=None):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "sim"}:
            return True
        if normalized in {"0", "false", "no", "n", "nao", "não"}:
            return False
    return default


def _normalize_text(value):
    return str(value or "").strip()


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


def _normalize_probe_summary_timestamps(probe):
    if not isinstance(probe, dict):
        return probe
    _refresh_named_timestamp_fields(
        probe,
        (
            ("last_sent_ts", "last_sent_at"),
            ("last_response_ts", "last_response_at"),
            ("pending_deadline_ts", "pending_deadline_at"),
        ),
    )
    last_response_info = probe.get("last_response_info")
    if isinstance(last_response_info, dict):
        board_timestamp_ts = _safe_float(last_response_info.get("board_timestamp_ts"), None)
        last_response_info["board_timestamp_at"] = _ts_to_str(board_timestamp_ts)
    return probe


def _normalize_modem_reset_timestamps(modem_reset):
    if not isinstance(modem_reset, dict):
        return modem_reset
    _refresh_named_timestamp_fields(
        modem_reset,
        (
            ("last_command_ts", "last_command_at"),
            ("last_ack_ts", "last_ack_at"),
        ),
    )
    return modem_reset


def _normalize_summary_timestamps(summary):
    if not isinstance(summary, dict):
        return summary
    _refresh_named_timestamp_fields(
        summary,
        (
            ("last_monitored_message_ts", "last_monitored_message_at"),
            ("last_ping_ts", "last_ping_at"),
            ("last_cloudv2_ts", "last_cloudv2_at"),
            ("last_activity_ts", "last_activity_at"),
        ),
    )
    last_cloud2 = summary.get("last_cloud2")
    if isinstance(last_cloud2, dict):
        _refresh_timestamp_field(last_cloud2)
    _normalize_probe_summary_timestamps(summary.get("probe"))
    _normalize_modem_reset_timestamps(summary.get("modem_reset"))
    return summary


def _normalize_snapshot_payload_timestamps(payload):
    if not isinstance(payload, dict):
        return payload
    updated_at_ts = _safe_float(payload.get("updated_at_ts"), None)
    if updated_at_ts is not None:
        payload["updated_at"] = _ts_to_str(updated_at_ts)
    _normalize_summary_timestamps(payload.get("summary"))
    metrics = payload.get("metrics")
    if isinstance(metrics, dict):
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        last_cloud2 = summary.get("last_cloud2") if isinstance(summary.get("last_cloud2"), dict) else {}
        if isinstance(last_cloud2, dict):
            metrics["last_cloud2_at"] = _ts_to_str(_safe_float(last_cloud2.get("ts"), None))
    return payload


def _overlay_probe_setting_on_summary(summary, probe_setting):
    if not isinstance(summary, dict) or not isinstance(probe_setting, dict):
        return summary

    probe = summary.get("probe") if isinstance(summary.get("probe"), dict) else {}

    enabled = _safe_bool(probe_setting.get("enabled"), None)
    if enabled is not None:
        probe["enabled"] = enabled

    interval_sec = _safe_int(probe_setting.get("interval_sec"), None)
    if interval_sec is not None and interval_sec > 0:
        probe["interval_sec"] = interval_sec

    summary["probe"] = probe
    return summary


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


def _ensure_summary_signal_fields(item):
    if not isinstance(item, dict):
        return item

    last_cloud2 = item.get("last_cloud2") if isinstance(item.get("last_cloud2"), dict) else {}
    signal = _normalize_text(item.get("signal"))
    technology = _normalize_text(item.get("technology"))
    combined = _normalize_text(item.get("signal_technology"))

    if not signal:
        signal = _normalize_text(last_cloud2.get("rssi"))
    if not technology:
        technology = _normalize_text(last_cloud2.get("technology"))

    if combined:
        parsed_signal, parsed_technology = _parse_signal_technology_combined(combined)
        if not signal and parsed_signal:
            signal = parsed_signal
        if not technology and parsed_technology:
            technology = parsed_technology

    if signal and not _normalize_text(last_cloud2.get("rssi")):
        last_cloud2["rssi"] = signal
    if technology and not _normalize_text(last_cloud2.get("technology")):
        last_cloud2["technology"] = technology

    item["last_cloud2"] = last_cloud2
    item["signal"] = signal
    item["technology"] = technology
    item["signal_technology"] = f"{signal or '-'} / {technology or '-'}"
    return item


def _normalize_timeline_mini_segments(raw_segments):
    if not isinstance(raw_segments, list):
        return []

    cleaned = []
    for segment in raw_segments:
        if not isinstance(segment, dict):
            continue
        state = _normalize_text(segment.get("state")).lower()
        if state not in ("online", "offline"):
            continue
        ratio = _safe_float(segment.get("ratio"), None)
        if ratio is None or ratio <= 0:
            continue
        cleaned.append({"state": state, "ratio": max(0.0, ratio)})
        if len(cleaned) >= TIMELINE_MINI_BINS:
            break

    if not cleaned:
        return []

    total = sum(segment["ratio"] for segment in cleaned)
    if total <= 0:
        return []

    merged = []
    for segment in cleaned:
        normalized_ratio = segment["ratio"] / total
        if merged and merged[-1]["state"] == segment["state"]:
            merged[-1]["ratio"] += normalized_ratio
        else:
            merged.append({"state": segment["state"], "ratio": normalized_ratio})

    for segment in merged:
        segment["ratio"] = round(segment["ratio"], 6)
    return merged


def _build_timeline_mini_segments(events, window_end_ts, window_sec, disconnect_threshold_sec):
    end_ts = _safe_float(window_end_ts, None)
    safe_window_sec = _safe_float(window_sec, None)
    threshold_sec = _safe_float(disconnect_threshold_sec, None)
    if end_ts is None or safe_window_sec is None or safe_window_sec <= 0 or threshold_sec is None or threshold_sec <= 0:
        return []

    min_timeline_ts = None
    for event in events or []:
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
    for event in events or []:
        if not isinstance(event, dict):
            continue
        topic = _normalize_text(event.get("topic"))
        if topic not in CONNECTIVITY_TOPICS:
            continue
        ts_value = _safe_float(event.get("ts"), None)
        if ts_value is None:
            continue
        if ts_value < min_relevant_ts or ts_value > end_ts:
            continue
        message_ts.append(ts_value)

    total_duration = end_ts - start_ts
    if total_duration <= 0:
        return []

    if not message_ts:
        return [{"state": "offline", "ratio": 1.0}]

    message_ts = sorted(set(message_ts))
    online_intervals = []
    current_start = None
    current_end = None
    for ts_value in message_ts:
        interval_start = max(start_ts, ts_value)
        interval_end = min(end_ts, ts_value + threshold_sec)
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
    for state in states[1:]:
        if state == current_state:
            current_count += 1
            continue
        compressed.append({"state": current_state, "ratio": current_count / float(TIMELINE_MINI_BINS)})
        current_state = state
        current_count = 1
    compressed.append({"state": current_state, "ratio": current_count / float(TIMELINE_MINI_BINS)})

    for segment in compressed:
        segment["ratio"] = round(segment["ratio"], 6)
    return compressed


def _resolve_timeline_disconnect_threshold(summary, settings=None):
    safe_summary = summary if isinstance(summary, dict) else {}
    safe_settings = settings if isinstance(settings, dict) else {}

    threshold = _safe_float(safe_summary.get("disconnect_threshold_sec"), None)
    if threshold is not None and threshold > 0:
        return threshold

    tolerance = _safe_float(safe_settings.get("tolerance_factor"), None)
    if tolerance is None or tolerance <= 0:
        tolerance = 3.0

    max_expected = _safe_float(safe_summary.get("max_expected_interval_sec"), None)
    if max_expected is None or max_expected <= 0:
        expected_by_topic = safe_summary.get("expected_by_topic_sec") if isinstance(safe_summary.get("expected_by_topic_sec"), dict) else {}
        candidates = []
        for topic in CONNECTIVITY_TOPICS:
            value = _safe_float(expected_by_topic.get(topic), None)
            if value is not None and value > 0:
                candidates.append(value)
        if candidates:
            max_expected = max(candidates)

    if max_expected is None or max_expected <= 0:
        max_expected = _safe_float(safe_settings.get("ping_expected_sec"), None)
    if max_expected is None or max_expected <= 0:
        max_expected = 180.0

    return max(30.0, max_expected * tolerance)


class TelemetryPersistence:
    def __init__(
        self,
        db_path=None,
        db_backend="sqlite",
        database_url=None,
        migrations_dir=None,
        max_events_per_pivot=5000,
        log=None,
    ):
        self.db_settings = resolve_database_settings(
            {
                "db_backend": db_backend,
                "sqlite_db_path": str(db_path or DEFAULT_DB_PATH),
                "database_url": database_url,
            }
        )
        self.db_backend = self.db_settings.backend
        self.db_path = self.db_settings.sqlite_db_path
        self.database_url = self.db_settings.database_url
        self.migrations_dir = str(migrations_dir or self._default_migrations_dir())
        self.max_events_per_pivot = max(100, int(max_events_per_pivot or 5000))
        self.log = log

        self._lock = threading.RLock()
        self._conn = None

    def start(self):
        with self._lock:
            if self._conn is not None:
                return

            conn = connect_database(self.db_settings, auth_mode=False)
            self._conn = conn
            self._ensure_migrations_table_locked()
            self._apply_migrations_locked()

    def stop(self):
        with self._lock:
            if self._conn is None:
                return
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _require_conn_locked(self):
        if self._conn is None:
            raise RuntimeError("Persistence not started")
        return self._conn

    def _default_migrations_dir(self):
        if self.db_backend == "postgres":
            return DEFAULT_MIGRATIONS_POSTGRES_DIR
        return DEFAULT_MIGRATIONS_SQLITE_DIR

    def _ensure_migrations_table_locked(self):
        conn = self._require_conn_locked()
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at_ts REAL NOT NULL
                )
                """
            )

    def _iter_migration_files_locked(self):
        if not os.path.isdir(self.migrations_dir):
            if self.db_backend == "postgres":
                raise RuntimeError(f"Diretorio de migrations PostgreSQL ausente: {self.migrations_dir}")
            return []

        entries = []
        for filename in os.listdir(self.migrations_dir):
            if not filename.lower().endswith(".sql"):
                continue

            match = re.match(r"^(\d+)[_-](.+)\.sql$", filename)
            if not match:
                continue

            version = int(match.group(1))
            name = match.group(2).replace("_", " ")
            path = os.path.join(self.migrations_dir, filename)
            entries.append((version, name, path))

        entries.sort(key=lambda item: item[0])
        return entries

    def _apply_migrations_locked(self):
        conn = self._require_conn_locked()
        rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
        applied = {int(row["version"]) for row in rows}

        pending = [item for item in self._iter_migration_files_locked() if item[0] not in applied]
        if not pending:
            return

        for version, name, path in pending:
            with open(path, "r", encoding="utf-8") as file:
                script = file.read()
            with conn:
                self._run_migration_script_locked(conn, script)
                self._insert_schema_migration_locked(conn, version, name, time.time())
            if self.log is not None:
                self.log.info("Migration aplicada: v%s (%s)", version, name)

    def _run_migration_script_locked(self, conn, script):
        safe_script = str(script or "").strip()
        if not safe_script:
            return
        if self.db_backend == "postgres":
            with conn.cursor() as cursor:
                cursor.execute(safe_script)
            return
        conn.executescript(safe_script)

    def _insert_schema_migration_locked(self, conn, version, name, applied_at_ts):
        if self.db_backend == "postgres":
            conn.execute(
                "INSERT INTO schema_migrations(version, name, applied_at_ts) VALUES (%s, %s, %s)",
                (int(version), str(name), float(applied_at_ts)),
            )
            return
        conn.execute(
            "INSERT INTO schema_migrations(version, name, applied_at_ts) VALUES (?, ?, ?)",
            (int(version), str(name), float(applied_at_ts)),
        )

    def _upsert_pivot_locked(self, conn, pivot_id, pivot_slug, seen_ts=None):
        now_ts = time.time()
        seen_value = _safe_float(seen_ts, None)

        with conn:
            conn.execute(
                """
                INSERT INTO pivots (
                    pivot_id,
                    pivot_slug,
                    first_seen_ts,
                    last_seen_ts,
                    created_at_ts,
                    updated_at_ts
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(pivot_id) DO UPDATE SET
                    pivot_slug = excluded.pivot_slug,
                    first_seen_ts = CASE
                        WHEN pivots.first_seen_ts IS NULL THEN excluded.first_seen_ts
                        WHEN excluded.first_seen_ts IS NULL THEN pivots.first_seen_ts
                        WHEN excluded.first_seen_ts < pivots.first_seen_ts THEN excluded.first_seen_ts
                        ELSE pivots.first_seen_ts
                    END,
                    last_seen_ts = CASE
                        WHEN pivots.last_seen_ts IS NULL THEN excluded.last_seen_ts
                        WHEN excluded.last_seen_ts IS NULL THEN pivots.last_seen_ts
                        WHEN excluded.last_seen_ts > pivots.last_seen_ts THEN excluded.last_seen_ts
                        ELSE pivots.last_seen_ts
                    END,
                    updated_at_ts = excluded.updated_at_ts
                """,
                (
                    str(pivot_id),
                    str(pivot_slug),
                    seen_value,
                    seen_value,
                    now_ts,
                    now_ts,
                ),
            )

    def ensure_pivot(self, pivot_id, pivot_slug=None, seen_ts=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return
        normalized_slug = str(pivot_slug or slugify(normalized_id))
        with self._lock:
            conn = self._require_conn_locked()
            self._upsert_pivot_locked(conn, normalized_id, normalized_slug, seen_ts)

    def pivot_exists(self, pivot_id):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return False

        with self._lock:
            conn = self._require_conn_locked()
            row = conn.execute(
                """
                SELECT 1
                FROM pivots
                WHERE pivot_id = ?
                LIMIT 1
                """,
                (normalized_id,),
            ).fetchone()
        return row is not None

    def touch_pivot_seen(self, pivot_id, seen_ts):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return
        seen_value = _safe_float(seen_ts, None)
        if seen_value is None:
            seen_value = time.time()
        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    UPDATE pivots
                    SET
                        last_seen_ts = CASE
                            WHEN last_seen_ts IS NULL THEN ?
                            WHEN ? > last_seen_ts THEN ?
                            ELSE last_seen_ts
                        END,
                        updated_at_ts = ?
                    WHERE pivot_id = ?
                    """,
                    (
                        seen_value,
                        seen_value,
                        seen_value,
                        time.time(),
                        normalized_id,
                    ),
                )

    def record_summary_cards_hourly(self, ts, counts):
        bucket_ts = _safe_int(ts, None)
        if bucket_ts is None:
            bucket_ts = int(time.time())
        bucket_ts = int(bucket_ts // SUMMARY_CARDS_BUCKET_SEC) * SUMMARY_CARDS_BUCKET_SEC
        raw_counts = counts if isinstance(counts, dict) else {}
        safe_counts = {
            field: max(0, int(_safe_int(raw_counts.get(field), 0) or 0))
            for field in SUMMARY_CARD_HISTORY_FIELDS
        }
        status_total = (
            safe_counts["connected_count"]
            + safe_counts["disconnected_count"]
            + safe_counts["initial_count"]
        )
        quality_total = (
            safe_counts["quality_green_count"]
            + safe_counts["quality_calculating_count"]
            + safe_counts["quality_yellow_count"]
            + safe_counts["quality_critical_count"]
        )
        safe_counts["total_count"] = max(safe_counts["total_count"], status_total, quality_total)
        now_ts = time.time()
        cutoff_ts = bucket_ts - SUMMARY_CARDS_WINDOW_SEC

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO summary_cards_hourly (
                        bucket_ts,
                        total_count,
                        connected_count,
                        disconnected_count,
                        initial_count,
                        quality_green_count,
                        quality_calculating_count,
                        quality_yellow_count,
                        quality_critical_count,
                        created_at_ts,
                        updated_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(bucket_ts) DO UPDATE SET
                        total_count = excluded.total_count,
                        connected_count = excluded.connected_count,
                        disconnected_count = excluded.disconnected_count,
                        initial_count = excluded.initial_count,
                        quality_green_count = excluded.quality_green_count,
                        quality_calculating_count = excluded.quality_calculating_count,
                        quality_yellow_count = excluded.quality_yellow_count,
                        quality_critical_count = excluded.quality_critical_count,
                        updated_at_ts = excluded.updated_at_ts
                    """,
                    (
                        bucket_ts,
                        safe_counts["total_count"],
                        safe_counts["connected_count"],
                        safe_counts["disconnected_count"],
                        safe_counts["initial_count"],
                        safe_counts["quality_green_count"],
                        safe_counts["quality_calculating_count"],
                        safe_counts["quality_yellow_count"],
                        safe_counts["quality_critical_count"],
                        now_ts,
                        now_ts,
                    ),
                )
                conn.execute(
                    """
                    DELETE FROM summary_cards_hourly
                    WHERE bucket_ts < ?
                    """,
                    (cutoff_ts,),
                )

    def fetch_summary_cards_hourly_history(self, now=None, window_sec=SUMMARY_CARDS_WINDOW_SEC, limit=None):
        safe_now = _safe_float(now, time.time())
        safe_window = max(
            SUMMARY_CARDS_BUCKET_SEC,
            int(_safe_int(window_sec, SUMMARY_CARDS_WINDOW_SEC) or SUMMARY_CARDS_WINDOW_SEC),
        )
        safe_limit = limit
        if safe_limit is None:
            safe_limit = max(24, int(safe_window // SUMMARY_CARDS_BUCKET_SEC))
        safe_limit = max(1, int(_safe_int(safe_limit, 720) or 720))
        cutoff_ts = int(safe_now) - safe_window

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT
                    bucket_ts,
                    total_count,
                    connected_count,
                    disconnected_count,
                    initial_count,
                    quality_green_count,
                    quality_calculating_count,
                    quality_yellow_count,
                    quality_critical_count
                FROM summary_cards_hourly
                WHERE bucket_ts >= ?
                ORDER BY bucket_ts DESC
                LIMIT ?
                """,
                (cutoff_ts, safe_limit),
            ).fetchall()

        points = []
        for row in reversed(rows):
            bucket_ts = _safe_int(row["bucket_ts"], None)
            if bucket_ts is None:
                continue
            item = {
                "ts": bucket_ts,
                "at": _ts_to_str(bucket_ts),
            }
            for field in SUMMARY_CARD_HISTORY_FIELDS:
                item[field] = max(0, int(_safe_int(row[field], 0) or 0))
            status_total = (
                item["connected_count"]
                + item["disconnected_count"]
                + item["initial_count"]
            )
            quality_total = (
                item["quality_green_count"]
                + item["quality_calculating_count"]
                + item["quality_yellow_count"]
                + item["quality_critical_count"]
            )
            item["total_count"] = max(item["total_count"], status_total, quality_total)
            points.append(item)
        return points

    def record_connected_pivots_hourly(self, ts, connected_count, total_count):
        safe_connected = max(0, int(_safe_int(connected_count, 0) or 0))
        safe_total = max(safe_connected, int(_safe_int(total_count, 0) or 0))
        self.record_summary_cards_hourly(
            ts,
            {
                "total_count": safe_total,
                "connected_count": safe_connected,
                "disconnected_count": max(0, safe_total - safe_connected),
                "initial_count": 0,
                "quality_green_count": 0,
                "quality_calculating_count": 0,
                "quality_yellow_count": 0,
                "quality_critical_count": 0,
            },
        )

    def fetch_connected_pivots_hourly_history(self, now=None, window_sec=CONNECTED_PIVOTS_WINDOW_SEC, limit=None):
        points = self.fetch_summary_cards_hourly_history(
            now=now,
            window_sec=window_sec,
            limit=limit,
        )
        return [
            {
                "ts": item["ts"],
                "at": item["at"],
                "connected_count": item["connected_count"],
                "total_count": item["total_count"],
            }
            for item in points
        ]

    def set_pivot_is_concentrator(self, pivot_id, is_concentrator, pivot_slug=None, seen_ts=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return False

        normalized_slug = str(pivot_slug or slugify(normalized_id))
        normalized_flag = 1 if bool(is_concentrator) else 0
        with self._lock:
            conn = self._require_conn_locked()
            self._upsert_pivot_locked(conn, normalized_id, normalized_slug, seen_ts=seen_ts)
            with conn:
                conn.execute(
                    """
                    UPDATE pivots
                    SET
                        is_concentrator = ?,
                        updated_at_ts = ?
                    WHERE pivot_id = ?
                    """,
                    (normalized_flag, time.time(), normalized_id),
                )
        return bool(normalized_flag)

    def get_pivot_is_concentrator(self, pivot_id):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return False

        with self._lock:
            conn = self._require_conn_locked()
            row = conn.execute(
                """
                SELECT is_concentrator
                FROM pivots
                WHERE pivot_id = ?
                LIMIT 1
                """,
                (normalized_id,),
            ).fetchone()
        if row is None:
            return False
        parsed = _safe_bool(row["is_concentrator"], None)
        if parsed is None:
            parsed = bool(_safe_int(row["is_concentrator"], 0) or 0)
        return bool(parsed)

    def set_pivot_coordinates(self, pivot_id, latitude, longitude, pivot_slug=None, seen_ts=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return {"latitude": None, "longitude": None}

        normalized_slug = str(pivot_slug or slugify(normalized_id))
        lat_value = _safe_float(latitude, None)
        lon_value = _safe_float(longitude, None)
        with self._lock:
            conn = self._require_conn_locked()
            self._upsert_pivot_locked(conn, normalized_id, normalized_slug, seen_ts=seen_ts)
            with conn:
                conn.execute(
                    """
                    UPDATE pivots
                    SET
                        latitude = ?,
                        longitude = ?,
                        updated_at_ts = ?
                    WHERE pivot_id = ?
                    """,
                    (lat_value, lon_value, time.time(), normalized_id),
                )
        return {"latitude": lat_value, "longitude": lon_value}

    def get_pivot_coordinates(self, pivot_id):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return {"latitude": None, "longitude": None}

        with self._lock:
            conn = self._require_conn_locked()
            row = conn.execute(
                """
                SELECT latitude, longitude
                FROM pivots
                WHERE pivot_id = ?
                LIMIT 1
                """,
                (normalized_id,),
            ).fetchone()
        if row is None:
            return {"latitude": None, "longitude": None}
        return {
            "latitude": _safe_float(row["latitude"], None),
            "longitude": _safe_float(row["longitude"], None),
        }

    def _query_run_row_locked(self, conn, run_id=None):
        normalized_run = str(run_id or "").strip()
        if normalized_run:
            return conn.execute(
                """
                SELECT *
                FROM monitoring_runs
                WHERE run_id = ?
                LIMIT 1
                """,
                (normalized_run,),
            ).fetchone()

        active_row = conn.execute(
            """
            SELECT *
            FROM monitoring_runs
            WHERE is_active = 1
            ORDER BY updated_at_ts DESC, started_at_ts DESC
            LIMIT 1
            """
        ).fetchone()
        if active_row is not None:
            return active_row

        return conn.execute(
            """
            SELECT *
            FROM monitoring_runs
            ORDER BY started_at_ts DESC, updated_at_ts DESC
            LIMIT 1
            """
        ).fetchone()

    def _row_to_run_dict_locked(self, row, now_ts=None):
        if row is None:
            return None

        now_value = _safe_float(now_ts, None)
        if now_value is None:
            now_value = time.time()

        started_at_ts = _safe_float(row["started_at_ts"], None)
        ended_at_ts = _safe_float(row["ended_at_ts"], None)
        updated_at_ts = _safe_float(row["updated_at_ts"], None)
        duration_anchor = ended_at_ts if ended_at_ts is not None else now_value
        duration_sec = None
        if started_at_ts is not None:
            duration_sec = max(0.0, duration_anchor - started_at_ts)

        return {
            "run_id": str(row["run_id"]),
            "started_at_ts": started_at_ts,
            "started_at": _ts_to_str(started_at_ts),
            "ended_at_ts": ended_at_ts,
            "ended_at": _ts_to_str(ended_at_ts),
            "updated_at_ts": updated_at_ts,
            "updated_at": _ts_to_str(updated_at_ts),
            "is_active": bool(int(row["is_active"])),
            "source": str(row["source"] or ""),
            "label": str(row["label"] or ""),
            "duration_sec": duration_sec,
        }

    def resolve_run(self, run_id=None):
        with self._lock:
            conn = self._require_conn_locked()
            row = self._query_run_row_locked(conn, run_id=run_id)
            return self._row_to_run_dict_locked(row)

    def get_or_create_active_run(self, now_ts=None, source="runtime", label="", metadata=None):
        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            row = self._query_run_row_locked(conn, run_id=None)
            if row is not None and bool(int(row["is_active"])):
                return self._row_to_run_dict_locked(row, now_ts=current_ts)

            run_id = str(uuid.uuid4())
            metadata_payload = metadata if isinstance(metadata, dict) else {}
            with conn:
                conn.execute(
                    """
                    INSERT INTO monitoring_runs (
                        run_id,
                        started_at_ts,
                        ended_at_ts,
                        is_active,
                        source,
                        label,
                        metadata_json,
                        created_at_ts,
                        updated_at_ts
                    ) VALUES (?, ?, NULL, 1, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        current_ts,
                        str(source or "runtime"),
                        str(label or ""),
                        json.dumps(metadata_payload, ensure_ascii=False),
                        current_ts,
                        current_ts,
                    ),
                )

            created = conn.execute(
                "SELECT * FROM monitoring_runs WHERE run_id = ? LIMIT 1",
                (run_id,),
            ).fetchone()
            return self._row_to_run_dict_locked(created, now_ts=current_ts)

    def create_new_run(self, now_ts=None, source="ui", label="", metadata=None):
        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            run_id = str(uuid.uuid4())
            metadata_payload = metadata if isinstance(metadata, dict) else {}
            with conn:
                conn.execute(
                    """
                    UPDATE monitoring_runs
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE is_active = 1
                    """,
                    (current_ts, current_ts),
                )
                conn.execute(
                    """
                    INSERT INTO monitoring_runs (
                        run_id,
                        started_at_ts,
                        ended_at_ts,
                        is_active,
                        source,
                        label,
                        metadata_json,
                        created_at_ts,
                        updated_at_ts
                    ) VALUES (?, ?, NULL, 1, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        current_ts,
                        str(source or "ui"),
                        str(label or ""),
                        json.dumps(metadata_payload, ensure_ascii=False),
                        current_ts,
                        current_ts,
                    ),
                )

            row = conn.execute(
                "SELECT * FROM monitoring_runs WHERE run_id = ? LIMIT 1",
                (run_id,),
            ).fetchone()
            return self._row_to_run_dict_locked(row, now_ts=current_ts)

    def activate_existing_run(self, run_id, now_ts=None):
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return None

        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            run_row = self._query_run_row_locked(conn, run_id=normalized_run_id)
            if run_row is None:
                return None

            with conn:
                conn.execute(
                    """
                    UPDATE monitoring_runs
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE is_active = 1
                        AND run_id <> ?
                    """,
                    (current_ts, current_ts, normalized_run_id),
                )
                conn.execute(
                    """
                    UPDATE monitoring_runs
                    SET
                        is_active = 1,
                        ended_at_ts = NULL,
                        updated_at_ts = ?
                    WHERE run_id = ?
                    """,
                    (current_ts, normalized_run_id),
                )

            row = conn.execute(
                "SELECT * FROM monitoring_runs WHERE run_id = ? LIMIT 1",
                (normalized_run_id,),
            ).fetchone()
            return self._row_to_run_dict_locked(row, now_ts=current_ts)

    def activate_latest_sessions_for_run(self, run_id, now_ts=None):
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return {}

        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            run_row = self._query_run_row_locked(conn, run_id=normalized_run_id)
            if run_row is None:
                return {}

            with conn:
                conn.execute(
                    """
                    UPDATE monitoring_sessions
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE is_active = 1
                    """,
                    (current_ts, current_ts),
                )

                latest_rows = conn.execute(
                    """
                    SELECT s.session_id
                    FROM monitoring_sessions AS s
                    WHERE s.run_id = ?
                        AND s.session_id = (
                            SELECT s2.session_id
                            FROM monitoring_sessions AS s2
                            WHERE s2.run_id = s.run_id
                                AND s2.pivot_id = s.pivot_id
                            ORDER BY s2.updated_at_ts DESC, s2.started_at_ts DESC
                            LIMIT 1
                        )
                    GROUP BY s.pivot_id
                    """,
                    (normalized_run_id,),
                ).fetchall()

                for row in latest_rows:
                    session_id = str(row["session_id"] or "").strip()
                    if not session_id:
                        continue
                    conn.execute(
                        """
                        UPDATE monitoring_sessions
                        SET
                            is_active = 1,
                            ended_at_ts = NULL,
                            updated_at_ts = ?
                        WHERE session_id = ?
                        """,
                        (current_ts, session_id),
                    )

            rows = conn.execute(
                """
                SELECT pivot_id, session_id
                FROM monitoring_sessions
                WHERE run_id = ?
                    AND is_active = 1
                ORDER BY updated_at_ts DESC, started_at_ts DESC
                """,
                (normalized_run_id,),
            ).fetchall()

            active_by_pivot = {}
            for row in rows:
                pivot_id = str(row["pivot_id"] or "").strip()
                session_id = str(row["session_id"] or "").strip()
                if not pivot_id or not session_id:
                    continue
                if pivot_id not in active_by_pivot:
                    active_by_pivot[pivot_id] = session_id
            return active_by_pivot

    def list_runs(self, limit=200):
        safe_limit = max(1, min(1000, int(limit or 200)))
        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT
                    runs.*,
                    COUNT(sessions.session_id) AS session_count,
                    COUNT(DISTINCT sessions.pivot_id) AS pivot_count
                FROM monitoring_runs AS runs
                LEFT JOIN monitoring_sessions AS sessions
                    ON sessions.run_id = runs.run_id
                GROUP BY runs.run_id
                ORDER BY runs.started_at_ts DESC, runs.updated_at_ts DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()

            items = []
            for row in rows:
                item = self._row_to_run_dict_locked(row)
                if item is None:
                    continue
                item["session_count"] = int(row["session_count"] or 0)
                item["pivot_count"] = int(row["pivot_count"] or 0)
                items.append(item)
            return items

    def _resolve_effective_run_id_locked(self, conn, run_id=None):
        normalized_run = str(run_id or "").strip()
        if normalized_run:
            return normalized_run
        run_row = self._query_run_row_locked(conn, run_id=None)
        if run_row is None:
            return None
        resolved = str(run_row["run_id"] or "").strip()
        return resolved or None

    def _list_distinct_cloud2_column_locked(self, conn, column_name, run_id=None, limit=500):
        if column_name not in ("technology", "firmware"):
            return []

        safe_limit = max(1, min(2000, int(limit or 500)))
        normalized_run = str(run_id or "").strip()
        if normalized_run:
            rows = conn.execute(
                f"""
                SELECT DISTINCT TRIM(cloud2.{column_name}) AS value
                FROM cloud2_events AS cloud2
                INNER JOIN monitoring_sessions AS sessions
                    ON sessions.session_id = cloud2.session_id
                WHERE sessions.run_id = ?
                    AND cloud2.{column_name} IS NOT NULL
                    AND TRIM(cloud2.{column_name}) <> ''
                ORDER BY value COLLATE NOCASE ASC
                LIMIT ?
                """,
                (normalized_run, safe_limit),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT DISTINCT TRIM({column_name}) AS value
                FROM cloud2_events
                WHERE {column_name} IS NOT NULL
                    AND TRIM({column_name}) <> ''
                ORDER BY value COLLATE NOCASE ASC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()

        values = []
        for row in rows:
            value = str(row["value"] or "").strip()
            if value:
                values.append(value)
        return values

    def _run_has_concentrator_pivots_locked(self, conn, run_id=None):
        normalized_run = str(run_id or "").strip()
        if normalized_run:
            row = conn.execute(
                """
                SELECT 1
                FROM pivots AS pivots
                INNER JOIN monitoring_sessions AS sessions
                    ON sessions.pivot_id = pivots.pivot_id
                WHERE sessions.run_id = ?
                    AND COALESCE(pivots.is_concentrator, 0) = 1
                LIMIT 1
                """,
                (normalized_run,),
            ).fetchone()
            return row is not None

        row = conn.execute(
            """
            SELECT 1
            FROM pivots
            WHERE COALESCE(is_concentrator, 0) = 1
            LIMIT 1
            """
        ).fetchone()
        return row is not None

    def get_cloud2_filter_options(self, run_id=None, limit=500):
        with self._lock:
            conn = self._require_conn_locked()
            effective_run = self._resolve_effective_run_id_locked(conn, run_id=run_id)
            technologies = self._list_distinct_cloud2_column_locked(
                conn,
                "technology",
                run_id=effective_run,
                limit=limit,
            )
            firmwares = self._list_distinct_cloud2_column_locked(
                conn,
                "firmware",
                run_id=effective_run,
                limit=limit,
            )
            if self._run_has_concentrator_pivots_locked(conn, run_id=effective_run):
                known_keys = {str(item or "").strip().lower() for item in technologies}
                if "concentrador" not in known_keys:
                    technologies.append("concentrador")
                    technologies.sort(key=lambda value: str(value).lower())
            return {
                "run_id": effective_run,
                "technologies": technologies,
                "firmwares": firmwares,
            }

    def _query_session_row_locked(self, conn, pivot_id, session_id=None, run_id=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return None

        normalized_run_id = str(run_id or "").strip()
        if session_id:
            normalized_session = str(session_id).strip()
            if not normalized_session:
                return None
            query = """
                SELECT *
                FROM monitoring_sessions
                WHERE pivot_id = ? AND session_id = ?
            """
            params = [normalized_id, normalized_session]
            if normalized_run_id:
                query += " AND run_id = ?"
                params.append(normalized_run_id)
            query += " LIMIT 1"
            return conn.execute(query, tuple(params)).fetchone()

        active_query = """
            SELECT *
            FROM monitoring_sessions
            WHERE pivot_id = ? AND is_active = 1
        """
        active_params = [normalized_id]
        if normalized_run_id:
            active_query += " AND run_id = ?"
            active_params.append(normalized_run_id)
        active_query += " ORDER BY updated_at_ts DESC, started_at_ts DESC LIMIT 1"

        active_row = conn.execute(active_query, tuple(active_params)).fetchone()
        if active_row is not None:
            return active_row

        latest_query = """
            SELECT *
            FROM monitoring_sessions
            WHERE pivot_id = ?
        """
        latest_params = [normalized_id]
        if normalized_run_id:
            latest_query += " AND run_id = ?"
            latest_params.append(normalized_run_id)
        latest_query += " ORDER BY started_at_ts DESC, updated_at_ts DESC LIMIT 1"
        return conn.execute(latest_query, tuple(latest_params)).fetchone()

    def _row_to_session_dict_locked(self, row, now_ts=None):
        if row is None:
            return None

        now_value = _safe_float(now_ts, None)
        if now_value is None:
            now_value = time.time()

        started_at_ts = _safe_float(row["started_at_ts"], None)
        ended_at_ts = _safe_float(row["ended_at_ts"], None)
        updated_at_ts = _safe_float(row["updated_at_ts"], None)
        duration_anchor = ended_at_ts if ended_at_ts is not None else now_value
        duration_sec = None
        if started_at_ts is not None:
            duration_sec = max(0.0, duration_anchor - started_at_ts)

        return {
            "session_id": str(row["session_id"]),
            "pivot_id": str(row["pivot_id"]),
            "run_id": str(row["run_id"] or ""),
            "started_at_ts": started_at_ts,
            "started_at": _ts_to_str(started_at_ts),
            "ended_at_ts": ended_at_ts,
            "ended_at": _ts_to_str(ended_at_ts),
            "updated_at_ts": updated_at_ts,
            "updated_at": _ts_to_str(updated_at_ts),
            "is_active": bool(int(row["is_active"])),
            "source": str(row["source"] or ""),
            "label": str(row["label"] or ""),
            "duration_sec": duration_sec,
        }

    def resolve_session(self, pivot_id, session_id=None, run_id=None):
        with self._lock:
            conn = self._require_conn_locked()
            row = self._query_session_row_locked(conn, pivot_id, session_id=session_id, run_id=run_id)
            return self._row_to_session_dict_locked(row)

    def get_or_create_active_session(self, pivot_id, pivot_slug=None, now_ts=None, source="runtime", run_id=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return None

        normalized_slug = str(pivot_slug or slugify(normalized_id))
        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            self._upsert_pivot_locked(conn, normalized_id, normalized_slug, current_ts)
            normalized_run_id = str(run_id or "").strip()
            if normalized_run_id:
                resolved_run = self._query_run_row_locked(conn, run_id=normalized_run_id)
            else:
                resolved_run = self.get_or_create_active_run(now_ts=current_ts, source=source)
            resolved_run_id = str(_row_get(resolved_run, "run_id", "") or "")
            if not resolved_run_id:
                return None

            row = self._query_session_row_locked(conn, normalized_id, session_id=None, run_id=resolved_run_id)
            if row is not None and bool(int(row["is_active"])):
                return self._row_to_session_dict_locked(row, now_ts=current_ts)

            # Reaproveita a ultima sessao do pivô no run atual, evitando criar
            # uma nova sessao em cada restart quando ja existe historico valido.
            if row is not None:
                existing_session_id = str(row["session_id"] or "").strip()
                if existing_session_id:
                    with conn:
                        conn.execute(
                            """
                            UPDATE monitoring_sessions
                            SET
                                is_active = 0,
                                ended_at_ts = COALESCE(ended_at_ts, ?),
                                updated_at_ts = ?
                            WHERE pivot_id = ? AND is_active = 1 AND session_id <> ?
                            """,
                            (current_ts, current_ts, normalized_id, existing_session_id),
                        )
                        conn.execute(
                            """
                            UPDATE monitoring_sessions
                            SET
                                is_active = 1,
                                ended_at_ts = NULL,
                                updated_at_ts = ?
                            WHERE session_id = ?
                            """,
                            (current_ts, existing_session_id),
                        )
                    reused = conn.execute(
                        "SELECT * FROM monitoring_sessions WHERE session_id = ? LIMIT 1",
                        (existing_session_id,),
                    ).fetchone()
                    return self._row_to_session_dict_locked(reused, now_ts=current_ts)

            session_id = str(uuid.uuid4())
            with conn:
                conn.execute(
                    """
                    UPDATE monitoring_sessions
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE pivot_id = ? AND is_active = 1
                    """,
                    (current_ts, current_ts, normalized_id),
                )
                conn.execute(
                    """
                    INSERT INTO monitoring_sessions (
                        session_id,
                        run_id,
                        pivot_id,
                        started_at_ts,
                        ended_at_ts,
                        is_active,
                        source,
                        label,
                        metadata_json,
                        created_at_ts,
                        updated_at_ts
                    ) VALUES (?, ?, ?, ?, NULL, 1, ?, ?, ?, ?, ?)
                    """,
                    (
                        session_id,
                        resolved_run_id,
                        normalized_id,
                        current_ts,
                        str(source or "runtime"),
                        "",
                        "{}",
                        current_ts,
                        current_ts,
                    ),
                )

            created = conn.execute(
                "SELECT * FROM monitoring_sessions WHERE session_id = ? LIMIT 1",
                (session_id,),
            ).fetchone()
            return self._row_to_session_dict_locked(created, now_ts=current_ts)

    def create_new_session(self, pivot_id, pivot_slug=None, now_ts=None, source="ui", run_id=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return None

        normalized_slug = str(pivot_slug or slugify(normalized_id))
        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            self._upsert_pivot_locked(conn, normalized_id, normalized_slug, current_ts)
            normalized_run_id = str(run_id or "").strip()
            if normalized_run_id:
                resolved_run = self._query_run_row_locked(conn, run_id=normalized_run_id)
            else:
                resolved_run = self.get_or_create_active_run(now_ts=current_ts, source=source)
            resolved_run_id = str(_row_get(resolved_run, "run_id", "") or "")
            if not resolved_run_id:
                return None

            session_id = str(uuid.uuid4())
            with conn:
                conn.execute(
                    """
                    UPDATE monitoring_sessions
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE pivot_id = ? AND is_active = 1
                    """,
                    (current_ts, current_ts, normalized_id),
                )
                conn.execute(
                    """
                    INSERT INTO monitoring_sessions (
                        session_id,
                        run_id,
                        pivot_id,
                        started_at_ts,
                        ended_at_ts,
                        is_active,
                        source,
                        label,
                        metadata_json,
                        created_at_ts,
                        updated_at_ts
                    ) VALUES (?, ?, ?, ?, NULL, 1, ?, ?, ?, ?, ?)
                    """,
                    (
                        session_id,
                        resolved_run_id,
                        normalized_id,
                        current_ts,
                        str(source or "ui"),
                        "",
                        "{}",
                        current_ts,
                        current_ts,
                    ),
                )

            row = conn.execute(
                "SELECT * FROM monitoring_sessions WHERE session_id = ? LIMIT 1",
                (session_id,),
            ).fetchone()
            return self._row_to_session_dict_locked(row, now_ts=current_ts)

    def list_sessions(self, pivot_id, limit=200, run_id=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return []
        safe_limit = max(1, min(1000, int(limit or 200)))
        normalized_run_id = str(run_id or "").strip()

        with self._lock:
            conn = self._require_conn_locked()
            query = """
                SELECT *
                FROM monitoring_sessions
                WHERE pivot_id = ?
            """
            params = [normalized_id]
            if normalized_run_id:
                query += " AND run_id = ?"
                params.append(normalized_run_id)
            query += " ORDER BY started_at_ts DESC, updated_at_ts DESC LIMIT ?"
            params.append(safe_limit)
            rows = conn.execute(query, tuple(params)).fetchall()
            return [self._row_to_session_dict_locked(row) for row in rows]

    def get_active_sessions_map(self, run_id=None):
        normalized_run_id = str(run_id or "").strip()
        with self._lock:
            conn = self._require_conn_locked()
            query = """
                SELECT pivot_id, session_id
                FROM monitoring_sessions
                WHERE is_active = 1
            """
            params = []
            if normalized_run_id:
                query += " AND run_id = ?"
                params.append(normalized_run_id)
            query += " ORDER BY updated_at_ts DESC, started_at_ts DESC"
            rows = conn.execute(query, tuple(params)).fetchall()
            result = {}
            for row in rows:
                pivot_id = str(row["pivot_id"] or "").strip()
                session_id = str(row["session_id"] or "").strip()
                if not pivot_id or not session_id:
                    continue
                if pivot_id not in result:
                    result[pivot_id] = session_id
            return result

    def deactivate_all_active_sessions(self, now_ts=None, run_id=None):
        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()
        normalized_run_id = str(run_id or "").strip()
        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                query = """
                    UPDATE monitoring_sessions
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE is_active = 1
                """
                params = [current_ts, current_ts]
                if normalized_run_id:
                    query += " AND run_id = ?"
                    params.append(normalized_run_id)
                conn.execute(query, tuple(params))

    def deactivate_all_active_runs(self, now_ts=None):
        current_ts = _safe_float(now_ts, None)
        if current_ts is None:
            current_ts = time.time()
        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    UPDATE monitoring_runs
                    SET
                        is_active = 0,
                        ended_at_ts = COALESCE(ended_at_ts, ?),
                        updated_at_ts = ?
                    WHERE is_active = 1
                    """,
                    (current_ts, current_ts),
                )

    def purge_all_data(self):
        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute("DELETE FROM connectivity_events")
                conn.execute("DELETE FROM probe_events")
                conn.execute("DELETE FROM probe_delay_points")
                conn.execute("DELETE FROM ping_rssi_points")
                conn.execute("DELETE FROM cloud2_events")
                conn.execute("DELETE FROM drop_events")
                conn.execute("DELETE FROM pivot_snapshots")
                conn.execute("DELETE FROM monitoring_sessions")
                conn.execute("DELETE FROM monitoring_runs")
                conn.execute("DELETE FROM probe_settings")
                conn.execute("DELETE FROM pivots")
                conn.execute(
                    """
                    DELETE FROM sqlite_sequence
                    WHERE name IN (
                        'connectivity_events',
                        'probe_events',
                        'probe_delay_points',
                        'ping_rssi_points',
                        'cloud2_events',
                        'drop_events'
                    )
                    """
                )

    def delete_pivot(self, pivot_id):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return False

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                row = conn.execute(
                    "SELECT 1 FROM pivots WHERE pivot_id = ? LIMIT 1",
                    (normalized_id,),
                ).fetchone()
                conn.execute(
                    "DELETE FROM probe_settings WHERE pivot_id = ?",
                    (normalized_id,),
                )
                conn.execute(
                    "DELETE FROM pivots WHERE pivot_id = ?",
                    (normalized_id,),
                )
            return row is not None

    def _json_dumps(self, value):
        return json.dumps(value, ensure_ascii=False)

    def _json_loads(self, value, fallback):
        if value is None:
            return fallback
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return fallback

    def upsert_snapshot(self, pivot_id, session_id, snapshot_payload, updated_at_ts=None):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        snapshot = snapshot_payload if isinstance(snapshot_payload, dict) else {}
        summary = snapshot.get("summary") if isinstance(snapshot.get("summary"), dict) else {}
        status = summary.get("status") if isinstance(summary.get("status"), dict) else {}
        quality = summary.get("quality") if isinstance(summary.get("quality"), dict) else {}

        status_code = str(status.get("code") or "")
        quality_code = str(quality.get("code") or "")
        last_activity_ts = _safe_float(summary.get("last_activity_ts"), None)
        last_seen_ts = _safe_float(summary.get("last_monitored_message_ts"), None)
        median_ready_value = _safe_bool(summary.get("median_ready"), None)
        median_ready = None if median_ready_value is None else int(median_ready_value)
        median_sample_count = _safe_int(summary.get("median_sample_count"), None)
        if median_sample_count is not None and median_sample_count < 0:
            median_sample_count = 0
        median_cloudv2_interval_sec = _safe_float(summary.get("median_cloudv2_interval_sec"), None)
        disconnect_threshold_sec = _safe_float(summary.get("disconnect_threshold_sec"), None)
        ts_value = _safe_float(updated_at_ts, None)
        if ts_value is None:
            ts_value = _safe_float(snapshot.get("updated_at_ts"), None)
        if ts_value is None:
            ts_value = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO pivot_snapshots (
                        pivot_id,
                        session_id,
                        updated_at_ts,
                        status_code,
                        quality_code,
                        last_activity_ts,
                        last_seen_ts,
                        median_ready,
                        median_sample_count,
                        median_cloudv2_interval_sec,
                        disconnect_threshold_sec,
                        snapshot_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(pivot_id, session_id) DO UPDATE SET
                        updated_at_ts = excluded.updated_at_ts,
                        status_code = excluded.status_code,
                        quality_code = excluded.quality_code,
                        last_activity_ts = excluded.last_activity_ts,
                        last_seen_ts = excluded.last_seen_ts,
                        median_ready = COALESCE(excluded.median_ready, pivot_snapshots.median_ready),
                        median_sample_count = COALESCE(excluded.median_sample_count, pivot_snapshots.median_sample_count),
                        median_cloudv2_interval_sec = COALESCE(excluded.median_cloudv2_interval_sec, pivot_snapshots.median_cloudv2_interval_sec),
                        disconnect_threshold_sec = COALESCE(excluded.disconnect_threshold_sec, pivot_snapshots.disconnect_threshold_sec),
                        snapshot_json = excluded.snapshot_json
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        status_code,
                        quality_code,
                        last_activity_ts,
                        last_seen_ts,
                        median_ready,
                        median_sample_count,
                        median_cloudv2_interval_sec,
                        disconnect_threshold_sec,
                        self._json_dumps(snapshot),
                    ),
                )
                conn.execute(
                    """
                    UPDATE monitoring_sessions
                    SET updated_at_ts = ?
                    WHERE session_id = ?
                    """,
                    (ts_value, normalized_session),
                )
                conn.execute(
                    """
                    UPDATE monitoring_runs
                    SET updated_at_ts = ?
                    WHERE run_id = (
                        SELECT run_id
                        FROM monitoring_sessions
                        WHERE session_id = ?
                        LIMIT 1
                    )
                    """,
                    (ts_value, normalized_session),
                )
                conn.execute(
                    """
                    UPDATE pivots
                    SET
                        updated_at_ts = ?,
                        last_seen_ts = CASE
                            WHEN ? IS NULL THEN last_seen_ts
                            WHEN last_seen_ts IS NULL THEN ?
                            WHEN ? > last_seen_ts THEN ?
                            ELSE last_seen_ts
                        END
                    WHERE pivot_id = ?
                    """,
                    (
                        ts_value,
                        last_seen_ts,
                        last_seen_ts,
                        last_seen_ts,
                        last_seen_ts,
                        normalized_id,
                    ),
                )

    def has_snapshot(self, pivot_id, session_id):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return False
        with self._lock:
            conn = self._require_conn_locked()
            row = conn.execute(
                """
                SELECT 1
                FROM pivot_snapshots
                WHERE pivot_id = ? AND session_id = ?
                LIMIT 1
                """,
                (normalized_id, normalized_session),
            ).fetchone()
            return row is not None

    def session_has_events(self, pivot_id, session_id):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return False
        with self._lock:
            conn = self._require_conn_locked()
            checks = (
                "SELECT 1 FROM connectivity_events WHERE pivot_id = ? AND session_id = ? LIMIT 1",
                "SELECT 1 FROM probe_events WHERE pivot_id = ? AND session_id = ? LIMIT 1",
                "SELECT 1 FROM cloud2_events WHERE pivot_id = ? AND session_id = ? LIMIT 1",
                "SELECT 1 FROM drop_events WHERE pivot_id = ? AND session_id = ? LIMIT 1",
                "SELECT 1 FROM ping_rssi_points WHERE pivot_id = ? AND session_id = ? LIMIT 1",
            )
            for query in checks:
                if conn.execute(query, (normalized_id, normalized_session)).fetchone() is not None:
                    return True
            return False

    def insert_connectivity_event(
        self,
        pivot_id,
        session_id,
        event,
        source_topic=None,
        raw_payload=None,
        parsed_payload=None,
    ):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        event_payload = event if isinstance(event, dict) else {}
        details = event_payload.get("details") if isinstance(event_payload.get("details"), dict) else {}

        ts_value = _safe_float(event_payload.get("ts"), None)
        if ts_value is None:
            ts_value = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO connectivity_events (
                        pivot_id,
                        session_id,
                        ts,
                        topic,
                        event_type,
                        summary,
                        details_json,
                        source_topic,
                        raw_payload,
                        parsed_payload_json,
                        event_json,
                        created_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        str(event_payload.get("topic") or ""),
                        str(event_payload.get("type") or ""),
                        str(event_payload.get("summary") or ""),
                        self._json_dumps(details),
                        str(source_topic or ""),
                        None if raw_payload is None else str(raw_payload),
                        self._json_dumps(parsed_payload if isinstance(parsed_payload, dict) else {}),
                        self._json_dumps(event_payload),
                        time.time(),
                    ),
                )

    def insert_probe_event(self, pivot_id, session_id, event):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        event_payload = event if isinstance(event, dict) else {}
        ts_value = _safe_float(event_payload.get("ts"), None)
        if ts_value is None:
            ts_value = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO probe_events (
                        pivot_id,
                        session_id,
                        ts,
                        event_type,
                        topic,
                        latency_sec,
                        deadline_ts,
                        sent_ts,
                        payload,
                        details_json,
                        event_json,
                        created_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        str(event_payload.get("type") or ""),
                        str(event_payload.get("topic") or ""),
                        _safe_float(event_payload.get("latency_sec"), None),
                        _safe_float(event_payload.get("deadline_ts"), None),
                        _safe_float(event_payload.get("sent_ts"), None),
                        str(event_payload.get("payload") or ""),
                        self._json_dumps(event_payload.get("details") or {}),
                        self._json_dumps(event_payload),
                        time.time(),
                    ),
                )

    def insert_probe_delay_point(
        self,
        pivot_id,
        session_id,
        ts,
        latency_sec,
        avg_latency_sec,
        median_latency_sec,
        sample_count,
    ):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        ts_value = _safe_float(ts, None)
        latency_value = _safe_float(latency_sec, None)
        avg_value = _safe_float(avg_latency_sec, None)
        sample_value = int(sample_count or 0)

        if ts_value is None or latency_value is None or avg_value is None or sample_value <= 0:
            return

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO probe_delay_points (
                        pivot_id,
                        session_id,
                        ts,
                        latency_sec,
                        avg_latency_sec,
                        median_latency_sec,
                        sample_count,
                        created_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        latency_value,
                        avg_value,
                        _safe_float(median_latency_sec, None),
                        sample_value,
                        time.time(),
                    ),
                )

    def insert_ping_rssi_point(self, pivot_id, session_id, ts, rssi):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        ts_value = _safe_float(ts, None)
        rssi_value = _safe_int(rssi, None)
        if ts_value is None or rssi_value is None or rssi_value < 0 or rssi_value > 31:
            return

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO ping_rssi_points (
                        pivot_id,
                        session_id,
                        ts,
                        rssi,
                        created_at_ts
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        rssi_value,
                        time.time(),
                    ),
                )

    def insert_cloud2_event(self, pivot_id, session_id, event):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        event_payload = event if isinstance(event, dict) else {}
        ts_value = _safe_float(event_payload.get("ts"), None)
        if ts_value is None:
            ts_value = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO cloud2_events (
                        pivot_id,
                        session_id,
                        ts,
                        rssi,
                        technology,
                        drop_duration_raw,
                        drop_duration_sec,
                        firmware,
                        event_date,
                        event_json,
                        created_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        None if event_payload.get("rssi") is None else str(event_payload.get("rssi")),
                        None if event_payload.get("technology") is None else str(event_payload.get("technology")),
                        None
                        if event_payload.get("drop_duration_raw") is None
                        else str(event_payload.get("drop_duration_raw")),
                        _safe_float(event_payload.get("drop_duration_sec"), None),
                        None if event_payload.get("firmware") is None else str(event_payload.get("firmware")),
                        None if event_payload.get("event_date") is None else str(event_payload.get("event_date")),
                        self._json_dumps(event_payload),
                        time.time(),
                    ),
                )

    def insert_drop_event(self, pivot_id, session_id, event):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return

        event_payload = event if isinstance(event, dict) else {}
        ts_value = _safe_float(event_payload.get("ts"), None)
        duration_sec = _safe_float(event_payload.get("duration_sec"), None)
        if ts_value is None or duration_sec is None:
            return

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute(
                    """
                    INSERT INTO drop_events (
                        pivot_id,
                        session_id,
                        ts,
                        duration_sec,
                        technology,
                        rssi,
                        event_json,
                        created_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_id,
                        normalized_session,
                        ts_value,
                        duration_sec,
                        None if event_payload.get("technology") is None else str(event_payload.get("technology")),
                        None if event_payload.get("rssi") is None else str(event_payload.get("rssi")),
                        self._json_dumps(event_payload),
                        time.time(),
                    ),
                )

    def upsert_probe_setting(self, pivot_id, enabled, interval_sec):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return
        interval_value = int(interval_sec or 0)
        if interval_value < 1:
            interval_value = 1
        enabled_value = 1 if bool(enabled) else 0
        ts_value = time.time()

        with self._lock:
            conn = self._require_conn_locked()
            self._upsert_pivot_locked(conn, normalized_id, slugify(normalized_id), None)
            with conn:
                conn.execute(
                    """
                    INSERT INTO probe_settings (pivot_id, enabled, interval_sec, updated_at_ts)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(pivot_id) DO UPDATE SET
                        enabled = excluded.enabled,
                        interval_sec = excluded.interval_sec,
                        updated_at_ts = excluded.updated_at_ts
                    """,
                    (
                        normalized_id,
                        enabled_value,
                        interval_value,
                        ts_value,
                    ),
                )

    def load_probe_settings(self):
        with self._lock:
            conn = self._require_conn_locked()
            return self._load_probe_settings_locked(conn)

    def _load_probe_settings_locked(self, conn):
        rows = conn.execute(
            """
            SELECT pivot_id, enabled, interval_sec
            FROM probe_settings
            ORDER BY pivot_id COLLATE NOCASE ASC
            """
        ).fetchall()
        settings = {}
        for row in rows:
            pivot_id = str(row["pivot_id"] or "").strip()
            if not pivot_id:
                continue
            settings[pivot_id] = {
                "enabled": bool(int(row["enabled"])),
                "interval_sec": int(row["interval_sec"]),
            }
        return settings

    def replace_expected_pivots_pending(self, pending_items):
        normalized_rows = []
        seen = set()
        updated_at_ts = time.time()

        for raw_item in pending_items or []:
            item = raw_item if isinstance(raw_item, dict) else {}
            pivot_id = str(item.get("pivot_id") or "").strip()
            if not pivot_id or pivot_id in seen:
                continue
            seen.add(pivot_id)
            normalized_rows.append(
                (
                    pivot_id,
                    _safe_float(item.get("added_at_ts"), None),
                    str(item.get("source") or "ui").strip() or "ui",
                    updated_at_ts,
                )
            )

        with self._lock:
            conn = self._require_conn_locked()
            with conn:
                conn.execute("DELETE FROM expected_pivots_pending")
                if normalized_rows:
                    conn.executemany(
                        """
                        INSERT INTO expected_pivots_pending (
                            pivot_id,
                            added_at_ts,
                            source,
                            updated_at_ts
                        ) VALUES (?, ?, ?, ?)
                        """,
                        normalized_rows,
                    )

    def load_expected_pivots_pending(self):
        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT pivot_id, added_at_ts, source
                FROM expected_pivots_pending
                ORDER BY pivot_id COLLATE NOCASE ASC
                """
            ).fetchall()

        items = []
        for row in rows:
            pivot_id = str(row["pivot_id"] or "").strip()
            if not pivot_id:
                continue
            items.append(
                {
                    "pivot_id": pivot_id,
                    "added_at_ts": _safe_float(row["added_at_ts"], None),
                    "source": str(row["source"] or "ui").strip() or "ui",
                }
            )
        return items

    def _parse_event_row(self, row, fallback_type, fallback_topic):
        event = self._json_loads(row["event_json"], {})
        if not isinstance(event, dict):
            event = {}
        ts_value = _safe_float(event.get("ts"), _safe_float(row["ts"], None))
        event["id"] = int(row["id"])
        event["ts"] = ts_value
        event["at"] = _ts_to_str(ts_value)
        event["type"] = str(event.get("type") or fallback_type)
        event["topic"] = str(event.get("topic") or fallback_topic)
        return event

    def fetch_timeline_events(self, pivot_id, session_id, limit=None):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return []
        safe_limit = max(1, min(50000, int(limit or self.max_events_per_pivot)))

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT *
                FROM connectivity_events
                WHERE pivot_id = ? AND session_id = ?
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (normalized_id, normalized_session, safe_limit),
            ).fetchall()

        events = []
        for row in rows:
            event = self._parse_event_row(
                row=row,
                fallback_type=str(row["event_type"] or ""),
                fallback_topic=str(row["topic"] or ""),
            )
            details = event.get("details")
            if not isinstance(details, dict):
                details = self._json_loads(row["details_json"], {})
                if not isinstance(details, dict):
                    details = {}
            source_topic = str(row["source_topic"] or "").strip()
            if source_topic and "source_topic" not in details:
                details["source_topic"] = source_topic

            raw_payload = row["raw_payload"]
            if raw_payload not in (None, "") and "raw_payload" not in details:
                details["raw_payload"] = str(raw_payload)

            parsed_payload = self._json_loads(row["parsed_payload_json"], {})
            if isinstance(parsed_payload, dict) and parsed_payload and "parsed_payload" not in details:
                details["parsed_payload"] = parsed_payload

            event["summary"] = str(event.get("summary") or row["summary"] or "")
            event["details"] = details
            events.append(event)
        return events

    def fetch_timeline_events_light(self, pivot_id, session_id, limit=None):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return []
        safe_limit = max(1, min(50000, int(limit or self.max_events_per_pivot)))

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT ts, topic
                FROM connectivity_events
                WHERE pivot_id = ? AND session_id = ?
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (normalized_id, normalized_session, safe_limit),
            ).fetchall()

        events = []
        for row in rows:
            ts_value = _safe_float(row["ts"], None)
            events.append(
                {
                    "ts": ts_value,
                    "topic": str(row["topic"] or ""),
                }
            )
        return events

    def fetch_probe_events(self, pivot_id, session_id, limit=None):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return []
        safe_limit = max(1, min(50000, int(limit or self.max_events_per_pivot)))

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT *
                FROM probe_events
                WHERE pivot_id = ? AND session_id = ?
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (normalized_id, normalized_session, safe_limit),
            ).fetchall()

        events = []
        for row in rows:
            event = self._parse_event_row(
                row=row,
                fallback_type=str(row["event_type"] or ""),
                fallback_topic=str(row["topic"] or ""),
            )
            details = event.get("details")
            if not isinstance(details, dict):
                details = self._json_loads(row["details_json"], {})
                if not isinstance(details, dict):
                    details = {}
            event["details"] = details
            events.append(event)
        return events

    def fetch_probe_delay_points(self, pivot_id, session_id, limit=None):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return []
        safe_limit = max(1, min(50000, int(limit or self.max_events_per_pivot)))

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT *
                FROM probe_delay_points
                WHERE pivot_id = ? AND session_id = ?
                ORDER BY ts ASC, id ASC
                LIMIT ?
                """,
                (normalized_id, normalized_session, safe_limit),
            ).fetchall()

        points = []
        for row in rows:
            ts_value = _safe_float(row["ts"], None)
            points.append(
                {
                    "id": int(row["id"]),
                    "ts": ts_value,
                    "at": _ts_to_str(ts_value),
                    "latency_sec": _safe_float(row["latency_sec"], None),
                    "avg_latency_sec": _safe_float(row["avg_latency_sec"], None),
                    "median_latency_sec": _safe_float(row["median_latency_sec"], None),
                    "sample_count": int(row["sample_count"]),
                }
            )
        return points

    def fetch_ping_rssi_points(self, pivot_id, session_id, limit=None):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return []
        safe_limit = max(1, min(50000, int(limit or self.max_events_per_pivot)))

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT *
                FROM ping_rssi_points
                WHERE pivot_id = ? AND session_id = ?
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (normalized_id, normalized_session, safe_limit),
            ).fetchall()

        points = []
        for row in reversed(rows):
            ts_value = _safe_float(row["ts"], None)
            rssi_value = _safe_int(row["rssi"], None)
            if rssi_value is None or rssi_value < 0 or rssi_value > 31:
                continue
            points.append(
                {
                    "id": int(row["id"]),
                    "ts": ts_value,
                    "at": _ts_to_str(ts_value),
                    "rssi": rssi_value,
                }
            )
        return points

    def summarize_probe_stats_for_pivot(self, pivot_id, window_sec=None, now_ts=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return {
                "sent_count": 0,
                "response_count": 0,
                "timeout_count": 0,
                "response_ratio_pct": None,
                "latency_sample_count": 0,
                "latency_last_sec": None,
                "latency_avg_sec": None,
                "latency_median_sec": None,
                "latency_min_sec": None,
                "latency_max_sec": None,
            }

        safe_window = _safe_float(window_sec, None)
        if safe_window is None or safe_window <= 0:
            safe_window = float(PROBE_STATS_WINDOW_SEC)

        reference_ts = _safe_float(now_ts, None)
        if reference_ts is None:
            reference_ts = time.time()
        cutoff_ts = reference_ts - safe_window

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT event_type, latency_sec, ts
                FROM probe_events
                WHERE pivot_id = ?
                    AND ts >= ?
                ORDER BY ts ASC, id ASC
                """,
                (normalized_id, cutoff_ts),
            ).fetchall()

        sent_count = 0
        response_count = 0
        timeout_count = 0
        latency_samples = []

        for row in rows:
            event_type = str(row["event_type"] or "").strip().lower()
            if event_type == "sent":
                sent_count += 1
                continue
            if event_type == "timeout":
                timeout_count += 1
                continue
            if event_type != "response":
                continue

            response_count += 1
            latency = _safe_float(row["latency_sec"], None)
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

    def fetch_cloud2_events(self, pivot_id, session_id, limit=None):
        normalized_id = str(pivot_id or "").strip()
        normalized_session = str(session_id or "").strip()
        if not normalized_id or not normalized_session:
            return []
        safe_limit = max(1, min(50000, int(limit or self.max_events_per_pivot)))

        with self._lock:
            conn = self._require_conn_locked()
            rows = conn.execute(
                """
                SELECT *
                FROM cloud2_events
                WHERE pivot_id = ? AND session_id = ?
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (normalized_id, normalized_session, safe_limit),
            ).fetchall()

        events = []
        for row in rows:
            event = self._json_loads(row["event_json"], {})
            if not isinstance(event, dict):
                event = {}
            ts_value = _safe_float(event.get("ts"), _safe_float(row["ts"], None))
            event["id"] = int(row["id"])
            event["ts"] = ts_value
            event["at"] = _ts_to_str(ts_value)
            if "rssi" not in event:
                event["rssi"] = row["rssi"]
            if "technology" not in event:
                event["technology"] = row["technology"]
            if "drop_duration_raw" not in event:
                event["drop_duration_raw"] = row["drop_duration_raw"]
            if "drop_duration_sec" not in event:
                event["drop_duration_sec"] = _safe_float(row["drop_duration_sec"], None)
            if "firmware" not in event:
                event["firmware"] = row["firmware"]
            if "event_date" not in event:
                event["event_date"] = row["event_date"]
            events.append(event)
        return events

    def _fallback_state_pivot_summary(self, pivot_id, pivot_slug, session_id, run_id):
        return {
            "pivot_id": str(pivot_id),
            "pivot_slug": str(pivot_slug or slugify(pivot_id)),
            "session_id": str(session_id or ""),
            "run_id": str(run_id or ""),
            "is_concentrator": False,
            "latitude": None,
            "longitude": None,
            "status": {
                "code": "gray",
                "label": "Inicial",
                "rank": 1,
                "reason": "Sem snapshot persistido.",
            },
            "quality": {
                "code": "green",
                "label": "Saudavel",
                "rank": 3,
                "reason": "Sem snapshot persistido.",
            },
            "last_ping_ts": None,
            "last_ping_at": "-",
            "last_cloudv2_ts": None,
            "last_cloudv2_at": "-",
            "last_cloud2": {},
            "signal": "",
            "technology": "",
            "signal_technology": "- / -",
            "timeline_mini": [],
            "last_activity_ts": None,
            "last_activity_at": "-",
            "median_ready": False,
            "median_cloudv2_interval_sec": None,
            "median_sample_count": 0,
            "probe": {
                "enabled": False,
                "interval_sec": 300,
                "last_sent_ts": None,
                "last_sent_at": "-",
                "last_response_ts": None,
                "last_response_at": "-",
                "pending": False,
                "pending_deadline_ts": None,
                "pending_deadline_at": "-",
                "timeout_streak": 0,
                "last_result": None,
                "alert": False,
                "sent_count": 0,
                "response_count": 0,
                "timeout_count": 0,
                "response_ratio_pct": None,
                "latency_sample_count": 0,
                "latency_last_sec": None,
                "latency_avg_sec": None,
                "latency_median_sec": None,
                "latency_min_sec": None,
                "latency_max_sec": None,
            },
        }

    def _build_state_summary_from_snapshot_row(self, row, run_id):
        pivot_id = str(row["pivot_id"] or "").strip()
        session_id = str(row["session_id"] or "").strip()
        snapshot_payload = self._json_loads(row["snapshot_json"], {})
        if not isinstance(snapshot_payload, dict):
            snapshot_payload = {}

        pivot_slug = str(snapshot_payload.get("pivot_slug") or slugify(pivot_id))
        summary = snapshot_payload.get("summary")
        if isinstance(summary, dict):
            item = dict(summary)
        else:
            item = self._fallback_state_pivot_summary(
                pivot_id=pivot_id,
                pivot_slug=pivot_slug,
                session_id=session_id,
                run_id=run_id,
            )
        item["pivot_id"] = str(item.get("pivot_id") or pivot_id)
        item["pivot_slug"] = str(item.get("pivot_slug") or pivot_slug)
        item["session_id"] = str(item.get("session_id") or session_id)
        item["run_id"] = str(item.get("run_id") or run_id or "")
        parsed = _safe_bool(item.get("is_concentrator"), None)
        if parsed is None:
            item.pop("is_concentrator", None)
        else:
            item["is_concentrator"] = parsed
        parsed = _safe_float(item.get("latitude"), None)
        if parsed is None:
            item.pop("latitude", None)
        else:
            item["latitude"] = parsed
        parsed = _safe_float(item.get("longitude"), None)
        if parsed is None:
            item.pop("longitude", None)
        else:
            item["longitude"] = parsed

        parsed = _safe_bool(item.get("median_ready"), None)
        if parsed is None:
            item.pop("median_ready", None)
        else:
            item["median_ready"] = parsed
        parsed = _safe_int(item.get("median_sample_count"), None)
        if parsed is None:
            item.pop("median_sample_count", None)
        else:
            item["median_sample_count"] = max(0, parsed)
        parsed = _safe_float(item.get("median_cloudv2_interval_sec"), None)
        if parsed is None:
            item.pop("median_cloudv2_interval_sec", None)
        else:
            item["median_cloudv2_interval_sec"] = parsed
        parsed = _safe_float(item.get("disconnect_threshold_sec"), None)
        if parsed is None:
            item.pop("disconnect_threshold_sec", None)
        else:
            item["disconnect_threshold_sec"] = parsed

        row_keys = set(row.keys()) if hasattr(row, "keys") else set()
        if "snapshot_median_ready" in row_keys:
            parsed = _safe_bool(row["snapshot_median_ready"], None)
            if parsed is not None:
                item["median_ready"] = parsed
        if "snapshot_median_sample_count" in row_keys:
            parsed = _safe_int(row["snapshot_median_sample_count"], None)
            if parsed is not None:
                item["median_sample_count"] = max(0, parsed)
        if "snapshot_median_cloudv2_interval_sec" in row_keys:
            parsed = _safe_float(row["snapshot_median_cloudv2_interval_sec"], None)
            if parsed is not None:
                item["median_cloudv2_interval_sec"] = parsed
        if "snapshot_disconnect_threshold_sec" in row_keys:
            parsed = _safe_float(row["snapshot_disconnect_threshold_sec"], None)
            if parsed is not None:
                item["disconnect_threshold_sec"] = parsed
        if "pivot_is_concentrator" in row_keys:
            parsed = _safe_bool(row["pivot_is_concentrator"], None)
            if parsed is None:
                parsed = bool(_safe_int(row["pivot_is_concentrator"], 0) or 0)
            item["is_concentrator"] = bool(parsed)
        if "pivot_latitude" in row_keys:
            parsed = _safe_float(row["pivot_latitude"], None)
            item["latitude"] = parsed
        if "pivot_longitude" in row_keys:
            parsed = _safe_float(row["pivot_longitude"], None)
            item["longitude"] = parsed

        if "median_ready" not in item:
            item["median_ready"] = False
        if "median_sample_count" not in item:
            item["median_sample_count"] = 0
        if "is_concentrator" not in item:
            item["is_concentrator"] = False
        if "latitude" not in item:
            item["latitude"] = None
        if "longitude" not in item:
            item["longitude"] = None

        if not isinstance(item.get("status"), dict):
            item["status"] = {
                "code": "gray",
                "label": "Inicial",
                "rank": 1,
                "reason": "Sem status persistido.",
            }
        if not isinstance(item.get("quality"), dict):
            item["quality"] = {
                "code": "green",
                "label": "Saudavel",
                "rank": 3,
                "reason": "Sem qualidade persistida.",
            }
        if not isinstance(item.get("probe"), dict):
            item["probe"] = self._fallback_state_pivot_summary(
                pivot_id=pivot_id,
                pivot_slug=pivot_slug,
                session_id=session_id,
                run_id=run_id,
            )["probe"]
        if not isinstance(item.get("last_cloud2"), dict):
            item["last_cloud2"] = {}
        _normalize_summary_timestamps(item)
        _ensure_summary_signal_fields(item)
        item["timeline_mini"] = _normalize_timeline_mini_segments(item.get("timeline_mini"))
        return item

    def get_run_state_payload(self, run_id=None, connectivity_settings=None):
        with self._lock:
            conn = self._require_conn_locked()
            run_row = self._query_run_row_locked(conn, run_id=run_id)
            if run_row is None:
                return None

            resolved_run_id = str(run_row["run_id"] or "").strip()
            if not resolved_run_id:
                return None

            rows = conn.execute(
                """
                SELECT
                    sessions.pivot_id,
                    sessions.session_id,
                    sessions.updated_at_ts AS session_updated_at_ts,
                    COALESCE(pivots.is_concentrator, 0) AS pivot_is_concentrator,
                    pivots.latitude AS pivot_latitude,
                    pivots.longitude AS pivot_longitude,
                    snapshots.snapshot_json,
                    snapshots.median_ready AS snapshot_median_ready,
                    snapshots.median_sample_count AS snapshot_median_sample_count,
                    snapshots.median_cloudv2_interval_sec AS snapshot_median_cloudv2_interval_sec,
                    snapshots.disconnect_threshold_sec AS snapshot_disconnect_threshold_sec,
                    snapshots.updated_at_ts AS snapshot_updated_at_ts
                FROM monitoring_sessions AS sessions
                INNER JOIN pivots AS pivots
                    ON pivots.pivot_id = sessions.pivot_id
                LEFT JOIN pivot_snapshots AS snapshots
                    ON snapshots.pivot_id = sessions.pivot_id
                    AND snapshots.session_id = sessions.session_id
                WHERE sessions.run_id = ?
                    AND sessions.session_id = (
                        SELECT sessions_inner.session_id
                        FROM monitoring_sessions AS sessions_inner
                        WHERE sessions_inner.run_id = sessions.run_id
                            AND sessions_inner.pivot_id = sessions.pivot_id
                        ORDER BY sessions_inner.updated_at_ts DESC, sessions_inner.started_at_ts DESC
                        LIMIT 1
                    )
                ORDER BY sessions.pivot_id COLLATE NOCASE ASC
                """,
                (resolved_run_id,),
            ).fetchall()
            probe_settings = self._load_probe_settings_locked(conn)

        pivots = []
        last_updated_ts = _safe_float(run_row["updated_at_ts"], None)
        for row in rows:
            summary = self._build_state_summary_from_snapshot_row(row, resolved_run_id)
            pivot_id = str(summary.get("pivot_id") or row["pivot_id"] or "").strip()
            session_id = str(summary.get("session_id") or row["session_id"] or "").strip()
            _overlay_probe_setting_on_summary(summary, probe_settings.get(pivot_id))
            row_updated = _safe_float(row["snapshot_updated_at_ts"], _safe_float(row["session_updated_at_ts"], None))
            if row_updated is None:
                row_updated = time.time()

            existing_timeline_mini = _normalize_timeline_mini_segments(summary.get("timeline_mini"))
            if existing_timeline_mini:
                summary["timeline_mini"] = existing_timeline_mini
            elif pivot_id and session_id:
                timeline_events = self.fetch_timeline_events_light(
                    pivot_id,
                    session_id,
                    limit=self.max_events_per_pivot,
                )
                disconnect_threshold_sec = _resolve_timeline_disconnect_threshold(
                    summary,
                    settings=connectivity_settings,
                )
                summary["timeline_mini"] = _build_timeline_mini_segments(
                    timeline_events,
                    window_end_ts=row_updated,
                    window_sec=TIMELINE_MINI_DEFAULT_WINDOW_SEC,
                    disconnect_threshold_sec=disconnect_threshold_sec,
                )
            else:
                summary["timeline_mini"] = []

            pivots.append(summary)
            if row_updated is None:
                continue
            if last_updated_ts is None or row_updated > last_updated_ts:
                last_updated_ts = row_updated

        if last_updated_ts is None:
            last_updated_ts = time.time()

        return {
            "run_id": resolved_run_id,
            "run": self._row_to_run_dict_locked(run_row, now_ts=last_updated_ts),
            "updated_at_ts": last_updated_ts,
            "updated_at": _ts_to_str(last_updated_ts),
            "pivots": pivots,
        }

    def get_quality_cards_payload(self, run_id=None, timeline_limit=None, pivot_ids=None):
        safe_timeline_limit = max(1, min(50000, int(timeline_limit or self.max_events_per_pivot)))
        normalized_pivot_ids = []
        seen_pivot_ids = set()
        for raw_pivot_id in pivot_ids or []:
            pivot_id = str(raw_pivot_id or "").strip()
            if not pivot_id or pivot_id in seen_pivot_ids:
                continue
            seen_pivot_ids.add(pivot_id)
            normalized_pivot_ids.append(pivot_id)

        with self._lock:
            conn = self._require_conn_locked()
            run_row = self._query_run_row_locked(conn, run_id=run_id)
            if run_row is None:
                return None

            resolved_run_id = str(run_row["run_id"] or "").strip()
            if not resolved_run_id:
                return None

            query = """
                SELECT
                    sessions.pivot_id,
                    sessions.session_id,
                    sessions.updated_at_ts AS session_updated_at_ts,
                    COALESCE(pivots.is_concentrator, 0) AS pivot_is_concentrator,
                    pivots.latitude AS pivot_latitude,
                    pivots.longitude AS pivot_longitude,
                    snapshots.snapshot_json,
                    snapshots.median_ready AS snapshot_median_ready,
                    snapshots.median_sample_count AS snapshot_median_sample_count,
                    snapshots.median_cloudv2_interval_sec AS snapshot_median_cloudv2_interval_sec,
                    snapshots.disconnect_threshold_sec AS snapshot_disconnect_threshold_sec,
                    snapshots.updated_at_ts AS snapshot_updated_at_ts
                FROM monitoring_sessions AS sessions
                INNER JOIN pivots AS pivots
                    ON pivots.pivot_id = sessions.pivot_id
                LEFT JOIN pivot_snapshots AS snapshots
                    ON snapshots.pivot_id = sessions.pivot_id
                    AND snapshots.session_id = sessions.session_id
                WHERE sessions.run_id = ?
            """
            params = [resolved_run_id]
            if normalized_pivot_ids:
                placeholders = ", ".join("?" for _ in normalized_pivot_ids)
                query += f" AND sessions.pivot_id IN ({placeholders})"
                params.extend(normalized_pivot_ids)
            query += """
                    AND sessions.session_id = (
                        SELECT sessions_inner.session_id
                        FROM monitoring_sessions AS sessions_inner
                        WHERE sessions_inner.run_id = sessions.run_id
                            AND sessions_inner.pivot_id = sessions.pivot_id
                        ORDER BY sessions_inner.updated_at_ts DESC, sessions_inner.started_at_ts DESC
                        LIMIT 1
                    )
                ORDER BY sessions.pivot_id COLLATE NOCASE ASC
                """
            rows = conn.execute(query, tuple(params)).fetchall()
            probe_settings = self._load_probe_settings_locked(conn)

        pivots = []
        last_updated_ts = _safe_float(run_row["updated_at_ts"], None)
        for row in rows:
            summary = self._build_state_summary_from_snapshot_row(row, resolved_run_id)
            pivot_id = str(summary.get("pivot_id") or row["pivot_id"] or "").strip()
            session_id = str(summary.get("session_id") or row["session_id"] or "").strip()
            _overlay_probe_setting_on_summary(summary, probe_settings.get(pivot_id))
            if not pivot_id or not session_id:
                continue

            timeline = self.fetch_timeline_events_light(
                pivot_id,
                session_id,
                limit=safe_timeline_limit,
            )
            timeline_latest_ts = _safe_float((timeline[0] or {}).get("ts"), None) if timeline else None
            pivot_updated_ts = _safe_float(
                row["snapshot_updated_at_ts"],
                _safe_float(row["session_updated_at_ts"], timeline_latest_ts),
            )
            if pivot_updated_ts is None:
                pivot_updated_ts = time.time()

            pivots.append(
                {
                    "pivot_id": pivot_id,
                    "pivot_slug": str(summary.get("pivot_slug") or slugify(pivot_id)),
                    "session_id": session_id,
                    "run_id": resolved_run_id,
                    "updated_at_ts": pivot_updated_ts,
                    "updated_at": _ts_to_str(pivot_updated_ts),
                    "summary": summary,
                    "timeline": timeline,
                }
            )
            if last_updated_ts is None or pivot_updated_ts > last_updated_ts:
                last_updated_ts = pivot_updated_ts

        if last_updated_ts is None:
            last_updated_ts = time.time()

        return {
            "run_id": resolved_run_id,
            "run": self._row_to_run_dict_locked(run_row, now_ts=last_updated_ts),
            "updated_at_ts": last_updated_ts,
            "updated_at": _ts_to_str(last_updated_ts),
            "pivots": pivots,
        }

    def get_panel_payload(self, pivot_id, session_id=None, run_id=None):
        normalized_id = str(pivot_id or "").strip()
        if not normalized_id:
            return None

        with self._lock:
            conn = self._require_conn_locked()
            session_row = self._query_session_row_locked(
                conn,
                normalized_id,
                session_id=session_id,
                run_id=run_id,
            )
            if session_row is None:
                return None

            resolved_session_id = str(session_row["session_id"])
            resolved_run_id = str(session_row["run_id"] or "").strip()
            run_row = self._query_run_row_locked(conn, run_id=resolved_run_id) if resolved_run_id else None
            snapshot_row = conn.execute(
                """
                SELECT snapshot_json
                FROM pivot_snapshots
                WHERE pivot_id = ? AND session_id = ?
                LIMIT 1
                """,
                (normalized_id, resolved_session_id),
            ).fetchone()
            pivot_row = conn.execute(
                """
                SELECT
                    COALESCE(is_concentrator, 0) AS is_concentrator,
                    latitude,
                    longitude
                FROM pivots
                WHERE pivot_id = ?
                LIMIT 1
                """,
                (normalized_id,),
            ).fetchone()
            pivot_is_concentrator = False
            pivot_latitude = None
            pivot_longitude = None
            if pivot_row is not None:
                parsed = _safe_bool(pivot_row["is_concentrator"], None)
                if parsed is None:
                    parsed = bool(_safe_int(pivot_row["is_concentrator"], 0) or 0)
                pivot_is_concentrator = bool(parsed)
                pivot_latitude = _safe_float(pivot_row["latitude"], None)
                pivot_longitude = _safe_float(pivot_row["longitude"], None)
            probe_settings = self._load_probe_settings_locked(conn)

        payload = {}
        if snapshot_row is not None:
            payload = self._json_loads(snapshot_row["snapshot_json"], {})
            if not isinstance(payload, dict):
                payload = {}

        if not payload:
            payload = {
                "pivot_id": normalized_id,
                "pivot_slug": slugify(normalized_id),
                "updated_at": _ts_to_str(time.time()),
                "updated_at_ts": time.time(),
                "summary": {},
                "metrics": {},
                "timeline": [],
                "probe_events": [],
                "cloud2_events": [],
                "probe_delay_points": [],
                "hasRssi": False,
                "rssiSeries": [],
            }

        timeline = self.fetch_timeline_events(normalized_id, resolved_session_id, limit=self.max_events_per_pivot)
        probe_events = self.fetch_probe_events(normalized_id, resolved_session_id, limit=self.max_events_per_pivot)
        cloud2_events = self.fetch_cloud2_events(normalized_id, resolved_session_id, limit=self.max_events_per_pivot)
        probe_delay_points = self.fetch_probe_delay_points(
            normalized_id,
            resolved_session_id,
            limit=self.max_events_per_pivot,
        )
        rssi_series = self.fetch_ping_rssi_points(
            normalized_id,
            resolved_session_id,
            limit=self.max_events_per_pivot,
        )

        payload["pivot_id"] = normalized_id
        payload["pivot_slug"] = str(payload.get("pivot_slug") or slugify(normalized_id))
        payload["timeline"] = timeline
        payload["probe_events"] = probe_events
        payload["cloud2_events"] = cloud2_events
        payload["probe_delay_points"] = probe_delay_points
        payload["rssiSeries"] = rssi_series
        payload["hasRssi"] = bool(rssi_series)
        payload["session_id"] = resolved_session_id
        payload["run_id"] = resolved_run_id
        payload["session"] = self._row_to_session_dict_locked(session_row)
        payload["run"] = self._row_to_run_dict_locked(run_row)
        _normalize_snapshot_payload_timestamps(payload)

        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        probe_summary = summary.get("probe") if isinstance(summary.get("probe"), dict) else {}
        probe_stats = self.summarize_probe_stats_for_pivot(
            normalized_id,
            window_sec=PROBE_STATS_WINDOW_SEC,
            now_ts=payload.get("updated_at_ts"),
        )
        probe_summary.update(
            {
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
            }
        )
        summary["probe"] = probe_summary
        summary["is_concentrator"] = bool(pivot_is_concentrator)
        summary["latitude"] = pivot_latitude
        summary["longitude"] = pivot_longitude
        _overlay_probe_setting_on_summary(summary, probe_settings.get(normalized_id))
        _normalize_summary_timestamps(summary)
        payload["summary"] = summary
        payload["is_concentrator"] = bool(pivot_is_concentrator)
        payload["latitude"] = pivot_latitude
        payload["longitude"] = pivot_longitude
        _normalize_snapshot_payload_timestamps(payload)
        return payload
