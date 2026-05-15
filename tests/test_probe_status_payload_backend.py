import os
import tempfile
import unittest
from unittest.mock import patch

import backend.cloudv2_telemetry as telemetry_mod
from backend.cloudv2_persistence import TelemetryPersistence
from backend.cloudv2_telemetry import TelemetryStore, parse_device_payload, parse_probe_status_payload


class ProbeStatusPayloadTests(unittest.TestCase):
    def _build_store(self, temp_dir):
        db_path = os.path.join(temp_dir, "telemetry.sqlite3")
        config = {
            "enable_background_worker": False,
            "require_apply_to_start": False,
            "continuous_monitoring_mode": True,
            "history_mode": "merge",
            "sqlite_db_path": db_path,
            "api_state_cache_ttl_sec": 0,
            "api_quality_cache_ttl_sec": 0,
        }
        ensure_dirs = lambda: os.makedirs(temp_dir, exist_ok=True)
        data_dir_patch = patch.object(telemetry_mod, "DATA_DIR", temp_dir)
        ensure_dirs_patch = patch.object(telemetry_mod, "ensure_dirs", ensure_dirs)
        data_dir_patch.start()
        ensure_dirs_patch.start()
        self.addCleanup(data_dir_patch.stop)
        self.addCleanup(ensure_dirs_patch.stop)
        store = TelemetryStore(config=config, log_dir=temp_dir)
        store.start()
        return store

    def test_parse_probe_status_payload_handles_hyphenated_modem_name(self):
        parsed, error = parse_device_payload(
            "#11-AgroMB_3-18-LTE-1773171351-VIRTUEYES-A7608SA-H-862733060787358-180-180-5000-virtueyes.com.br-1-56.7-v2.8.4$"
        )
        self.assertIsNone(error)

        info = parse_probe_status_payload(parsed)

        self.assertEqual(info["operator"], "VIRTUEYES")
        self.assertEqual(info["modem_name"], "A7608SA-H")
        self.assertEqual(info["firmware"], "v2.8.4")
        self.assertEqual(info["board_timestamp_ts"], 1773171351)
        self.assertEqual(info["esp_temp_c"], 56.7)

    def test_parse_probe_status_payload_handles_at_command_noise_in_modem_name(self):
        parsed, error = parse_device_payload(
            "#11-Savana_16-20-LTE-1773171016-Unknown Operator-AT+CGMM  A7608SA-H-862733060786376-180-180-5000-APNNAME1-8-81.7-v2.8.7$"
        )
        self.assertIsNone(error)

        info = parse_probe_status_payload(parsed)

        self.assertEqual(info["operator"], "Unknown Operator")
        self.assertEqual(info["modem_name"], "AT+CGMM A7608SA-H")
        self.assertEqual(info["apn"], "APNNAME1")
        self.assertEqual(info["networks"], 8)
        self.assertEqual(info["firmware"], "v2.8.7")

    def test_probe_response_info_is_added_to_pivot_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            sent_messages = []
            try:
                store.queue_expected_pivots(["Savana_16"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-Savana_16-discovery$", ts=1_773_171_001.0)
                store.update_probe_setting("Savana_16", enabled=True, interval_sec=900)
                store.set_probe_sender(lambda topic, payload: sent_messages.append((topic, payload)) or True)

                changed = store.tick(now=1_773_171_010.0)
                self.assertTrue(changed)
                self.assertEqual(sent_messages, [("Savana_16", "#11$")])

                result = store.process_message(
                    "cloudv2-info",
                    "#11-Savana_16-20-LTE-1773171016-Unknown Operator-AT+CGMM  A7608SA-H-862733060786376-180-180-5000-APNNAME1-8-81.7-v2.8.7$",
                    ts=1_773_171_012.0,
                )
                self.assertTrue(result["accepted"])

                snapshot = store.get_pivot_snapshot("Savana_16", now=1_773_171_020.0)
                probe = snapshot["summary"]["probe"]

                self.assertEqual(probe["response_count"], 1)
                self.assertEqual(probe["last_response_info"]["operator"], "Unknown Operator")
                self.assertEqual(probe["last_response_info"]["modem_name"], "AT+CGMM A7608SA-H")
                self.assertEqual(probe["last_response_info"]["firmware"], "v2.8.7")
                self.assertEqual(probe["last_response_info"]["keep_alive_sec"], 180)
            finally:
                store.stop()

    def test_event_only_topics_are_recorded_without_affecting_probe_logic(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)

                shutdown = store.process_message(
                    "cloudv2-shutdown",
                    "#90-PivotA_1-shutdown_reason$",
                    ts=1_773_171_010.0,
                )
                error = store.process_message(
                    "cloudv2-error",
                    "#91-PivotA_1-error_reason$",
                    ts=1_773_171_011.0,
                )

                self.assertTrue(shutdown["accepted"])
                self.assertTrue(error["accepted"])

                snapshot = store.get_pivot_snapshot("PivotA_1", now=1_773_171_020.0)
                events = snapshot["timeline"]
                self.assertTrue(
                    any(
                        item.get("topic") == "cloudv2-shutdown"
                        and item.get("details", {}).get("raw_payload") == "#90-PivotA_1-shutdown_reason$"
                        for item in events
                    )
                )
                self.assertTrue(
                    any(
                        item.get("topic") == "cloudv2-error"
                        and item.get("details", {}).get("raw_payload") == "#91-PivotA_1-error_reason$"
                        for item in events
                    )
                )
            finally:
                store.stop()

    def test_dynamic_pivot_topic_probe_sent_keeps_payload_on_timeline(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            sent_messages = []
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)
                store.update_probe_setting("PivotA_1", enabled=True, interval_sec=900)
                store.set_probe_sender(lambda topic, payload: sent_messages.append((topic, payload)) or True)

                store.tick(now=1_773_171_010.0)

                self.assertEqual(sent_messages, [("PivotA_1", "#11$")])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=1_773_171_020.0)
                probe_sent = next(item for item in snapshot["timeline"] if item.get("type") == "probe_sent")
                self.assertEqual(probe_sent["topic"], "PivotA_1")
                self.assertEqual(probe_sent["details"]["raw_payload"], "#11$")
            finally:
                store.stop()

    def test_restart_prefers_db_probe_settings_over_stale_runtime_store(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pivot_id = "Savana_16"
            first_store = self._build_store(temp_dir)
            try:
                first_store.queue_expected_pivots([pivot_id], now=1_773_171_000.0, source="test")
                first_store.process_message("cloudv2", f"#01-{pivot_id}-discovery$", ts=1_773_171_001.0)
                first_store.update_probe_setting(pivot_id, enabled=True, interval_sec=300)
            finally:
                first_store.stop()

            db_path = os.path.join(temp_dir, "telemetry.sqlite3")
            persistence = TelemetryPersistence(db_path=db_path, max_events_per_pivot=5000)
            persistence.start()
            try:
                persistence.upsert_probe_setting(pivot_id, False, 900)
            finally:
                persistence.stop()

            restarted_store = self._build_store(temp_dir)
            try:
                probe_config = restarted_store.get_probe_config_snapshot()
                probe_item = next(item for item in probe_config["items"] if item["pivot_id"] == pivot_id)
                self.assertFalse(probe_item["enabled"])
                self.assertEqual(probe_item["interval_sec"], 900)

                snapshot = restarted_store.get_pivot_snapshot(pivot_id, now=1_773_171_100.0)
                self.assertIsNotNone(snapshot)
                self.assertFalse(snapshot["summary"]["probe"]["enabled"])
                self.assertEqual(snapshot["summary"]["probe"]["interval_sec"], 900)
            finally:
                restarted_store.stop()


if __name__ == "__main__":
    unittest.main()
