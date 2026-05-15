import os
import tempfile
import unittest
from unittest.mock import patch

import backend.cloudv2_telemetry as telemetry_mod
from backend.cloudv2_persistence import TelemetryPersistence
from backend.cloudv2_telemetry import (
    TelemetryStore,
    parse_comm_main_mode_config_payload,
    parse_device_payload,
    parse_network_config_payload,
    parse_physical_barrier_config_payload,
    parse_pivot_config_payload,
    parse_probe_status_payload,
    parse_reboot_config_payload,
    parse_rush_config_payload,
    parse_sector_config_payload,
    parse_shutdown_reason_payload,
    parse_virtual_barrier_config_payload,
)


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
                scheduling = store.process_message(
                    "cloudv2-scheduling",
                    "#93-PivotA_1-scheduling_payload$",
                    ts=1_773_171_012.0,
                )

                self.assertTrue(shutdown["accepted"])
                self.assertTrue(error["accepted"])
                self.assertTrue(scheduling["accepted"])

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
                self.assertTrue(
                    any(
                        item.get("topic") == "cloudv2-scheduling"
                        and item.get("details", {}).get("raw_payload") == "#93-PivotA_1-scheduling_payload$"
                        for item in events
                    )
                )
            finally:
                store.stop()

    def test_pivot_config_response_requires_prior_request(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            sent_messages = []
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)

                parsed, error = parse_device_payload("#03-PivotA_1-K1-P1-120-10-20-30$")
                self.assertIsNone(error)
                parsed_config = parse_pivot_config_payload(parsed)
                self.assertEqual(parsed_config["contactor"], "K1")
                self.assertEqual(parsed_config["pressurization_time"], 120)

                unsolicited = store.process_message(
                    "cloudv2-config",
                    "#03-PivotA_1-K1-P1-120-10-20-30$",
                    ts=1_773_171_010.0,
                )
                self.assertTrue(unsolicited["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=1_773_171_011.0)
                self.assertIsNone(snapshot["summary"]["pivot_config"]["contactor"])

                store.set_pivot_config_sender(lambda topic, payload: sent_messages.append((topic, payload)) or True)
                request = store.send_pivot_config_request("PivotA_1")
                self.assertEqual(request["payload"], "#03-PivotA_1$")
                self.assertEqual(sent_messages, [("PivotA_1", "#03-PivotA_1$")])

                response = store.process_message(
                    "cloudv2-config",
                    "#03-PivotA_1-K1-P1-120-10-20-30$",
                    ts=request["request_ts"] + 1,
                )
                self.assertTrue(response["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=request["request_ts"] + 2)
                config = snapshot["summary"]["pivot_config"]
                self.assertEqual(config["contactor"], "K1")
                self.assertEqual(config["pressure"], "P1")
                self.assertEqual(config["pressurization_time"], 120)
                self.assertEqual(config["on_time"], 10)
                self.assertEqual(config["off_time"], 20)
                self.assertEqual(config["read_time"], 30)
                self.assertFalse(config["pending"])

                late_unrequested = store.process_message(
                    "cloudv2-config",
                    "#03-PivotA_1-K2-P2-240-11-21-31$",
                    ts=request["request_ts"] + 2,
                )
                self.assertTrue(late_unrequested["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=request["request_ts"] + 3)
                config = snapshot["summary"]["pivot_config"]
                self.assertEqual(config["contactor"], "K1")
                self.assertEqual(config["pressure"], "P1")
            finally:
                store.stop()

    def test_network_config_response_requires_prior_request(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            sent_messages = []
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)

                parsed, error = parse_device_payload("#02-PivotA_1-GPRS001-virtueyes.com.br-WifiSoil-12345678$")
                self.assertIsNone(error)
                parsed_config = parse_network_config_payload(parsed)
                self.assertEqual(parsed_config["gprs_id"], "GPRS001")
                self.assertEqual(parsed_config["modem_apn"], "virtueyes.com.br")

                unsolicited = store.process_message(
                    "cloudv2-config",
                    "#02-PivotA_1-GPRS001-virtueyes.com.br-WifiSoil-12345678$",
                    ts=1_773_171_010.0,
                )
                self.assertTrue(unsolicited["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=1_773_171_011.0)
                self.assertIsNone(snapshot["summary"]["network_config"]["gprs_id"])

                store.set_pivot_config_sender(lambda topic, payload: sent_messages.append((topic, payload)) or True)
                request = store.send_config_request("PivotA_1", idp="02")
                self.assertEqual(request["payload"], "#02-PivotA_1$")
                self.assertEqual(sent_messages, [("PivotA_1", "#02-PivotA_1$")])

                response = store.process_message(
                    "cloudv2-config",
                    "#02-PivotA_1-GPRS001-virtueyes.com.br-WifiSoil-12345678$",
                    ts=request["request_ts"] + 1,
                )
                self.assertTrue(response["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=request["request_ts"] + 2)
                config = snapshot["summary"]["network_config"]
                self.assertEqual(config["gprs_id"], "GPRS001")
                self.assertEqual(config["modem_apn"], "virtueyes.com.br")
                self.assertEqual(config["wifi_ssid"], "WifiSoil")
                self.assertEqual(config["wifi_pass"], "12345678")
                self.assertFalse(config["pending"])
            finally:
                store.stop()

    def test_rush_config_response_requires_prior_request(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            sent_messages = []
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)

                parsed, error = parse_device_payload("#04-PivotA_1-0800-1830-1$")
                self.assertIsNone(error)
                parsed_config = parse_rush_config_payload(parsed)
                self.assertEqual(parsed_config["start_time_hhmm"], "0800")
                self.assertEqual(parsed_config["end_time_hhmm"], "1830")
                self.assertTrue(parsed_config["enabled"])

                parsed_disabled, error = parse_device_payload("#04-PivotA_1-0000-0000-0$")
                self.assertIsNone(error)
                self.assertFalse(parse_rush_config_payload(parsed_disabled)["enabled"])

                parsed_partial, error = parse_device_payload("#04-PivotA_1-0000-0000$")
                self.assertIsNone(error)
                partial_config = parse_rush_config_payload(parsed_partial)
                self.assertEqual(partial_config["start_time_hhmm"], "0000")
                self.assertEqual(partial_config["end_time_hhmm"], "0000")
                self.assertIsNone(partial_config["enabled"])

                unsolicited = store.process_message(
                    "cloudv2-config",
                    "#04-PivotA_1-0800-1830-1$",
                    ts=1_773_171_010.0,
                )
                self.assertTrue(unsolicited["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=1_773_171_011.0)
                self.assertIsNone(snapshot["summary"]["rush_config"]["start_time_hhmm"])

                store.set_pivot_config_sender(lambda topic, payload: sent_messages.append((topic, payload)) or True)
                request = store.send_config_request("PivotA_1", idp="04")
                self.assertEqual(request["payload"], "#04-PivotA_1$")
                self.assertEqual(sent_messages, [("PivotA_1", "#04-PivotA_1$")])

                response = store.process_message(
                    "cloudv2-config",
                    "#04-PivotA_1-0800-1830$",
                    ts=request["request_ts"] + 1,
                )
                self.assertTrue(response["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=request["request_ts"] + 2)
                config = snapshot["summary"]["rush_config"]
                self.assertEqual(config["start_time_hhmm"], "0800")
                self.assertEqual(config["end_time_hhmm"], "1830")
                self.assertIsNone(config["enabled"])
                self.assertFalse(config["pending"])
            finally:
                store.stop()

    def test_sector_config_response_requires_prior_request(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            sent_messages = []
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)

                parsed, error = parse_device_payload("#05-PivotA_1-2-10-90-120-180$")
                self.assertIsNone(error)
                parsed_config = parse_sector_config_payload(parsed)
                self.assertEqual(parsed_config["sector_number"], 2)
                self.assertEqual(parsed_config["sectors"][0]["start_angle"], 10)
                self.assertEqual(parsed_config["sectors"][1]["end_angle"], 180)
                self.assertIsNone(parsed_config["sectors"][2]["start_angle"])

                unsolicited = store.process_message(
                    "cloudv2-config",
                    "#05-PivotA_1-2-10-90-120-180$",
                    ts=1_773_171_010.0,
                )
                self.assertTrue(unsolicited["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=1_773_171_011.0)
                self.assertIsNone(snapshot["summary"]["sector_config"]["sector_number"])

                store.set_pivot_config_sender(lambda topic, payload: sent_messages.append((topic, payload)) or True)
                request = store.send_config_request("PivotA_1", idp="05")
                self.assertEqual(request["payload"], "#05-PivotA_1$")
                self.assertEqual(sent_messages, [("PivotA_1", "#05-PivotA_1$")])

                response = store.process_message(
                    "cloudv2-config",
                    "#05-PivotA_1-2-10-90-120-180$",
                    ts=request["request_ts"] + 1,
                )
                self.assertTrue(response["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=request["request_ts"] + 2)
                config = snapshot["summary"]["sector_config"]
                self.assertEqual(config["sector_number"], 2)
                self.assertEqual(config["sectors"][0]["start_angle"], 10)
                self.assertEqual(config["sectors"][0]["end_angle"], 90)
                self.assertEqual(config["sectors"][1]["start_angle"], 120)
                self.assertEqual(config["sectors"][1]["end_angle"], 180)
                self.assertFalse(config["pending"])
            finally:
                store.stop()

    def test_physical_barrier_config_response_requires_prior_request(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            sent_messages = []
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)

                parsed, error = parse_device_payload("#22-PivotA_1-10-90-1-0-5$")
                self.assertIsNone(error)
                parsed_config = parse_physical_barrier_config_payload(parsed)
                self.assertEqual(parsed_config["start_angle"], 10)
                self.assertEqual(parsed_config["end_angle"], 90)
                self.assertTrue(parsed_config["automatic_return"])
                self.assertFalse(parsed_config["water_return"])
                self.assertEqual(parsed_config["time_leaving_barrier"], 5)

                unsolicited = store.process_message(
                    "cloudv2-config",
                    "#22-PivotA_1-10-90-1-0-5$",
                    ts=1_773_171_010.0,
                )
                self.assertTrue(unsolicited["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=1_773_171_011.0)
                self.assertIsNone(snapshot["summary"]["physical_barrier_config"]["start_angle"])

                store.set_pivot_config_sender(lambda topic, payload: sent_messages.append((topic, payload)) or True)
                request = store.send_config_request("PivotA_1", idp="22")
                self.assertEqual(request["payload"], "#22-PivotA_1$")
                self.assertEqual(sent_messages, [("PivotA_1", "#22-PivotA_1$")])

                response = store.process_message(
                    "cloudv2-config",
                    "#22-PivotA_1-10-90-1-0-5$",
                    ts=request["request_ts"] + 1,
                )
                self.assertTrue(response["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=request["request_ts"] + 2)
                config = snapshot["summary"]["physical_barrier_config"]
                self.assertEqual(config["start_angle"], 10)
                self.assertEqual(config["end_angle"], 90)
                self.assertTrue(config["automatic_return"])
                self.assertFalse(config["water_return"])
                self.assertEqual(config["time_leaving_barrier"], 5)
                self.assertFalse(config["pending"])
            finally:
                store.stop()

    def test_reboot_config_response_requires_prior_request(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            sent_messages = []
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)

                parsed, error = parse_device_payload("#24-PivotA_1-1-3600$")
                self.assertIsNone(error)
                parsed_config = parse_reboot_config_payload(parsed)
                self.assertTrue(parsed_config["enabled"])
                self.assertEqual(parsed_config["reboot_timeout_sec"], 3600)

                unsolicited = store.process_message(
                    "cloudv2-config",
                    "#24-PivotA_1-1-3600$",
                    ts=1_773_171_010.0,
                )
                self.assertTrue(unsolicited["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=1_773_171_011.0)
                self.assertIsNone(snapshot["summary"]["reboot_config"]["enabled"])

                store.set_pivot_config_sender(lambda topic, payload: sent_messages.append((topic, payload)) or True)
                request = store.send_config_request("PivotA_1", idp="24")
                self.assertEqual(request["payload"], "#24-PivotA_1$")
                self.assertEqual(sent_messages, [("PivotA_1", "#24-PivotA_1$")])

                response = store.process_message(
                    "cloudv2-config",
                    "#24-PivotA_1-1-3600$",
                    ts=request["request_ts"] + 1,
                )
                self.assertTrue(response["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=request["request_ts"] + 2)
                config = snapshot["summary"]["reboot_config"]
                self.assertTrue(config["enabled"])
                self.assertEqual(config["reboot_timeout_sec"], 3600)
                self.assertFalse(config["pending"])
            finally:
                store.stop()

    def test_virtual_barrier_config_response_requires_prior_request(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            sent_messages = []
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)

                parsed, error = parse_device_payload("#26-PivotA_1-20-160-1-0$")
                self.assertIsNone(error)
                parsed_config = parse_virtual_barrier_config_payload(parsed)
                self.assertEqual(parsed_config["start_angle"], 20)
                self.assertEqual(parsed_config["end_angle"], 160)
                self.assertTrue(parsed_config["automatic_return"])
                self.assertFalse(parsed_config["water_return"])

                unsolicited = store.process_message(
                    "cloudv2-config",
                    "#26-PivotA_1-20-160-1-0$",
                    ts=1_773_171_010.0,
                )
                self.assertTrue(unsolicited["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=1_773_171_011.0)
                self.assertIsNone(snapshot["summary"]["virtual_barrier_config"]["start_angle"])

                store.set_pivot_config_sender(lambda topic, payload: sent_messages.append((topic, payload)) or True)
                request = store.send_config_request("PivotA_1", idp="26")
                self.assertEqual(request["payload"], "#26-PivotA_1$")
                self.assertEqual(sent_messages, [("PivotA_1", "#26-PivotA_1$")])

                response = store.process_message(
                    "cloudv2-config",
                    "#26-PivotA_1-20-160-1-0$",
                    ts=request["request_ts"] + 1,
                )
                self.assertTrue(response["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=request["request_ts"] + 2)
                config = snapshot["summary"]["virtual_barrier_config"]
                self.assertEqual(config["start_angle"], 20)
                self.assertEqual(config["end_angle"], 160)
                self.assertTrue(config["automatic_return"])
                self.assertFalse(config["water_return"])
                self.assertFalse(config["pending"])
            finally:
                store.stop()

    def test_comm_main_mode_config_response_requires_prior_request(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            sent_messages = []
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)

                parsed, error = parse_device_payload("#31-PivotA_1-COMM_MQTT$")
                self.assertIsNone(error)
                parsed_config = parse_comm_main_mode_config_payload(parsed)
                self.assertEqual(parsed_config["comm_main_mode"], "COMM_MQTT")

                unsolicited = store.process_message(
                    "cloudv2-config",
                    "#31-PivotA_1-COMM_MQTT$",
                    ts=1_773_171_010.0,
                )
                self.assertTrue(unsolicited["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=1_773_171_011.0)
                self.assertIsNone(snapshot["summary"]["comm_main_mode_config"]["comm_main_mode"])

                store.set_pivot_config_sender(lambda topic, payload: sent_messages.append((topic, payload)) or True)
                request = store.send_config_request("PivotA_1", idp="31")
                self.assertEqual(request["payload"], "#31-PivotA_1$")
                self.assertEqual(sent_messages, [("PivotA_1", "#31-PivotA_1$")])

                response = store.process_message(
                    "cloudv2-config",
                    "#31-PivotA_1-COMM_RF$",
                    ts=request["request_ts"] + 1,
                )
                self.assertTrue(response["accepted"])
                snapshot = store.get_pivot_snapshot("PivotA_1", now=request["request_ts"] + 2)
                config = snapshot["summary"]["comm_main_mode_config"]
                self.assertEqual(config["comm_main_mode"], "COMM_RF")
                self.assertFalse(config["pending"])
            finally:
                store.stop()

    def test_config_update_sends_full_payload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            sent_messages = []
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)
                store.set_pivot_config_sender(lambda topic, payload: sent_messages.append((topic, payload)) or True)

                network = store.send_config_update(
                    "PivotA_1",
                    idp="02",
                    values={
                        "gprs_id": "PivotA_1",
                        "modem_apn": "virtueyes.com.br",
                        "wifi_ssid": "Pivo_A",
                        "wifi_pass": "soiltech",
                    },
                )
                pivot = store.send_config_update(
                    "PivotA_1",
                    idp="03",
                    values={
                        "contactor": "NA",
                        "pressure": "NA",
                        "pressurization_time": "600",
                        "on_time": "2",
                        "off_time": "5",
                        "read_time": "10",
                    },
                )
                rush = store.send_config_update(
                    "PivotA_1",
                    idp="04",
                    values={
                        "start_time_hhmm": "0800",
                        "end_time_hhmm": "1830",
                        "enabled": "1",
                    },
                )
                sector = store.send_config_update(
                    "PivotA_1",
                    idp="05",
                    values={
                        "sector_number": "2",
                        "sectors": [
                            {"start_angle": "10", "end_angle": "90"},
                            {"start_angle": "120", "end_angle": "180"},
                        ],
                    },
                )
                physical_barrier = store.send_config_update(
                    "PivotA_1",
                    idp="22",
                    values={
                        "start_angle": "10",
                        "end_angle": "90",
                        "automatic_return": "1",
                        "water_return": "0",
                        "time_leaving_barrier": "5",
                    },
                )
                reboot = store.send_config_update(
                    "PivotA_1",
                    idp="24",
                    values={
                        "enabled": "1",
                        "reboot_timeout_sec": "3600",
                    },
                )
                virtual_barrier = store.send_config_update(
                    "PivotA_1",
                    idp="26",
                    values={
                        "start_angle": "20",
                        "end_angle": "160",
                        "automatic_return": "1",
                        "water_return": "0",
                    },
                )
                comm_main_mode = store.send_config_update(
                    "PivotA_1",
                    idp="31",
                    values={"comm_main_mode": "COMM_RF"},
                )

                self.assertEqual(network["payload"], "#02-PivotA_1-PivotA_1-virtueyes.com.br-Pivo_A-soiltech$")
                self.assertEqual(pivot["payload"], "#03-PivotA_1-NA-NA-600-2-5-10$")
                self.assertEqual(rush["payload"], "#04-PivotA_1-0800-1830-1$")
                self.assertEqual(sector["payload"], "#05-PivotA_1-2-10-90-120-180-0-0-0-0$")
                self.assertEqual(physical_barrier["payload"], "#22-PivotA_1-10-90-1-0-5$")
                self.assertEqual(reboot["payload"], "#24-PivotA_1-1-3600$")
                self.assertEqual(virtual_barrier["payload"], "#26-PivotA_1-20-160-1-0$")
                self.assertEqual(comm_main_mode["payload"], "#31-PivotA_1-COMM_RF$")
                self.assertEqual(
                    sent_messages,
                    [
                        ("PivotA_1", "#02-PivotA_1-PivotA_1-virtueyes.com.br-Pivo_A-soiltech$"),
                        ("PivotA_1", "#03-PivotA_1-NA-NA-600-2-5-10$"),
                        ("PivotA_1", "#04-PivotA_1-0800-1830-1$"),
                        ("PivotA_1", "#05-PivotA_1-2-10-90-120-180-0-0-0-0$"),
                        ("PivotA_1", "#22-PivotA_1-10-90-1-0-5$"),
                        ("PivotA_1", "#24-PivotA_1-1-3600$"),
                        ("PivotA_1", "#26-PivotA_1-20-160-1-0$"),
                        ("PivotA_1", "#31-PivotA_1-COMM_RF$"),
                    ],
                )
            finally:
                store.stop()

    def test_config_commands_accept_persisted_pivot_not_loaded_in_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            sent_messages = []
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)
                store.write()
                with store._lock:
                    store.pivots.pop("PivotA_1", None)

                store.set_pivot_config_sender(lambda topic, payload: sent_messages.append((topic, payload)) or True)
                request = store.send_config_request("PivotA_1", idp="03")
                update = store.send_config_update(
                    "PivotA_1",
                    idp="02",
                    values={
                        "gprs_id": "PivotA_1",
                        "modem_apn": "virtueyes.com.br",
                        "wifi_ssid": "Pivo_A",
                        "wifi_pass": "soiltech",
                    },
                )

                self.assertEqual(request["payload"], "#03-PivotA_1$")
                self.assertEqual(update["payload"], "#02-PivotA_1-PivotA_1-virtueyes.com.br-Pivo_A-soiltech$")
                self.assertEqual(
                    sent_messages,
                    [
                        ("PivotA_1", "#03-PivotA_1$"),
                        ("PivotA_1", "#02-PivotA_1-PivotA_1-virtueyes.com.br-Pivo_A-soiltech$"),
                    ],
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

    def test_shutdown_history_parses_and_deduplicates_idp28(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = self._build_store(temp_dir)
            try:
                store.queue_expected_pivots(["PivotA_1"], now=1_773_171_000.0, source="test")
                store.process_message("cloudv2", "#01-PivotA_1-discovery$", ts=1_773_171_001.0)

                first_payload = "#28-PivotA_1-soil_app-01-0-alex-0-124-15/05/2026_11:29:40$"
                second_payload = "#28-PivotA_1-actuation_app-30-0-manual-1-110-15/05/2026_16:01:08$"
                parsed, error = parse_device_payload(first_payload)
                self.assertIsNone(error)
                parsed_shutdown = parse_shutdown_reason_payload(parsed)
                self.assertEqual(parsed_shutdown["command_origin"], "soil_app")
                self.assertEqual(parsed_shutdown["shutdown_idp"], "01")
                self.assertFalse(parsed_shutdown["physical_barrier"])
                self.assertEqual(parsed_shutdown["position"], 124)

                self.assertTrue(
                    store.process_message("cloudv2-shutdown", first_payload, ts=1_773_171_010.0)["accepted"]
                )
                self.assertTrue(
                    store.process_message("cloudv2-error", second_payload, ts=1_773_171_030.0)["accepted"]
                )
                self.assertTrue(
                    store.process_message("cloudv2-shutdown", first_payload, ts=1_773_171_050.0)["accepted"]
                )

                snapshot = store.get_pivot_snapshot("PivotA_1", now=1_773_171_060.0)
                history = snapshot["summary"]["shutdown_history"]
                self.assertEqual(len(history), 2)
                self.assertEqual(history[0]["raw_payload"], second_payload)
                self.assertEqual(history[0]["topic"], "cloudv2-error")
                self.assertEqual(history[0]["command_origin_label"], "Controle interno da placa")
                self.assertEqual(history[0]["shutdown_idp_label"], "Desligamento manual")
                self.assertEqual(history[0]["physical_barrier_label"], "Sim, perto da barreira")
                self.assertEqual(history[1]["raw_payload"], first_payload)
                self.assertEqual(history[1]["command_origin_label"], "Aplicativo Soil")
                self.assertEqual(history[1]["shutdown_idp_label"], "Aplicativo externo")
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
