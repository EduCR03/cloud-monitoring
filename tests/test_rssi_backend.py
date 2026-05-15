import os
import tempfile
import unittest

from backend.cloudv2_persistence import TelemetryPersistence


class RssiPanelPayloadTests(unittest.TestCase):
    def test_panel_payload_returns_only_valid_rssi_series(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "telemetry.sqlite3")
            persistence = TelemetryPersistence(db_path=db_path, max_events_per_pivot=5000)
            persistence.start()
            try:
                pivot_id = "PivotA_1"
                base_ts = 1_700_000_000.0

                run = persistence.get_or_create_active_run(now_ts=base_ts, source="test")
                session = persistence.get_or_create_active_session(
                    pivot_id,
                    pivot_slug="pivota-1",
                    now_ts=base_ts,
                    source="test",
                    run_id=run["run_id"],
                )
                session_id = session["session_id"]

                persistence.upsert_snapshot(
                    pivot_id,
                    session_id,
                    {
                        "pivot_id": pivot_id,
                        "session_id": session_id,
                        "run_id": run["run_id"],
                        "updated_at_ts": base_ts + 1,
                        "summary": {
                            "status": {"code": "green"},
                            "quality": {"code": "green"},
                        },
                    },
                    updated_at_ts=base_ts + 1,
                )

                persistence.insert_ping_rssi_point(pivot_id, session_id, ts=base_ts + 10, rssi=12)
                persistence.insert_ping_rssi_point(pivot_id, session_id, ts=base_ts + 11, rssi=-1)
                persistence.insert_ping_rssi_point(pivot_id, session_id, ts=base_ts + 12, rssi=99)
                persistence.insert_ping_rssi_point(pivot_id, session_id, ts=base_ts + 13, rssi=31)

                payload = persistence.get_panel_payload(
                    pivot_id,
                    session_id=session_id,
                    run_id=run["run_id"],
                )

                self.assertIsNotNone(payload)
                self.assertTrue(payload["hasRssi"])
                self.assertEqual(
                    [(item["ts"], item["rssi"]) for item in payload["rssiSeries"]],
                    [
                        (base_ts + 10, 12),
                        (base_ts + 13, 31),
                    ],
                )
            finally:
                persistence.stop()

    def test_panel_payload_keeps_latest_rssi_points_when_limited(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "telemetry.sqlite3")
            persistence = TelemetryPersistence(db_path=db_path, max_events_per_pivot=3)
            persistence.start()
            try:
                pivot_id = "PivotA_1"
                base_ts = 1_700_000_000.0

                run = persistence.get_or_create_active_run(now_ts=base_ts, source="test")
                session = persistence.get_or_create_active_session(
                    pivot_id,
                    pivot_slug="pivota-1",
                    now_ts=base_ts,
                    source="test",
                    run_id=run["run_id"],
                )
                session_id = session["session_id"]

                persistence.upsert_snapshot(
                    pivot_id,
                    session_id,
                    {
                        "pivot_id": pivot_id,
                        "session_id": session_id,
                        "run_id": run["run_id"],
                        "updated_at_ts": base_ts + 10,
                        "summary": {
                            "status": {"code": "green"},
                            "quality": {"code": "green"},
                        },
                    },
                    updated_at_ts=base_ts + 10,
                )

                for index, rssi in enumerate([10, 11, 12, 13, 14], start=1):
                    persistence.insert_ping_rssi_point(pivot_id, session_id, ts=base_ts + index, rssi=rssi)

                points = persistence.fetch_ping_rssi_points(
                    pivot_id,
                    session_id,
                    limit=3,
                )
                self.assertEqual(
                    [(item["ts"], item["rssi"]) for item in points],
                    [
                        (base_ts + 3, 12),
                        (base_ts + 4, 13),
                        (base_ts + 5, 14),
                    ],
                )
            finally:
                persistence.stop()

    def test_rssi_window_samples_latest_point_per_hour(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "telemetry.sqlite3")
            persistence = TelemetryPersistence(db_path=db_path, max_events_per_pivot=100)
            persistence.start()
            try:
                pivot_id = "PivotA_1"
                base_ts = 1_700_000_000.0

                run = persistence.get_or_create_active_run(now_ts=base_ts, source="test")
                session = persistence.get_or_create_active_session(
                    pivot_id,
                    pivot_slug="pivota-1",
                    now_ts=base_ts,
                    source="test",
                    run_id=run["run_id"],
                )
                session_id = session["session_id"]

                samples = [
                    (base_ts + 10, 10),
                    (base_ts + 600, 11),
                    (base_ts + 3700, 12),
                    (base_ts + 4200, 13),
                    (base_ts + 7300, 14),
                ]
                for ts_value, rssi in samples:
                    persistence.insert_ping_rssi_point(pivot_id, session_id, ts=ts_value, rssi=rssi)

                points = persistence.fetch_ping_rssi_points_window(
                    pivot_id,
                    session_id,
                    start_ts=base_ts,
                    end_ts=base_ts + 8000,
                    bucket_sec=3600,
                )

                self.assertEqual(
                    [(item["ts"], item["rssi"]) for item in points],
                    [
                        (base_ts + 600, 11),
                        (base_ts + 4200, 13),
                        (base_ts + 7300, 14),
                    ],
                )
            finally:
                persistence.stop()


if __name__ == "__main__":
    unittest.main()
