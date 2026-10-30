import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from busmap_all_v19 import app, _minutes_from_value, normalize_stop_timetable


class MinutesFromValueTests(unittest.TestCase):
    def test_parses_numeric_and_greek_minute_values(self):
        self.assertEqual(_minutes_from_value(12), 12)
        self.assertEqual(_minutes_from_value("7 λεπτά"), 7)

    def test_clamps_small_negative_values_to_now(self):
        self.assertEqual(_minutes_from_value(-2), 0)

    def test_returns_none_for_unrecognized_values(self):
        self.assertIsNone(_minutes_from_value("άγνωστο"))

    def test_parses_iso_utc_timestamp(self):
        target_utc = datetime.now(timezone.utc) + timedelta(minutes=15, seconds=10)
        iso_str = target_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        mins = _minutes_from_value(iso_str)
        self.assertIn(mins, (14, 15, 16))


class ApiArrivalsTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    @patch("busmap_all_v19.get_json")
    def test_raw_omitted_by_default_and_included_with_debug(self, mock_get_json):
        mock_get_json.return_value = {
            "data": {
                "arrivals": [
                    {
                        "routeShortName": "31",
                        "destination": "Βούλγαρη",
                        "arrivalInMinutes": 5,
                    }
                ]
            }
        }

        # Χωρίς debug: δεν πρέπει να υπάρχει το "raw"
        res = self.client.get("/api/arrivals?stop_id=1234")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertTrue(data["ok"])
        self.assertNotIn("raw", data)
        self.assertEqual(len(data["arrivals"]), 1)

        # Με debug=1: πρέπει να περιλαμβάνεται το "raw"
        res_debug = self.client.get("/api/arrivals?stop_id=1234&debug=1")
        self.assertEqual(res_debug.status_code, 200)
        data_debug = res_debug.get_json()
        self.assertIn("raw", data_debug)


class NormalizeStopTimetableTests(unittest.TestCase):
    def test_normalizes_arrivals_and_sorts_by_minutes(self):
        payload = {
            "data": {
                "arrivals": [
                    {
                        "routeShortName": "10",
                        "destination": "Κέντρο",
                        "vehicleId": "123",
                        "arrivalInMinutes": 8,
                    },
                    {
                        "routeShortName": "5",
                        "destination": "Σταθμός",
                        "vehicleId": "456",
                        "arrivalInMinutes": 3,
                    },
                ]
            }
        }

        self.assertEqual(
            normalize_stop_timetable(payload),
            [
                {
                    "line": "5",
                    "headsign": "Σταθμός",
                    "vehicle": "456",
                    "minutes": 3,
                    "arrival_time": "",
                },
                {
                    "line": "10",
                    "headsign": "Κέντρο",
                    "vehicle": "123",
                    "minutes": 8,
                    "arrival_time": "",
                },
            ],
        )

    def test_ignores_empty_rows(self):
        self.assertEqual(normalize_stop_timetable({"arrivals": [{}]}), [])


class ApiLiveTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    @patch("busmap_all_v19.get_route_info")
    @patch("busmap_all_v19.load_routes")
    def test_line_mode_returns_stops_shapes_and_vehicles(self, mock_load_routes, mock_get_route_info):
        mock_load_routes.return_value = [
            {
                "shortName": "31",
                "longName": "ΒΟΥΛΓΑΡΗ - Κ.Τ.Ε.Λ.",
                "color": "#1f6feb",
                "tripHeadsigns": [
                    {
                        "routeId": "r31",
                        "shapeId": "s31_1",
                        "headsign": "ΠΡΟΣ Κ.Τ.Ε.Λ.",
                    }
                ],
            }
        ]
        mock_get_route_info.return_value = {
            "headsign": "ΠΡΟΣ Κ.Τ.Ε.Λ.",
            "shape": {
                "lineString": "LINESTRING(22.94 40.64, 22.95 40.65)",
            },
            "vehicles": [
                {
                    "id": "1801",
                    "latitude": 40.642,
                    "longitude": 22.945,
                    "bearing": 120.0,
                }
            ],
            "stops": [
                {
                    "id": "01042",
                    "code": "01042",
                    "name": "ΑΡΙΣΤΟΤΕΛΟΥΣ",
                    "latitude": 40.635,
                    "longitude": 22.941,
                    "sequence": 1,
                }
            ],
        }

        res = self.client.get("/api/live?line=31")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["mode"], "line")
        self.assertEqual(data["line"], "31")
        self.assertEqual(len(data["stops"]), 1)
        self.assertEqual(data["stops"][0]["id"], "01042")
        self.assertEqual(data["stops"][0]["name"], "ΑΡΙΣΤΟΤΕΛΟΥΣ")
        self.assertEqual(len(data["shapes"]), 1)
        self.assertEqual(len(data["vehicles"]), 1)


if __name__ == "__main__":
    unittest.main()
