import unittest

from busmap_all_v19 import _minutes_from_value, normalize_stop_timetable


class MinutesFromValueTests(unittest.TestCase):
    def test_parses_numeric_and_greek_minute_values(self):
        self.assertEqual(_minutes_from_value(12), 12)
        self.assertEqual(_minutes_from_value("7 λεπτά"), 7)

    def test_clamps_small_negative_values_to_now(self):
        self.assertEqual(_minutes_from_value(-2), 0)

    def test_returns_none_for_unrecognized_values(self):
        self.assertIsNone(_minutes_from_value("άγνωστο"))


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


if __name__ == "__main__":
    unittest.main()
