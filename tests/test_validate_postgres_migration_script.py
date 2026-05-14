import unittest

from scripts.validate_postgres_migration import _compare_json, _normalize_payload


class ValidatePostgresMigrationScriptTests(unittest.TestCase):
    def test_normalize_payload_sorts_dict_keys(self):
        payload = {"b": 2, "a": {"d": 4, "c": 3}}
        normalized = _normalize_payload(payload)
        self.assertEqual(list(normalized.keys()), ["a", "b"])
        self.assertEqual(list(normalized["a"].keys()), ["c", "d"])

    def test_compare_json_detects_match(self):
        result = _compare_json("sample", {"b": 2, "a": 1}, {"a": 1, "b": 2})
        self.assertTrue(result["match"])
        self.assertEqual(result["label"], "sample")

    def test_compare_json_detects_mismatch(self):
        result = _compare_json("sample", {"a": 1}, {"a": 2})
        self.assertFalse(result["match"])
        self.assertEqual(result["left"]["a"], 1)
        self.assertEqual(result["right"]["a"], 2)


if __name__ == "__main__":
    unittest.main()
