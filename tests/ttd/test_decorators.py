import unittest
from typing import Optional, Any, Dict
from ttd.decorators import return_on_key_error


class TestReturnOnKeyErrorDecorator(unittest.TestCase):

    def test_no_effect_when_key_present(self):

        @return_on_key_error(default_value="default")
        def get_value(data: Dict, key: str) -> Any:
            return data[key]

        self.assertEqual(get_value({"a": 1}, "a"), 1)
        self.assertEqual(get_value({"b": 2}, "b"), 2)

    def test_provided_default_value_returned_when_key_not_present(self):

        @return_on_key_error(default_value="default")
        def get_value(data: dict, key: str) -> Any:
            return data[key]

        self.assertEqual(get_value({"a": 1}, "b"), "default")
        self.assertEqual(get_value({"b": 2}, "missing_key"), "default")

    def test_none_returned_when_key_not_present_and_no_default_provided(self):

        @return_on_key_error()
        def get_value(data: dict, key: str) -> Optional[Any]:
            return data[key]

        self.assertIsNone(get_value({"a": 1}, "b"))
        self.assertIsNone(get_value({}, "key"))


if __name__ == "__main__":
    unittest.main()
