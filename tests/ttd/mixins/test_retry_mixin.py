import logging
import unittest

from ttd.mixins.retry_mixin import RetryMixin, RetryLimitException


class RetryMixinTest(unittest.TestCase):

    def test_calc_sleep_for_exponential_retry(self):
        for i in range(0, 10):
            interval = RetryMixin.calc_sleep(attempt_number=6, retry_interval=2, exponential_retry=True)
            self.assertGreater(interval, 32)
            self.assertLessEqual(interval, 64)

    def test_it_retry_and_fail(self):
        counter = 0

        ex = Exception("problem")

        def action():
            nonlocal counter
            logging.info("Action")
            counter = counter + 1
            raise ex

        def condition(ex):
            logging.info("Condition: ", exc_info=ex)
            return True

        mx = RetryMixin(max_retries=2, retry_interval=2)
        result = mx.with_retry(action, condition)
        self.assertEqual(counter, 3)
        self.assertIsInstance(result.failed().get(), RetryLimitException)
        self.assertEqual(result.failed().get().nested_exception, ex)
