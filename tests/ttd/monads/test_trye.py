import unittest

from ttd.monads.trye import Success, Failure, Try


class TryTest(unittest.TestCase):

    def test_Success_is_truthy_value(self):
        s = Success.unit(10)
        self.assertTrue(bool(s), "Success is convertible to bool")
        self.assertTrue(True if s else False, "Success is truthy in if expression")

        r = False
        if s:
            r = True
        self.assertTrue(r, "Success is truthy in if statement")

    def test_Failure_is_falsy_value(self):
        f = Failure.unit(Exception("Test"))
        self.assertFalse(bool(f), "Failure is convertible to bool")
        self.assertFalse(True if f else False, "Failure is falsy in if expression")

        r = True
        if not f:
            r = False
        self.assertFalse(r, "Failure is falsy in if statement")

    def test_left_identity_Success(self):

        def fn(v: int) -> Success[int]:
            return Success.unit(v + 10)

        a = 10
        lhs = Success.unit(a).flat_map(fn)
        rhs = fn(a)
        self.assertEqual(lhs, rhs)

    def test_right_identity_Success(self):
        s = Success.unit(11)
        lhs = s.flat_map(lambda a: Success(a))
        rhs = s
        self.assertEqual(lhs, rhs)

    def test_associativity_Success(self):

        def fn(v: int) -> Try[int]:
            return Success.unit(v + 10) if v < 30 else Failure(Exception("test"))

        def gn(v: int) -> Try[int]:
            return Success.unit(v - 5) if v > 10 else Failure(Exception("test"))

        s = Success(20)
        lhs = s.flat_map(fn).flat_map(gn)
        rhs = s.flat_map(lambda a: fn(a).flat_map(gn))
        self.assertEqual(lhs, rhs)

    def test_map_on_Failure_is_Failure(self):

        def fn(v):
            return v + 10

        f = Failure(Exception("Test"))
        result = f.map(fn)
        self.assertEqual(f, result)

    def test_map_on_Success_is_Success(self):

        def fn(v):
            return v + 10

        s = Success(20)
        result = s.map(fn)
        self.assertEqual(Success(30), result)

    def test_try_apply_on_failing(self):
        # noinspection PyUnreachableCode
        def fn(v):
            raise Exception("Error")
            return v + 1

        t = Try.apply(lambda: fn(10))
        self.assertTrue(t.is_failure, "Result should be Failure")
        ex = t.failed().get()
        self.assertTrue(isinstance(ex, Exception))
