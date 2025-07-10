from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar, Callable, Any, Optional

from ttd.monads.maybe import Maybe, Nothing, Just

T = TypeVar("T")
R = TypeVar("R")


class Try(Generic[T], metaclass=ABCMeta):
    """
    The Try type represents a computation that may either result in an exception, or return a successfully computed value.
    It's similar to, but semantically different from the Either type.
    Instances of Try[T], are either an instance of Success[T] or Failure[T].
    For example, Try can be used to perform division on a user-defined input, without the need to do explicit exception-handling in all
    of the places that an exception might occur.

    An important property of Try shown in the above example is its ability to pipeline, or chain, operations,
    catching exceptions along the way. The flatMap and map combinators in the above example each essentially pass off either their
    successfully completed value, wrapped in the Success type for it to be further operated upon by the next combinator in the chain,
    or the exception wrapped in the Failure type usually to be simply passed on down the chain.
    Combinators such as recover and recoverWith are designed to provide some type of default behavior in the case of failure.
    """

    def __init__(self, is_failure: bool, is_success: bool):
        if is_failure and is_success or not is_failure and not is_success:
            raise Exception("is_failure and is_success can't be the same!")
        self._is_failure = is_failure
        self._is_success = is_success

    @property
    def is_failure(self) -> bool:
        """
        Returns true if the Try is a Failure, false otherwise.
        """
        return self._is_failure

    @property
    def is_success(self) -> bool:
        """
        Returns `true` if the `Try` is a `Success`, `false` otherwise.
        """
        return self._is_success

    @abstractmethod
    def get(self) -> T:
        """
        Returns the value from this `Success` or throws the exception if this is a `Failure`.
        """
        pass

    @abstractmethod
    def map(self, f: Callable[[T], R]) -> "Try[R]":
        """
        Maps the given function to the value from this `Success` or returns this if this is a `Failure`.
        """
        pass

    @abstractmethod
    def flat_map(self, f: Callable[[T], "Try[R]"]) -> "Try[R]":
        """
        Returns the given function applied to the value from this `Success` or returns this if this is a `Failure`.
        """
        pass

    @abstractmethod
    def failed(self) -> "Try[Exception]":
        """
        Inverts this Try. If this is a Failure, returns its exception wrapped in a Success.
        If this is a Success, returns a Failure containing an UnsupportedOperationException.
        """
        pass

    @abstractmethod
    def as_optional(self) -> Optional[T]:
        """
        If `Success` return value, otherwise return `None`
        :return:
        """
        pass

    @abstractmethod
    def as_maybe(self) -> Maybe[T]:
        pass

    @abstractmethod
    def __bool__(self) -> bool:
        pass

    @classmethod
    @abstractmethod
    def unit(cls, a: T) -> "Try[T]":
        pass

    @staticmethod
    def apply(f: Callable[[], T]) -> "Try[T]":
        """
        Applies Try to the result of the provided callable `f`. If it's `Exception`, then Failure will be returned, otherwise `Success`
        :param f:
        :return:
        """
        try:
            return Success(f())
        except Exception as e:
            return Failure(e)

    @abstractmethod
    def __getitem__(self, item: int) -> T:
        pass


class Failure(Try[T]):

    def __init__(self, exception: Exception):
        super().__init__(is_failure=True, is_success=False)
        self._exception = exception

    def get(self) -> T:
        raise self._exception

    def map(self, f: Callable[[T], R]) -> Try[R]:
        return self

    def flat_map(self, f: Callable[[T], Try[R]]) -> Try[R]:
        return self

    def failed(self) -> "Try[Exception]":
        return Success(self._exception)

    def as_optional(self) -> Optional[T]:
        return None

    def as_maybe(self) -> Maybe[T]:
        return Nothing.unit()

    def __bool__(self) -> bool:
        return False

    @classmethod
    def unit(cls, e: Exception) -> "Failure[T]":
        return Failure(e)

    def __eq__(self, other: Any) -> bool:
        """Return self == other. This comparison is somewhat questionable as only check instance equality of Exception"""

        if not isinstance(other, Failure):
            return False

        return bool(other._exception == self._exception)

    def __str__(self) -> str:
        return f"Failure[{self._exception.__class__.__name__}]"

    def __repr__(self) -> str:
        return str(self)

    def __getitem__(self, item: int) -> T:
        raise IndexError()


class Success(Try[T]):

    def __init__(self, value: T):
        super().__init__(is_failure=False, is_success=True)
        self._value = value

    def get(self) -> T:
        return self._value

    def map(self, f: Callable[[T], R]) -> "Try[R]":
        return Try.apply(lambda: f(self._value))

    def flat_map(self, f: Callable[[T], "Try[R]"]) -> Try[R]:
        try:
            return f(self._value)
        except Exception as e:
            return Failure(e)

    def failed(self) -> "Try[Exception]":
        return Failure(UnsupportedOperationException("Success.failed()"))

    def as_optional(self) -> Optional[T]:
        return self._value

    def as_maybe(self) -> Maybe[T]:
        return Just.unit(self._value)

    def __bool__(self) -> bool:
        return True

    @classmethod
    def unit(cls, a: T) -> "Success[T]":
        return Success(a)

    def __eq__(self, other) -> bool:
        """Return self == other."""

        if not isinstance(other, Success):
            return False

        return other._value == self._value

    def __str__(self) -> str:
        return f"Success({self._value})"

    def __repr__(self) -> str:
        return str(self)

    def __getitem__(self, item: int) -> T:
        if item == 0:
            return self._value
        else:
            raise IndexError()


class UnsupportedOperationException(Exception):

    def __init__(self, message: str):
        super().__init__(message)
