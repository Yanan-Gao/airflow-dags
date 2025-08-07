import functools
from abc import abstractmethod, ABCMeta
from functools import partial, reduce
from typing import TypeVar, Generic, Callable, Any, cast, Optional

from ttd.monads.either import Either, Left, Right

RT = TypeVar("RT", contravariant=True)
T = TypeVar("T", bound=RT)
R = TypeVar("R")


class Maybe(Generic[T], metaclass=ABCMeta):
    """Encapsulates an optional value.
    The Maybe type encapsulates an optional value. A value of type
    Maybe a either contains a value of (represented as Just a), or it is
    empty (represented as Nothing). Using Maybe is a good way to deal
    with errors or exceptional cases without resorting to drastic
    measures such as error.
    """

    @abstractmethod
    def get(self) -> T:
        pass

    @classmethod
    def empty(cls) -> "Maybe[T]":
        return Nothing()

    @abstractmethod
    def __add__(self, other: "Maybe[T]") -> "Maybe[T]":
        raise NotImplementedError

    @abstractmethod
    def map(self, mapper: Callable[[T], R]) -> "Maybe[R]":
        raise NotImplementedError

    @abstractmethod
    def apply(self: "Maybe[Callable[[T], R]]", something: "Maybe[T]") -> "Maybe[R]":
        raise NotImplementedError

    @abstractmethod
    def bind(self, fn: Callable[[T], "Maybe[R]"]) -> "Maybe[R]":
        raise NotImplementedError

    def flat_map(self, fn: Callable[[T], "Maybe[R]"]) -> "Maybe[R]":
        return self.bind(fn)

    @abstractmethod
    def as_optional(self) -> Optional[T]:
        pass

    def or_else(self, alternative: Callable[[], "Maybe[RT]"]) -> "Maybe[RT]":
        if self.is_nothing():
            return alternative()
        else:
            return self

    def to_right(self, left: Callable[[], R]) -> Either[R, T]:
        if self.is_nothing():
            return Left(left())
        else:
            return Right(self.get())

    @abstractmethod
    def is_just(self) -> bool:
        pass

    @abstractmethod
    def is_nothing(self) -> bool:
        pass

    @classmethod
    def concat(cls, xs):
        """concat :: [m] -> m
        Fold a list using the monoid. For most types, the default
        definition for mconcat will be used, but the function is
        included in the class definition so that an optimized version
        can be provided for specific types.
        """

        def reducer(a, b):
            return a + b

        return reduce(reducer, xs, cls.empty())

    def __rmod__(self, fn):
        """Infix version of map.
        Haskell: <$>
        Example:
        >>> (lambda x: x+2) % Just(40)
        42
        Returns a new Functor.
        """
        return self.map(fn)


class Just(Maybe[T]):
    """A Maybe that contains a value.
    Represents a Maybe that contains a value (represented as Just a).
    """

    def __init__(self, value: T) -> None:
        self._value = value

    def get(self) -> T:
        return self._value

    # Monoid Section
    # ==============

    def __add__(self, other: Maybe[T]) -> Maybe[T]:
        # m `append` Nothing = m
        if isinstance(other, Nothing):
            return self

        # Just m1 `append` Just m2 = Just (m1 `append` m2)
        return other.map(
            lambda other_value: cast(Any, self._value) + other_value if hasattr(self._value, "__add__") else Nothing()  # type: ignore
        )

    # Functor Section
    # ===============

    def map(self, mapper: Callable[[T], R]) -> Maybe[R]:
        # fmap f (Just x) = Just (f x)

        result = mapper(self._value)

        return Just(result)

    # Applicative Section
    # ===================

    @classmethod
    def pure(cls, value: Callable[[T], R]) -> "Just[Callable[[T], R]]":
        return Just(value)

    def apply(self: "Just[Callable[[T], R]]", something: Maybe[T]) -> Maybe[R]:

        def mapper(other_value):
            try:
                return self._value(other_value)
            except TypeError:
                return partial(self._value, other_value)

        return something.map(mapper)

    # Monad Section
    # =============

    @classmethod
    def unit(cls, value: T) -> Maybe[T]:
        return Just(value)

    def bind(self, func: Callable[[T], Maybe[R]]) -> Maybe[R]:
        """Just x >>= f = f x."""

        value = self._value
        return func(value)

    # Utilities Section
    # =================

    def is_just(self) -> bool:
        return True

    def is_nothing(self) -> bool:
        return False

    def as_optional(self) -> Optional[T]:
        return self._value

    # Operator Overloads Section
    # ==========================

    def __bool__(self) -> bool:
        """Return True."""
        return True

    def __eq__(self, other) -> bool:
        """Return self == other."""

        if isinstance(other, Nothing):
            return False

        return bool(other.flat_map(lambda other_value: other_value == self._value))

    def __str__(self) -> str:
        return "Just(%s)" % self._value

    def __repr__(self) -> str:
        return str(self)


class Nothing(Maybe[T]):
    """Represents an empty Maybe.
    Represents an empty Maybe that holds nothing (in which case it has
    the value of Nothing).
    """
    _unit = None

    def get(self) -> T:
        raise TypeError("Nothing.get")

    # Monoid Section
    # ==============

    def __add__(self, other: Maybe) -> Maybe:
        # m `append` Nothing = m
        return other

    # Functor Section
    # ===============

    def map(self, mapper: Callable[[T], R]) -> Maybe[R]:
        return Nothing()

    # Applicative Section
    # ===================

    @classmethod
    def pure(cls) -> Maybe[Callable[[T], R]]:
        return Nothing()

    def apply(self: "Nothing[Callable[[T], R]]", something: Maybe[T]) -> Maybe[R]:
        return Nothing()

    # Monad Section
    # =============

    @classmethod
    @functools.cache
    def unit(cls) -> Maybe[T]:
        if cls._unit is None:
            cls._unit = cls()
        return cls._unit

    def bind(self, func: Callable[[T], Maybe[R]]) -> Maybe[R]:
        """Nothing >>= f = Nothing
        Nothing in, Nothing out.
        """

        return Nothing()

    # Utilities Section
    # =================

    def is_just(self) -> bool:
        return False

    def is_nothing(self) -> bool:
        return True

    def as_optional(self) -> Optional[T]:
        return None

    # Operator Overloads Section
    # ==========================
    def __bool__(self) -> bool:
        return False

    def __eq__(self, other) -> bool:
        return isinstance(other, Nothing)

    def __str__(self) -> str:
        return "Nothing"

    def __repr__(self) -> str:
        return str(self)
