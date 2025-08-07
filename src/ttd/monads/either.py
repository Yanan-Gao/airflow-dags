from typing import Generic, TypeVar


class Monad:
    # pure :: a -> M a
    @staticmethod
    def pure(x):
        raise Exception("pure method needs to be implemented")

    # flat_map :: # M a -> (a -> M b) -> M b
    def flat_map(self, f):
        raise Exception("flat_map method needs to be implemented")

    # map :: # M a -> (a -> b) -> M b
    def map(self, f):
        return self.flat_map(lambda x: self.pure(f(x)))


A = TypeVar("A")
B = TypeVar("B")


class Either(Monad, Generic[A, B]):
    is_left: bool
    is_right: bool
    a: A
    b: B

    # pure :: a -> Either a
    @staticmethod
    def pure(value):
        return Right(value)

    # flat_map :: # Either a -> (a -> Either b) -> Either b
    def flat_map(self, f):
        if self.is_left:
            return self
        else:
            return f(self.b)

    @property
    def left(self) -> A:
        if self.is_left:
            return self.a
        else:
            raise ValueError("left called on Right!")

    @property
    def right(self) -> B:
        if self.is_right:
            return self.b
        else:
            raise ValueError("right called on Left!")


class Left(Either[A, B]):

    def __init__(self, value: A):
        self.a: A = value
        self.is_left = True
        self.is_right = False


class Right(Either[A, B]):

    def __init__(self, value: B):
        self.b: B = value
        self.is_left = False
        self.is_right = True
