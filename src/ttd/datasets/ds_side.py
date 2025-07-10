from abc import ABC


class DsSide(ABC):

    def __eq__(self, other):
        return isinstance(self, type(other))


class DsReadSide(DsSide):
    pass


class DsWriteSide(DsSide):
    pass


class DsSides:
    read: DsReadSide = DsReadSide()
    write: DsWriteSide = DsWriteSide()
