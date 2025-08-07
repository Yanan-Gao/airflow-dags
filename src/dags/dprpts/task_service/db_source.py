class DBSource:

    def get_datasource(self) -> str | None:
        pass


class Production(DBSource):

    def get_datasource(self) -> None:
        return None


class CloudSpin(DBSource):

    def __init__(self, ip: str):
        self._ip = ip

    def get_datasource(self) -> str:
        return self._ip


class SandBox(DBSource):

    def __init__(self):
        self._ip = "dip-provdb.adsrvr.org"

    def get_datasource(self) -> str:
        return self._ip
