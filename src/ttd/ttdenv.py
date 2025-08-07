from abc import ABC
from typing import Optional


class TtdEnv(ABC):

    def __init__(self, env: str, ds_read_env: str, ds_write_env: str, **kwargs):
        if not kwargs.pop("safe", False):
            raise TypeError("Use predefined environments instead: TtdEnv.prod, TtdEnv.prodTest, TtdEnv.test, TtdEnv.staging, TtdEnv.dev")
        self._env = env
        self._ds_read_env = ds_read_env
        self._ds_write_env = ds_write_env

    @property
    def execution_env(self) -> str:
        return self._env

    @property
    def dataset_read_env(self) -> str:
        return self._ds_read_env

    @property
    def dataset_write_env(self) -> str:
        return self._ds_write_env

    def __str__(self) -> str:
        return self.execution_env

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TtdEnv):
            return NotImplemented
        return self._env == other._env and self._ds_read_env == other._ds_read_env and self._ds_write_env == other._ds_write_env

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)


class ProdEnv(TtdEnv):

    def __init__(self):
        super().__init__("prod", ds_read_env="prod", ds_write_env="prod", safe=True)


class ProdTestEnv(TtdEnv):

    def __init__(self):
        super().__init__("prodTest", ds_read_env="prod", ds_write_env="test", safe=True)


class StagingEnv(TtdEnv):

    def __init__(self):
        super().__init__("staging", ds_read_env="prod", ds_write_env="test", safe=True)


class TestEnv(TtdEnv):

    def __init__(self):
        super().__init__("test", ds_read_env="test", ds_write_env="test", safe=True)


class DevEnv(TtdEnv):

    def __init__(self):
        super().__init__("dev", ds_read_env="test", ds_write_env="test", safe=True)


class TtdEnvFactory:
    prod: "ProdEnv" = ProdEnv()
    prodTest: "ProdTestEnv" = ProdTestEnv()
    staging: "StagingEnv" = StagingEnv()
    test: "TestEnv" = TestEnv()
    dev: "DevEnv" = DevEnv()

    _system_env: Optional[TtdEnv] = None

    @classmethod
    def get_from_system(cls) -> TtdEnv:
        """
        Gets environment configuration from the Airflow variable configured for the current deployment
        """
        if cls._system_env is None:
            system_env = ""
            try:
                from airflow.models import Variable

                system_env = Variable.get("ENVIRONMENT", default_var="dev")
            except ImportError:
                pass
            cls._system_env = cls.get_from_str(system_env)

        return cls._system_env

    @classmethod
    def get_from_str(cls, env_str: str) -> TtdEnv:
        match env_str.lower():
            case "prod":
                return cls.prod
            case "prodtest":
                return cls.prodTest
            case "staging":
                return cls.staging
            case "test":
                return cls.test
            case "dev":
                return cls.dev
            case _:
                return cls.dev
