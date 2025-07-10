class TtdPlatform:
    _platform = None

    @classmethod
    def get_from_system(cls) -> str:
        if cls._platform is None:
            from airflow.models import Variable

            platform = Variable.get("PLATFORM", default_var="airflow2")

            cls._platform = cls.get_from_string(platform)

        return cls._platform

    @staticmethod
    def get_from_string(platform: str) -> str:
        if platform != "airflow2":
            raise ValueError("Platform must be 'airflow2'")
        else:
            return platform
