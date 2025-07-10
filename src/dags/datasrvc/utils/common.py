from ttd.ttdenv import TtdEnvFactory


def is_prod() -> bool:
    return True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False
