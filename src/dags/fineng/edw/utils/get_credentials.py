import logging
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from ttd.ttdenv import TtdEnvFactory


def credentials(name):
    env = str(TtdEnvFactory.get_from_system())
    logging.info(f"Environment: {env}")
    try:
        logging.info(name + '_' + env.strip())
        return Variable.get(name + '_' + env.strip())
    except:
        raise AirflowFailException("Unable to retrieve Secret")
