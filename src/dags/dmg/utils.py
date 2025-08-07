import logging


def get_jar_file_path(jar_file_default: str):
    jar_file = f"{{{{ dag_run.conf.get('jar_file', '{jar_file_default}') }}}}"
    logging.info('The following jar_path was used: ' + jar_file)
    return jar_file
