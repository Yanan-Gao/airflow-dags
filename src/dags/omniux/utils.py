import logging

jar_default = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/cloudsmith/libs-staging/com/thetradedesk/ctv/upstreaminsights_2.12/0.1.0-SNAPSHOT/upstreaminsights_2.12-0.1.0-20250731.060201-7-assembly.jar"


def get_jar_file_path(jar_file_default: str = jar_default):
    jar_file = f"{{{{ dag_run.conf.get('jar_file', '{jar_file_default}') }}}}"
    logging.info('The following jar_path was used: ' + jar_file)
    return jar_file
