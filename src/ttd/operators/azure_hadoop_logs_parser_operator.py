import logging
import re
import tempfile
import os
import subprocess

from typing import TYPE_CHECKING, List

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder, CloudStorage
from ttd.kubernetes.pod_resources import PodResources
from ttd.workers.worker import Workers

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureHadoopLogsParserOperator(BaseOperator):
    """
    An Airflow operator for parsing Hadoop logs stored in Azure Blob Storage.

    @param task_id: The ID of the Airflow task.
    @type task_id: str
    @param app_id: Identifier for the application, used to correlate related logs.
    @type app_id: str
    @param cluster_name: The name of the cluster.
    @type cluster_name: str
    """
    log_folder_prefix = "/app-logs/livy/bucket-logs-tfile/"
    parser_container_name = "ttd-build-artefacts"
    parser_executable_path = "eldorado-extra/hadooplogparser-1.1.jar"
    build_artefacts_storage_account_name = "ttdartefacts"
    logs_storage_account_name = "ttdeldorado"
    parsed_logs_folder_name = "parsed"

    STEP_NAME = 'parse_logs_task'

    template_fields = ['app_id', 'cluster_name']

    @apply_defaults
    def __init__(
        self,
        task_id: str,
        app_id: str,
        cluster_name: str,
        print_parsed_spark_driver_logs: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            queue=Workers.k8s.queue,
            pool=Workers.k8s.pool,
            executor_config=PodResources(
                request_cpu="500m",
                request_memory="2.5Gi",
                limit_memory="3.75Gi",
                limit_ephemeral_storage="5Gi",
            ).as_executor_config(),
            *args,
            **kwargs
        )

        self.app_id = app_id
        self.cluster_name = cluster_name
        self.print_parsed_spark_driver_logs = print_parsed_spark_driver_logs

    def execute(self, context: "Context"):
        try:
            self.log_container_name = f"{self.cluster_name}-container"
            if len(self.app_id) < 4:
                raise ValueError("app_id must be at least 4 characters long")
            self.batch_id = self.app_id[-4:]

            logging.info(f"app_id={self.app_id}")
            logging.info(f"log_container_name={self.log_container_name}")
            with tempfile.TemporaryDirectory() as temp_folder:
                output_folder = os.path.join(temp_folder, self.parsed_logs_folder_name)

                logging.info("Initialize Azure Cloud Storage Client.")
                cloud_storage = CloudStorageBuilder(CloudProviders.azure).build()

                logging.info("Download parser executable.")
                parser_local_path = os.path.join(temp_folder, "hadooplogparser.jar")
                cloud_storage.download_file(
                    file_path=parser_local_path,
                    key=self.parser_executable_path,
                    bucket_name=f"{self.parser_container_name}@{self.build_artefacts_storage_account_name}",
                )

                app_folder = os.path.join(self.log_container_name, self.log_folder_prefix, self.batch_id, self.app_id)

                logging.info(f"Extracting the logs from {app_folder}.")

                keys = cloud_storage.list_keys(
                    prefix=app_folder,
                    bucket_name=f"{self.log_container_name}@{self.logs_storage_account_name}",
                )

                log_files = keys[1:]
                logging.info(f"Length of logs files {len(log_files)}.")
                logging.info(f"Logs files are {log_files}.")

                app_output_folder = os.path.join(output_folder, self.app_id)

                self._process_application_folder_logs(
                    app_folder=app_folder,
                    log_files=log_files,
                    cloud_storage=cloud_storage,
                    parser_local_path=parser_local_path,
                    output_folder=app_output_folder,
                )
        except Exception as e:
            from ttd.metrics.opentelemetry.ttdopentelemetry import TtdGauge, get_or_register_gauge

            logging.error(f"Exception while parsing logs: {e}")

            hdi_logs_parser_errors_metric: TtdGauge = get_or_register_gauge(
                job=context["dag"].dag_id,
                name="airflow_hdi_logs_parser_errors",
                description="Tracks the number of errors encountered while parsing HDI logs",
            )
            hdi_logs_parser_errors_metric.set(1)

    def _process_application_folder_logs(
        self,
        app_folder: str,
        log_files: List[str],
        cloud_storage: CloudStorage,
        parser_local_path: str,
        output_folder: str,
    ) -> None:
        found_driver_logs = False
        driver_log_storage_path = None
        driver_log_local_path = os.path.join(output_folder, "driver_logs.txt")

        for log_file in sorted(log_files):
            logging.info(f"Processing {log_file}.")

            log_local_path = self._download_log_file(cloud_storage=cloud_storage, log_file=log_file, output_folder=output_folder)
            logging.info(f"Downloaded {log_local_path}.")

            parsed_log_path = self._parse_log_file(
                parser_local_path=parser_local_path, log_file=log_local_path, output_folder=output_folder
            )
            logging.info(f"Parsed {parsed_log_path}.")

            destination_file_name = os.path.join(
                app_folder,
                self.parsed_logs_folder_name,
                f"{os.path.basename(parsed_log_path)}",
            )
            self._upload_log_file(cloud_storage=cloud_storage, destination_file_name=destination_file_name, parsed_log_path=parsed_log_path)
            if not found_driver_logs:
                found_driver_logs = self._write_driver_logs_to_file(
                    parsed_log_path=parsed_log_path, driver_log_output_path=driver_log_local_path
                )
                if found_driver_logs:
                    driver_log_storage_path = os.path.join(
                        app_folder,
                        self.parsed_logs_folder_name,
                        "driver_logs.txt",
                    )
                    self._upload_log_file(
                        cloud_storage=cloud_storage, destination_file_name=driver_log_storage_path, parsed_log_path=driver_log_local_path
                    )

            os.remove(log_local_path)
            os.remove(parsed_log_path)

        if not found_driver_logs:
            logging.info("No driver logs found.")
        elif self.print_parsed_spark_driver_logs:
            self._log_driver_logs(path=driver_log_local_path)
        else:
            logging.info(f"Parsed driver logs are available at {driver_log_storage_path}, but printing to the Airflow UI is disabled")

    def _download_log_file(self, cloud_storage: CloudStorage, log_file: str, output_folder: str) -> str:
        file_name = os.path.basename(log_file)
        local_path = os.path.join(output_folder, file_name)
        cloud_storage.download_file(
            file_path=local_path, key=log_file, bucket_name=f"{self.log_container_name}@{self.logs_storage_account_name}"
        )

        return local_path

    def _parse_log_file(self, parser_local_path: str, log_file: str, output_folder: str) -> str:
        parser_command = [
            "java",
            "-jar",
            parser_local_path,
            "-d",
            log_file,
            "-o",
            output_folder,
        ]

        try:
            subprocess.run(parser_command, check=True)
            logging.info(f"Hadoop log parser succeeded for {log_file}.")
        except subprocess.CalledProcessError as e:
            logging.warning(f"Error occurred while processing {log_file}: {e}")

        file_name = os.path.basename(log_file)
        parsed_log_path = os.path.join(output_folder, f"{file_name}.txt")
        return parsed_log_path

    def _upload_log_file(self, cloud_storage: CloudStorage, destination_file_name: str, parsed_log_path: str) -> None:
        cloud_storage.upload_file(
            key=destination_file_name,
            file_path=parsed_log_path,
            replace=True,
            bucket_name=f"{self.log_container_name}@{self.logs_storage_account_name}"
        )
        logging.info(f"Azure log upload succeeded for {parsed_log_path}, Azure Blob Storage address is {destination_file_name}.")

    def _log_driver_logs(self, path: str) -> None:
        logging.info("::group::Driver logs:")
        with open(path, "r") as file:
            for line in file:
                logging.info(line)
        logging.info("::endgroup::")

    def _write_driver_logs_to_file(self, parsed_log_path: str, driver_log_output_path: str) -> bool:
        log_pattern = re.compile(r'Container: container_\S+ on \S+')
        driver_container_id = "_000001"
        found_driver_logs = False

        with open(parsed_log_path, 'r') as parsed_log_file, open(driver_log_output_path, 'w') as driver_log_file:
            inside_driver_logs = False
            for line in parsed_log_file:
                match = log_pattern.search(line)
                if match:
                    if driver_container_id in match.group(0):
                        inside_driver_logs = True
                        found_driver_logs = True
                        driver_log_file.write(line.strip() + '\n')
                    else:
                        inside_driver_logs = False
                elif inside_driver_logs:
                    driver_log_file.write(line.strip() + '\n')
        if found_driver_logs:
            logging.info(f"Found driver logs in {parsed_log_path}")

        return found_driver_logs
