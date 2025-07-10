from dags.pdg.data_subject_request.operators.parquet_delete_operators import shared
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.eldorado.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.tasks.base import BaseTask


class AzureParquetDeleteOperation(shared.IParquetDeleteOperation):
    JAR_PATH = "abfs://ttd-build-artefacts@ttdartefacts.dfs.core.windows.net/data-subject-request-processing/release/master/latest/data-subject-request-processing.jar"

    def _create_delete_job_task(self, dataset_name: str) -> BaseTask:
        """
        Create delete job task for a dataset. The job will be one downstream job of upstream_task.

        @param dataset_name Dataset name
        @param parent_dag_id Parent DAG id
        @param uiids UIIDs to delete from datasets
        @param upstream_task upstream task
        @return delete task
        """
        name = self.dataset_configs[dataset_name]['task_name'] or self._clean_name_for_azure(dataset_name)

        cluster_task = HDIClusterTask(
            name=self._clean_name_for_azure(f"{self.cluster_name}-{name}"),
            vm_config=HDIVMConfig(
                **{
                    'headnode_type': HDIInstanceTypes.Standard_D14_v2(),
                    'workernode_type': HDIInstanceTypes.Standard_D32A_v4(),
                    'num_workernode': 1,
                    **self.dataset_configs[dataset_name]['HDIVMConfig_override']  # Overrides
                }
            ),
            cluster_version=HDIClusterVersions.AzureHdiSpark31,
            cluster_tags={"Team": "PDG"},
            enable_openlineage=False,
        )

        job_task = HDIJobTask(
            name=name,
            class_name=self.job_class_name,
            jar_path=AzureParquetDeleteOperation.JAR_PATH,
            additional_args_option_pairs_list=[
                ("conf", "spark.sql.files.ignoreCorruptFiles=true"),
                ("conf", "spark.executor.cores=3"),
                ("conf", "spark.executor.memory=46208m"),
                ("conf", "spark.driver.maxResultSize=32g"),
                ("conf", "spark.driver.memory=105g"),
                ("conf", "spark.default.parallelism=128"),
                ("conf", "spark.driver.cores=15"),
                ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseG1GC"),
            ],
            eldorado_config_option_pairs_list=self._get_arguments_dsr_processing_job(
                dataset_name,
                log_output_path="wasbs://ttd-data-subject-requests@ttddatasubjectrequests.blob.core.windows.net/delete",
            ),
            configure_cluster_automatically=True
        )

        cluster_task.add_parallel_body_task(job_task)

        return cluster_task

    def _clean_name_for_azure(self, name: str):
        return name.replace('_', '-')
