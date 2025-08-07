from datetime import timedelta

from airflow.operators.python_operator import BranchPythonOperator

from dags.cmo.utils.pipeline_config import PipelineConfig
from datasources.sources.shareshift_datasources import ShareshiftDataType, ShareshiftDatasource
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask


class ShareShiftTasks:

    def __init__(
        self,
        dag: TtdDag,
        pipeline_config: PipelineConfig,
        provider: str,
        country: str,
        start_date: str,
        end_date: str,
        ds_version: int = 1
    ):
        self.dag = dag.airflow_dag
        self.pipeline_config = pipeline_config

        self.shareshift_dataset = ShareshiftDatasource(country, provider)
        self.provider = provider
        self.country = country
        self.start_date = start_date
        self.ds_version = ds_version
        self.shareshift_config = [
            ("country", country), ("provider", provider), ("startDate", start_date.replace("$format$", "%Y-%m-%d")),
            ("endDate", end_date.replace("$format$", "%Y-%m-%d")),
            ("shareshiftPath", self.shareshift_dataset.get_shareshift_dataset(ShareshiftDataType.Impression).get_root_path()),
            ("ttd.ds.DemographicDataSet.isInChain", "true"), ("ttd.ds.WeightDataSet.isInChain", "true"),
            ("ttd.ds.CorrectionDataSet.isInChain", "true"), ("ttd.ds.ImpressionDataSet.isInChain", "true"),
            ("ttd.ds.NetworkDataSet.isInChain", "true")
        ]

    def get_run_shareshift_branch_task(self, run_date_of_month: int, task_id: str):
        return OpTask(
            op=BranchPythonOperator(
                dag=self.dag,
                task_id='shareshift_branch',
                python_callable=lambda logical_date, **_: [task_id] if logical_date.day == run_date_of_month else [],
                provide_context=True
            )
        )

    def get_emr_cluster_task(self, name: str, master: EmrFleetInstanceTypes, worker: EmrFleetInstanceTypes):
        return EmrClusterTask(
            name=name + f"-{self.provider}-{self.country}",
            master_fleet_instance_type_configs=master,
            cluster_tags={"Team": CMO.team.jira_team},
            core_fleet_instance_type_configs=worker,
            emr_release_label=self.pipeline_config.emr_release_label,
            additional_application_configurations=self.pipeline_config.get_cluster_additional_configurations(),
            enable_prometheus_monitoring=True,
            environment=self.pipeline_config.env
        )

    def get_impressions_task(
        self, imps_master_cluster, imps_worker_cluster, additional_eldorado_config=[], additional_step_config=[], timeout_in_min=60
    ):
        imps_cluster_task = self.get_emr_cluster_task(
            f"shareshift-imps-demo-aggregation-cluster-{self.ds_version}", imps_master_cluster, imps_worker_cluster
        )

        imps_job_task = EmrJobTask(
            name=f'ShareShift-{self.provider}-imps-demos-aggregation-{self.ds_version}',
            class_name="jobs.ctv.shareshift.LinearImpDemoAggregation",
            eldorado_config_option_pairs_list=self.shareshift_config + additional_eldorado_config,
            additional_args_option_pairs_list=self.pipeline_config.get_step_additional_configurations() + additional_step_config,
            timeout_timedelta=timedelta(minutes=timeout_in_min),
            executable_path=self.pipeline_config.jar,
            cluster_specs=imps_cluster_task.cluster_specs
        )
        imps_cluster_task.add_parallel_body_task(imps_job_task)

        return imps_cluster_task

    def get_weights_job_task(self, limit_ephemeral_storage='500M', request_memory='2G', limit_memory='4G'):
        demo_path = self.shareshift_dataset.get_full_s3_path(
            ShareshiftDataType.Demographic, self.pipeline_config.env.dataset_write_env, version=self.ds_version
        ) + f'/monthdate={self.start_date.replace("$format$", "%Y%m")}'
        output_path = self.shareshift_dataset.get_full_s3_path(
            ShareshiftDataType.Weight, self.pipeline_config.env.dataset_write_env, version=self.ds_version
        ) + f'/monthdate={self.start_date.replace("$format$", "%Y%m")}/part-weights.parquet'
        weights_job = TtdKubernetesPodOperator(
            namespace='linear-tv-data',
            image='production.docker.adsrvr.org/ttd/ctv/python-script-runner:2619602',
            name=f"ShareShift-{self.provider}-weight-{self.ds_version}",
            task_id=f"shareshift_weight-{self.ds_version}",
            dnspolicy='ClusterFirst',
            get_logs=True,
            is_delete_operator_pod=True,
            dag=self.dag,
            service_account_name='linear-tv-data',
            startup_timeout_seconds=800,
            log_events_on_failure=True,
            cmds=["python"],
            arguments=[
                "./executor_shareshift.py", "--program=weight", f"--country={self.country}", f"--observed_demo_path={demo_path}",
                f"--output_path={output_path}"
            ],
            env_vars=dict(PROMETHEUS_ENV=self.pipeline_config.env.execution_env),
            resources=PodResources(
                limit_ephemeral_storage=limit_ephemeral_storage, request_memory=request_memory, limit_memory=limit_memory, request_cpu='2'
            )
        )
        return OpTask(op=weights_job)

    def get_sampling_task(
        self,
        sampling_master_cluster,
        sampling_worker_cluster,
        additional_eldorado_config=[],
        additional_step_config=[],
        timeout_in_min=60
    ):
        sampling_cluster_task = self.get_emr_cluster_task(
            f"shareshift-hh-sampling-{self.ds_version}", sampling_master_cluster, sampling_worker_cluster
        )

        sampling_job_task = EmrJobTask(
            name=f'ShareShift-{self.provider}-hh-sampling-{self.ds_version}',
            class_name="jobs.ctv.shareshift.LinearDatasetsSamplingJob",
            eldorado_config_option_pairs_list=self.shareshift_config + additional_eldorado_config,
            additional_args_option_pairs_list=self.pipeline_config.get_step_additional_configurations() + additional_step_config,
            timeout_timedelta=timedelta(minutes=timeout_in_min),
            executable_path=self.pipeline_config.jar,
            cluster_specs=sampling_cluster_task.cluster_specs
        )
        sampling_cluster_task.add_parallel_body_task(sampling_job_task)

        return sampling_cluster_task

    def get_corrections_task(
        self,
        corrections_master_cluster,
        corrections_worker_cluster,
        additional_eldorado_config=[],
        additional_step_config=[],
        timeout_in_min=200
    ):
        corrections_cluster_task = self.get_emr_cluster_task(
            f"shareshift-corrections-clusters-{self.ds_version}", corrections_master_cluster, corrections_worker_cluster
        )

        corrections_job_task = EmrJobTask(
            name=f'ShareShift-{self.provider}-corrections-{self.ds_version}',
            class_name="jobs.ctv.shareshift.Corrections",
            eldorado_config_option_pairs_list=self.shareshift_config + additional_eldorado_config,
            additional_args_option_pairs_list=self.pipeline_config.get_step_additional_configurations() + additional_step_config,
            timeout_timedelta=timedelta(minutes=timeout_in_min),
            executable_path=self.pipeline_config.jar,
            cluster_specs=corrections_cluster_task.cluster_specs
        )
        corrections_cluster_task.add_parallel_body_task(corrections_job_task)

        return corrections_cluster_task

    def get_optout_task(
        self, optout_master_cluster, optout_worker_cluster, additional_eldorado_config=[], additional_step_config=[], timeout_in_min=30
    ):

        optout_cluster_task = self.get_emr_cluster_task(
            f"shareshift-optout-delete-old-partitions-{self.provider}-{self.country}-cluster-{self.ds_version}", optout_master_cluster,
            optout_worker_cluster
        )

        optout_job_task = EmrJobTask(
            name=f'ShareShift-{self.provider}-{self.country}-optout-delete-old-partitions-{self.ds_version}',
            class_name="jobs.ctv.shareshift.OptOutHandler",
            eldorado_config_option_pairs_list=self.shareshift_config + additional_eldorado_config,
            additional_args_option_pairs_list=self.pipeline_config.get_step_additional_configurations() + additional_step_config,
            timeout_timedelta=timedelta(minutes=timeout_in_min),
            executable_path=self.pipeline_config.jar,
            cluster_specs=optout_cluster_task.cluster_specs
        )

        optout_cluster_task.add_parallel_body_task(optout_job_task)
        return optout_cluster_task
