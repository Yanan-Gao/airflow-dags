import unittest.mock
from datetime import timedelta

from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ttdenv import TestEnv, ProdEnv, ProdTestEnv
from ttd.eldorado.aws.emr_cluster_specs import EmrClusterSpecs
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.docker import PySparkEmrTask


class TestEmrJobTask(unittest.TestCase):

    def test_spark_submit_args_w_ttdenv(self):
        for env in (TestEnv(), ProdEnv(), ProdTestEnv()):
            cluster_config = EmrClusterSpecs(
                cluster_name="test",
                environment=env,
                core_fleet_instance_type_configs=EmrFleetInstanceTypes(
                    instance_types=[
                        M5.m5_large().with_max_ondemand_price().with_fleet_weighted_capacity(1),
                    ],
                    on_demand_weighted_capacity=1 * 2,
                    spot_weighted_capacity=0,
                ),
            )
            spark_step = PySparkEmrTask(
                name="mlflow-pyspark-smoke-test",
                entry_point_path="/home/hadoop/app/pyspark_smoke_test.py",
                image_name="ttd-base/aifun/mlflow_pyspark_job",
                image_tag="latest",
                docker_registry="internal.docker.adsrvr.org",
                action_on_failure="CANCEL_AND_WAIT",
                additional_args_option_pairs_list=[
                    ("conf", "spark.yarn.appMasterEnv.EMR_JOB=true"),
                ],
                timeout_timedelta=timedelta(hours=1),
                cluster_specs=cluster_config,
            )

            spark_cli = spark_step._build_spark_step_args()
            idx = spark_cli.index(f"spark.yarn.appMasterEnv.ttdenv={env}")
            assert spark_cli[idx - 1] == "--conf"
