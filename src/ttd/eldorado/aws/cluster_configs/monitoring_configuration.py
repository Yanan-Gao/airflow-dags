from ttd.eldorado.aws.cluster_configs.emr_conf import EmrConf, EmrConfiguration


class CustomMonitoringConfiguration(EmrConf):

    def __init__(self, base_path: str, bootstrap_path: str):
        self.MONITORING_BASE_PATH = base_path
        self.BOOTSTRAP_BASE_PATH = bootstrap_path
        self.NODE_EXPORTER_BOOTSTRAP_PATH = (self.MONITORING_BASE_PATH + "/" + "node_exporter_bootstrap.sh")
        self.GRAPHITE_EXPORTER_BOOTSTRAP_PATH: str = (self.MONITORING_BASE_PATH + "/" + "graphite_exporter_bootstrap.sh")
        self.IMPORT_TTD_CERTS_PATH: str = (self.BOOTSTRAP_BASE_PATH + "/" + "import-ttd-certs.sh")
        self.DATABRICKS_IMPORT_TTD_CERTS_PATH: str = (self.BOOTSTRAP_BASE_PATH + "/" + "import-ttd-certs-databricks.sh")
        self.CONFIGURATION: EmrConfiguration = {
            "Classification": "spark-metrics",
            "Properties": {
                "master.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
                "*.sink.graphite.host": "localhost",
                "worker.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
                "*.sink.graphite.class": "org.apache.spark.metrics.sink.GraphiteSink",
                "*.sink.graphite.port": "9109",
                "executor.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
                "driver.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
                "*.sink.graphite.period": "5",
            },
            "Configurations": [],
        }
        self.SPARK_HISTORY_STAT_BOOTSTRAP_PATH = self.MONITORING_BASE_PATH + "/bootstrap_stat_collector.sh"
        self.SPARK_HISTORY_STAT_SHUTDOWN_PATH = self.MONITORING_BASE_PATH + "/spark_stat_collect.py"

    def to_dict(self) -> EmrConfiguration:
        return self.CONFIGURATION


class MonitoringConfigurationSpark2(CustomMonitoringConfiguration):

    def __init__(self):
        base_path: str = "s3://ttd-build-artefacts/eldorado-core/release/v0-spark-2.4.0/latest/monitoring-scripts"
        bootstrap_path: str = "s3://ttd-build-artefacts/eldorado-core/release/v0-spark-2.4.8/latest/bootstrap"
        super().__init__(base_path, bootstrap_path)


class MonitoringConfigurationSpark3(CustomMonitoringConfiguration):

    def __init__(self):
        base_path: str = "s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/monitoring-scripts"
        bootstrap_path: str = "s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/bootstrap"
        super().__init__(base_path, bootstrap_path)
