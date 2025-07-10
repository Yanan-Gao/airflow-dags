from typing import Dict


class Log4j:
    """Default Log4j configuration for EmrClusterTask.instance_configuration_spark_log4j."""

    @staticmethod
    def get_default_configuration() -> Dict[str, str]:
        # Note that this depends on Spark/EMR version: Amazon EMR releases 6.8.0 requires Log4j 2.
        # See https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html
        return {
            # The set of defaults from https://github.com/apache/spark/blob/branch-3.2/conf/log4j.properties.template
            "log4j.rootCategory": "INFO, console",
            "log4j.appender.console": "org.apache.log4j.ConsoleAppender",
            "log4j.appender.console.target": "System.err",
            "log4j.appender.console.layout": "org.apache.log4j.PatternLayout",
            "log4j.appender.console.layout.ConversionPattern": "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n",
            "log4j.logger.org.apache.spark.repl.Main": "WARN",
            "log4j.logger.org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver": "WARN",
            "log4j.logger.org.sparkproject.jetty": "WARN",
            "log4j.logger.org.sparkproject.jetty.util.component.AbstractLifeCycle": "ERROR",
            "log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper": "INFO",
            "log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter": "INFO",
            "log4j.logger.org.apache.parquet": "ERROR",
            "log4j.logger.parquet": "ERROR",
            "log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler": "FATAL",
            "log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry": "ERROR",
            "log4j.appender.console.filter.1": "org.apache.log4j.varia.StringMatchFilter",
            "log4j.appender.console.filter.1.StringToMatch": "Thrift error occurred during processing of message",
            "log4j.appender.console.filter.1.AcceptOnMatch": "false",
            # Disable INFO logging for task set updates. This is not ideal because we use this info for data
            # skew detection some times; however, the logs are so huge that they are not getting archived to S3.
            "log4j.logger.org.apache.spark.scheduler.TaskSetManager": "WARN",
            # Reduce the log size further. This is one of the heaviest log lines' producers.
            "log4j.logger.org.apache.spark.storage.BlockManagerInfo": "WARN",
        }
