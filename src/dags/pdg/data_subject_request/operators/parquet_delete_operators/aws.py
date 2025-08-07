from datetime import timedelta

from dags.pdg.data_subject_request.operators.parquet_delete_operators import shared, DSR_DELETE_DATASET_CONFIG, PARTNER_DSR_DATASET_CONFIG
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.openlineage import IgnoreOwnershipType, OpenlineageConfig
from ttd.tasks.base import BaseTask
from ttd.ttdenv import TtdEnvFactory


class AWSParquetDeleteOperation(shared.IParquetDeleteOperation):
    EMR_RELEASE_LABEL = AwsEmrVersions.AWS_EMR_SPARK_3_5_2
    job_type = "UserDsr"
    env_prefix = "env=prod" if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else "env=test"

    # Sentinel object to distinguish between None and not provided
    _UNSET = object()

    def __init__(
        self,
        dataset_configs,
        job_class_name,
        dsr_request_id=None,
        mode=None,
        cluster_name="datagov-dsr-delete",
        additional_args=None,
        jar_path=shared.DEFAULT_JAR_PATH,
        log_uri=_UNSET
    ):
        # If log_uri was not provided (default), use the default S3 path
        # If log_uri was explicitly set to None, keep it as None
        if log_uri is self._UNSET:
            log_uri = f"s3://ttd-data-subject-requests/{self.env_prefix}/emr"

        super().__init__(
            dataset_configs=dataset_configs,
            job_class_name=job_class_name,
            dsr_request_id=dsr_request_id,
            mode=mode,
            cluster_name=cluster_name,
            additional_args=additional_args,
            jar_path=jar_path,
            log_uri=log_uri
        )

    # Simplified log4j2 configuration for EMR 7.x - avoiding complex filters
    emr_configuration = [
        {
            "Classification": "yarn-site",
            "Properties": {
                # Include custom log files in YARN log aggregation to S3
                "yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds": "3600",
                "yarn.nodemanager.log-aggregation.num-log-files-per-app": "30",
                "yarn.nodemanager.log-aggregation.retain-seconds": "604800",  # 7 days
                "yarn.log-aggregation.include-pattern": "**/*.log",
            }
        },
        {
            "Classification": "spark-log4j2",
            "Properties": {
                # Root logger configuration
                "rootLogger.level": "INFO",
                "rootLogger.appenderRef.stdout.ref": "Console",

                # Console appender - simplified without filter
                "appender.console.type": "Console",
                "appender.console.name": "Console",
                "appender.console.target": "SYSTEM_OUT",
                "appender.console.layout.type": "PatternLayout",
                "appender.console.layout.pattern": "%d{HH:mm:ss} %p %c{1}: %m%n",

                # Application log file
                "appender.app.type": "File",
                "appender.app.name": "ApplicationFile",
                "appender.app.fileName": "${sys:spark.yarn.app.container.log.dir}/application.log",
                "appender.app.layout.type": "PatternLayout",
                "appender.app.layout.pattern": "%d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1} - %m%n",

                # Spark log file
                "appender.spark.type": "File",
                "appender.spark.name": "SparkFile",
                "appender.spark.fileName": "${sys:spark.yarn.app.container.log.dir}/spark.log",
                "appender.spark.layout.type": "PatternLayout",
                "appender.spark.layout.pattern": "%d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c - %m%n",

                # OpenLineage log file
                "appender.ol.type": "File",
                "appender.ol.name": "OpenLineageFile",
                "appender.ol.fileName": "${sys:spark.yarn.app.container.log.dir}/openlineage.log",
                "appender.ol.layout.type": "PatternLayout",
                "appender.ol.layout.pattern": "%d{yyyy-MM-dd HH:mm:ss} %-5p %c - %m%n",

                # Application loggers
                "logger.ttd.name": "com.thetradedesk",
                "logger.ttd.level": "DEBUG",
                "logger.ttd.additivity": "false",
                "logger.ttd.appenderRef.app.ref": "ApplicationFile",

                # Spark loggers
                "logger.spark.name": "org.apache.spark",
                "logger.spark.level": "INFO",
                "logger.spark.additivity": "false",
                "logger.spark.appenderRef.spark.ref": "SparkFile",

                # OpenLineage loggers
                "logger.ol1.name": "io.openlineage",
                "logger.ol1.level": "INFO",
                "logger.ol1.additivity": "false",
                "logger.ol1.appenderRef.ol.ref": "OpenLineageFile",
                "logger.ol2.name": "RddExecutionContext",
                "logger.ol2.level": "INFO",
                "logger.ol2.additivity": "false",
                "logger.ol2.appenderRef.ol.ref": "OpenLineageFile",

                # Reduce Hadoop noise
                "logger.hadoop.name": "org.apache.hadoop",
                "logger.hadoop.level": "WARN",

                # Reduce other noise
                "logger.aws.name": "com.amazonaws",
                "logger.aws.level": "WARN",
            }
        }
    ]

    spark_options = [
        # Memory and driver settings - optimized for r6gd.8xlarge master
        ("conf", "spark.driver.maxResultSize=40g"),
        ("conf", "spark.driver.memory=64g"),
        ("conf", "spark.driver.memoryOverhead=8g"),  # Increased
        ("conf", "spark.driver.cores=20"),  # r6gd.8xlarge has 32 vCPUs, leave some for system

        # Executor settings - MORE executors for I/O bound workload on r6gd.12xlarge
        # r6gd.12xlarge: 384 GB RAM, 48 vCPUs
        # Strategy: More, smaller executors for better I/O concurrency
        # With 3 cores per executor: 11 executors per node
        # Each executor: 30GB + 4GB overhead = 34GB total (11 × 34GB = 374GB, leaving 10GB for system)
        ("conf", "spark.executor.instances=22"),  # Initial executors for 2 nodes (11 per node)
        ("conf", "spark.executor.memory=30g"),  # Smaller memory per executor
        ("conf", "spark.executor.memoryOverhead=4g"),  # Smaller overhead

        # Dynamic allocation - adjusted for 3-core executors
        ("conf", "spark.dynamicAllocation.enabled=true"),
        ("conf", "spark.dynamicAllocation.minExecutors=22"),  # 2 nodes × 11 executors
        ("conf", "spark.dynamicAllocation.maxExecutors=220"),  # 20 nodes × 11 executors
        ("conf", "spark.dynamicAllocation.executorIdleTimeout=300s"),
        ("conf", "spark.dynamicAllocation.schedulerBacklogTimeout=1s"),  # More aggressive scaling
        ("conf", "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout=1s"),
        ("conf", "spark.dynamicAllocation.executorAllocationRatio=1.0"),  # More aggressive

        # Ensure dynamic allocation can scale up quickly when needed
        ("conf", "spark.dynamicAllocation.initialExecutors=22"),  # Start with minimum

        # YARN configuration
        ("conf", "spark.yarn.am.memory=4g"),
        ("conf", "spark.yarn.am.cores=1"),
        ("conf", "spark.yarn.executor.launch.container.launch.context.maximum.wait.ms=300000"),  # 5 min wait
        ("conf", "spark.yarn.priority=1000"),  # Higher priority for container allocation

        # Parallelism settings - INCREASE for better CPU utilization
        ("conf", "spark.default.parallelism=2640"),  # 220 executors × 3 cores × 4
        ("conf", "spark.sql.shuffle.partitions=10000"),  # More partitions = more concurrent tasks

        # CRITICAL: Smaller executors, more concurrency for I/O bound workloads
        ("conf", "spark.executor.cores=3"),  # Fewer cores per executor = more executors
        ("conf", "spark.task.cpus=1"),  # Each task uses 1 CPU
        ("conf", "spark.executor.heartbeatInterval=20s"),  # Faster heartbeat for more tasks

        # Adaptive query execution - enhanced in Spark 3.5
        ("conf", "spark.sql.adaptive.enabled=true"),
        ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"),
        ("conf", "spark.sql.adaptive.advisoryPartitionSizeInBytes=128m"),  # Smaller partitions
        ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
        ("conf", "spark.sql.adaptive.coalescePartitions.minPartitionSize=1m"),  # Allow smaller partitions
        ("conf", "spark.sql.adaptive.autoBroadcastJoinThreshold=100m"),  # Auto-broadcast smaller tables
        ("conf", "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled=true"),  # New in 3.2+

        # File handling optimizations - INCREASED for better CPU utilization
        ("conf", "spark.sql.files.maxPartitionBytes=4294967296"),  # 4GB - larger partitions = fewer tasks
        ("conf", "spark.sql.files.openCostInBytes=8388608"),  # 8MB - higher cost for small files
        ("conf", "spark.sql.files.ignoreCorruptFiles=true"),
        ("conf", "spark.sql.files.minPartitionNum=1000"),  # Minimum partitions to create

        # CRITICAL: Missing configurations for handling millions of files
        ("conf", "spark.sql.files.ignoreMissingFiles=true"),
        ("conf", "spark.sql.hive.filesourcePartitionFileCacheSize=8589934592"),  # 8GB cache for 13M files
        ("conf", "spark.sql.sources.parallelPartitionDiscovery.parallelism=5000"),  # MAX parallel discovery
        ("conf", "spark.sql.sources.parallelPartitionDiscovery.threshold=1"),  # Always use parallel discovery

        # Additional file handling for massive scale
        ("conf", "spark.sql.leafNodeDefaultParallelism=10000"),  # Force high parallelism at leaf level
        ("conf", "spark.hadoop.fs.s3a.experimental.fadvise=random"),  # Random access pattern for many files
        ("conf", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2"),  # Faster commit

        # Additional file listing optimizations for S3
        ("conf", "spark.hadoop.mapreduce.input.fileinputformat.list-status.num-threads=1000"),
        # More threads with more executors
        ("conf", "spark.hadoop.fs.s3a.directory.marker.retention=keep"),  # Keep directory markers for faster listing
        ("conf", "spark.sql.hive.metastorePartitionPruning=true"),  # Enable partition pruning
        ("conf", "spark.sql.hive.caseSensitiveInferenceMode=NEVER_INFER"),  # Skip schema inference
        ("conf", "spark.sql.statistics.size.autoUpdate.enabled=false"),  # Don't update statistics

        # CRITICAL for I/O bound workloads
        ("conf", "spark.sql.adaptive.localShuffleReader.enabled=true"),  # Reduce shuffle overhead
        ("conf", "spark.sql.adaptive.skewJoin.skewedPartitionFactor=5"),
        ("conf", "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256m"),
        ("conf", "spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin=0.2"),

        # Increase parallelism for file operations - MAXIMIZE with more executors
        ("conf", "spark.sql.files.maxConcurrentReads=2000"),  # Double concurrent reads
        ("conf", "spark.sql.files.prefetch.enabled=true"),  # Enable prefetching
        ("conf", "spark.hadoop.fs.s3a.prefetch.enabled=true"),
        ("conf", "spark.hadoop.fs.s3a.prefetch.block.size=4m"),  # Smaller blocks for more concurrency
        ("conf", "spark.hadoop.fs.s3a.prefetch.block.count=16"),  # More blocks prefetched

        # Additional optimizations for many small executors
        ("conf", "spark.task.maxFailures=10"),  # More retries for flaky S3 operations
        ("conf", "spark.stage.maxConsecutiveAttempts=10"),  # More stage retries
        ("conf", "spark.excludeOnFailure.enabled=false"),  # Don't exclude executors (updated from blacklist)

        # Optimize partition reading
        ("conf", "spark.sql.files.maxRecordsPerFile=0"),  # No limit on records per file
        ("conf", "spark.sql.execution.arrow.maxRecordsPerBatch=10000"),  # Larger batches

        # Serialization settings
        ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
        ("conf", "spark.kryoserializer.buffer.max=512m"),

        # GC and JVM settings - Optimized for high throughput + log4j2 config
        (
            "conf",
            "spark.executor.extraJavaOptions=-Dlog4j2.configurationFile=file:///etc/spark/conf/log4j2.properties -XX:+UseG1GC -XX:+UseCompressedOops -XX:+UseCompressedClassPointers -XX:InitiatingHeapOccupancyPercent=35 -XX:G1ReservePercent=20 -XX:MaxGCPauseMillis=200 -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:ParallelGCThreads=8 -XX:ConcGCThreads=2 -XX:+AlwaysPreTouch -XX:+UseNUMA"
        ),

        # Driver JVM options - include log4j2 config
        ("conf", "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=file:///etc/spark/conf/log4j2.properties"),

        # Network and timeout settings
        ("conf", "spark.network.timeout=800s"),
        ("conf", "spark.executor.heartbeatInterval=60s"),
        ("conf", "spark.rpc.askTimeout=600s"),
        ("conf", "spark.sql.broadcastTimeout=1200"),
        ("conf", "spark.rpc.message.maxSize=512"),

        # Memory fractions - optimized for 30GB executors
        ("conf", "spark.memory.fraction=0.8"),
        ("conf", "spark.memory.storageFraction=0.3"),  # Less storage, more execution memory
        ("conf", "spark.memory.offHeap.enabled=true"),
        ("conf", "spark.memory.offHeap.size=24g"),  # 80% of 30GB for off-heap

        # Storage settings
        ("conf", "spark.storage.memoryFraction=0.5"),

        # Parquet settings
        ("conf", "spark.sql.parquet.mergeSchema=false"),
        ("conf", "spark.sql.parquet.filterPushdown=true"),
        ("conf", "spark.sql.parquet.compression.codec=snappy"),
        ("conf", "spark.sql.parquet.cacheMetadata=true"),
        ("conf", "spark.sql.parquet.int96RebaseModeInWrite=LEGACY"),  # Keep only the non-deprecated version

        # S3 optimizations - MAXIMUM for 13M files
        ("conf", "spark.hadoop.fs.s3a.connection.maximum=5000"),  # 5x more connections
        ("conf", "spark.hadoop.fs.s3a.threads.max=2000"),  # 2x more threads
        ("conf", "spark.hadoop.fs.s3a.threads.core=256"),  # 4x more core threads
        ("conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false"),  # Disable SSL for speed (if security allows)
        ("conf", "spark.hadoop.fs.s3a.connection.timeout=50000"),
        ("conf", "spark.hadoop.fs.s3a.connection.establish.timeout=30000"),
        ("conf", "spark.hadoop.fs.s3a.attempts.maximum=10"),
        ("conf", "spark.hadoop.fs.s3a.retry.limit=6"),
        ("conf", "spark.hadoop.fs.s3a.retry.interval=250ms"),
        ("conf", "spark.hadoop.fs.s3a.retry.throttle.interval=500ms"),
        ("conf", "spark.hadoop.fs.s3a.fast.upload=true"),
        ("conf", "spark.hadoop.fs.s3a.fast.upload.buffer=disk"),
        ("conf", "spark.hadoop.fs.s3a.fast.upload.active.blocks=32"),
        ("conf", "spark.hadoop.fs.s3a.multipart.size=10485760"),  # 10MB - Match Databricks
        ("conf", "spark.hadoop.fs.s3a.multipart.threshold=104857600"),  # 100MB - Match Databricks
        ("conf", "spark.hadoop.fs.s3a.readahead.range=2m"),
        ("conf", "spark.hadoop.fs.s3a.block.size=67108864"),  # 64MB - Match Databricks
        ("conf", "spark.hadoop.fs.s3a.max.total.tasks=1000"),

        # Additional S3 optimizations for massive file counts
        ("conf", "spark.hadoop.fs.s3a.list.version=2"),  # Use S3 ListObjectsV2
        ("conf", "spark.hadoop.fs.s3a.paging.maximum=5000"),  # Max page size for listing

        # Disable S3 bucket verification
        ("conf", "spark.hadoop.fs.s3.verifyBucketExists=false"),
        ("conf", "spark.hadoop.fs.s3a.bucket.probe=0"),

        # Use magic committer for better S3 performance
        ("conf", "spark.hadoop.fs.s3a.committer.magic.enabled=true"),
        ("conf", "spark.hadoop.fs.s3a.committer.name=magic"),

        # Speculation - MORE AGGRESSIVE for I/O bound workloads
        ("conf", "spark.speculation=true"),
        ("conf", "spark.speculation.multiplier=2"),  # More aggressive - launch speculative tasks sooner
        ("conf", "spark.speculation.quantile=0.8"),  # Speculate on slower 20% of tasks
        ("conf", "spark.speculation.interval=1s"),  # Check more frequently

        # Additional optimizations from Databricks
        ("conf", "spark.sql.sources.default=parquet"),
        ("conf", "spark.shuffle.service.enabled=true"),
        ("conf", "spark.shuffle.manager=SORT"),
        ("conf", "spark.shuffle.memoryFraction=0.2"),
        ("conf", "spark.shuffle.reduceLocality.enabled=false"),

        # Task scheduling - optimized for many executors
        ("conf", "spark.scheduler.mode=FAIR"),
        ("conf", "spark.task.reaper.enabled=true"),
        ("conf", "spark.task.reaper.killTimeout=60s"),
        ("conf", "spark.scheduler.listenerbus.eventqueue.capacity=100000"),
        ("conf", "spark.scheduler.maxRegisteredResourcesWaitingTime=60s"),

        # Increase reducer parallelism for better CPU utilization
        ("conf", "spark.reducer.maxReqsInFlight=20"),
        ("conf", "spark.reducer.maxBlocksInFlightPerAddress=20"),
        ("conf", "spark.network.maxRemoteBlockSizeFetchToMem=2047m"),

        # Compression
        ("conf", "spark.rdd.compress=true"),
    ]

    def _create_delete_job_task(self, dataset_name: str) -> BaseTask:
        """
        Create delete job task for a dataset. The job will be one downstream job of upstream_task.

        @param dataset_name Dataset name
        @return delete task
        """

        # Check if this dataset is in the predefined DSR configurations
        is_predefined_dataset = (
            dataset_name in DSR_DELETE_DATASET_CONFIG.get('AWS', {}) or dataset_name in PARTNER_DSR_DATASET_CONFIG.get('AWS', {})
        )

        # Set conditional configurations based on dataset type
        additional_application_configurations = AWSParquetDeleteOperation.emr_configuration if is_predefined_dataset else None
        additional_args_option_pairs_list = AWSParquetDeleteOperation.spark_options if is_predefined_dataset else None
        maximize_resource_allocation = not is_predefined_dataset

        # Create cluster task
        cluster_task = EmrClusterTask(
            name=f"{self.cluster_name}-{dataset_name}",
            emr_release_label=AWSParquetDeleteOperation.EMR_RELEASE_LABEL,
            master_fleet_instance_type_configs=self.dataset_configs[dataset_name]["master_fleet_instance_type_configs"],
            cluster_tags={
                "Team": "PDG",
                "Job": AWSParquetDeleteOperation.job_type,
                "very_long_running": "na"
            },
            core_fleet_instance_type_configs=self.dataset_configs[dataset_name]["core_fleet_instance_type_configs"],
            managed_cluster_scaling_config=self.dataset_configs.get(dataset_name, {}).get("managed_cluster_scaling_config", None),
            log_uri=self.log_uri,
            additional_application_configurations=additional_application_configurations,
            enable_prometheus_monitoring=True,
            retries=1,
            maximize_resource_allocation=maximize_resource_allocation,
        )

        timeout = timedelta(hours=36)

        # Create job task
        job_task = EmrJobTask(
            name=dataset_name,
            class_name=self.job_class_name,
            executable_path=self.jar_path,
            eldorado_config_option_pairs_list=self._get_arguments_dsr_processing_job(dataset_name),
            additional_args_option_pairs_list=additional_args_option_pairs_list,
            timeout_timedelta=timeout,
            retries=0,
            openlineage_config=OpenlineageConfig(ignore_ownership=IgnoreOwnershipType.PDG_SCRUBBING),
            maximize_resource_allocation=maximize_resource_allocation,
        )

        cluster_task.add_parallel_body_task(job_task)

        return cluster_task
