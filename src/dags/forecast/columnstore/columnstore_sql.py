from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

from dags.forecast.columnstore.enums.table_type import TableType
from dags.forecast.columnstore.enums.xd_level import XdLevel
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask

from ttd.workers.worker import Workers
from ttd.kubernetes.k8s_executor_config import K8sExecutorConfig


class ColumnStoreFunctions:

    @staticmethod
    def create_sql_sensor(task_id: str, conn_id: str, sql: list[str], poke_interval: int, timeout: int) -> SqlSensor:
        return SqlSensor(
            task_id=task_id,
            conn_id=conn_id,
            sql=sql,  # type: ignore
            poke_interval=poke_interval,
            timeout=timeout,
            queue=Workers.k8s.queue,
            pool=Workers.k8s.pool,
            executor_config=K8sExecutorConfig.watch_task()
        )

    def create_simple_load_task_group(
        self, loading_query: str, directory_root: str, direct_copy: bool, task_id_base: str, task_group_name: str, on_load_complete: OpTask
    ) -> ChainOfTasks:
        """
        :param loading_query: The query to load the data. This should not include the 'FROM' clause or any later elements
            - these are added for each task individually
        :param directory_root: The full file path, up to the individual file names
        :param direct_copy: True if no other transformations are needed, false otherwise
        :param task_id_base: A description of the job. This will be appended with '_part_<part_number>'
        :param task_group_name: The name for the task group
        :param on_load_complete: The OpTask to run once the loads have completed
        :return: A group of SqlSensor tasks for loading a dataset into Vertica by parts
        """

        tasks = []

        for part_number in range(0, 10):
            # To ensure that each task is relatively small and that we can pick up close to where we left off from when
            # DAGs fail, we split the load into 10 tasks, based on the last digit of the part number. We use this logic
            # for all our loading processes.
            queries = []

            queries.append(
                f"""{loading_query}
                FROM '{directory_root}/part-[0-9][0-9][0-9][0-9]{part_number}-*.parquet' PARQUET {"DIRECT" if direct_copy else ""};""",
            )

            tasks.append(
                OpTask(
                    op=self.create_sql_sensor(
                        task_id=f"{task_id_base}_{part_number}",
                        conn_id="haystack_loader",
                        sql=queries,
                        poke_interval=10,
                        timeout=3600,
                    )
                )
            )

        tasks.append(on_load_complete)

        return ChainOfTasks(task_id=task_group_name, tasks=tasks).as_taskgroup(task_group_name)

    def create_uuid_variable_load_task_group(
        self,
        is_uuid: bool,
        uuid_loading_query: str,
        non_uuid_loading_query: str,
        direct_copy_without_uuid: bool,
        directory_root: str,
        task_id_base: str,
        task_group_name: str,
        on_load_complete: OpTask,
    ) -> ChainOfTasks:
        """
        A function to be used to load in the avails and audience datasets. Depending on the cross-device level, these may
        contain UUIDs.
        :param is_uuid: True if the cross-device level uses UUIDs for their IDs, false otherwise.
        :param uuid_loading_query: The query to use if is_uuid == True
        :param non_uuid_loading_query: The query to use if is_uuid == False
        :param direct_copy_without_uuid: true if no other transformations are needed on the non-UUID datasets, false otherwise
        :param directory_root: The full file path, up to the individual file names.
        :param task_id_base: A description of the job. This will be appended with '_part_<part_number>'.
        :param task_group_name: The name for the task group
        :param on_load_complete: The OpTask to run once the loads have completed
        :return: A group of SqlSensor tasks for loading a dataset into Vertica by parts
        """

        tasks = []

        for part_number in range(0, 10):
            # To ensure that each task is relatively small and that we can pick up close to where we left off from when
            # DAGs fail, we split the load into 10 tasks, based on the last digit of the part number. We use this logic
            # for all our loading processes.
            queries = []

            queries.append(
                f"""{uuid_loading_query if is_uuid else non_uuid_loading_query}
                FROM '{directory_root}/part-[0-9][0-9][0-9][0-9]{part_number}-*.parquet' PARQUET {"DIRECT" if not is_uuid and direct_copy_without_uuid else ""};""",
            )

            tasks.append(
                OpTask(
                    op=self.create_sql_sensor(
                        task_id=f"{task_id_base}_part_{part_number}",
                        conn_id="haystack_loader",
                        sql=queries,
                        poke_interval=10,
                        timeout=3600,
                    )
                )
            )

        tasks.append(on_load_complete)

        return ChainOfTasks(task_id=task_group_name, tasks=tasks).as_taskgroup(task_group_name)

    def drop_partitions_query(self, tables: list[str], latest_date_to_drop: str, task_id: str, schema: str = "ttd_forecast") -> OpTask:
        """This function is used to clear out old datasets.
        :param tables: The list of tables to drop partitions for.
        :param latest_date_to_drop: The last date of data to drop partitions for. Partitions will be dropped from
            2024-01-01 to this date.
        :param task_id: A description of the task."""
        queries = []

        for table in tables:
            queries.append(f"SELECT DROP_PARTITIONS('{schema}.{table}', '2024-01-01', '{latest_date_to_drop}');")

        return OpTask(
            op=self.create_sql_sensor(
                task_id=task_id,
                conn_id="haystack_purger",
                sql=queries,
                poke_interval=5,
                timeout=600
                # If it takes more than 10 minutes to drop the partitions, something is seriously wrong here and there
                # should be some manual work here.
            )
        )

    def adjust_pinning_query(self, function: str, date: str, task_id: str) -> OpTask:
        """Here, we just want to call a function. The user we use in this connection does not have permission to pin, it just
        has permission to execute the sproc. Any changes to the tables we should be pinning should be made to the relevant
        sprocs.
        :param function: The name of the function to call, without the schema prefix. The schema is fixed as ttd_forecast.
        :param date: The date to load data for
        :param task_id: A description of the task"""
        return OpTask(
            op=self.create_sql_sensor(
                task_id=task_id,
                conn_id="haystack_pinner",
                sql=[f"CALL ttd_forecast.{function}('{date}'::DATE);", "SELECT 1;"],
                poke_interval=5,
                timeout=600,
            )
        )

    def mark_dataset_loaded(self, date: str, xd_vendor_ids: list[XdLevel], table_type: TableType, isClickHouse: bool = False) -> OpTask:
        """Used to mark that a dataset has been fully loaded, so that haystack knows it can use that dataset."""
        queries = []
        replicated_suffix = "_Replicated" if isClickHouse else ""

        for xd_vendor_id in xd_vendor_ids:
            queries.append(
                f"INSERT INTO ttd_forecast.SuccessfulLoads{replicated_suffix} (Date, XdLevel, TableType) VALUES ('{date}'::DATE, {xd_vendor_id.value}, {table_type.value});"
            )

        return OpTask(
            op=ClickHouseOperator(
                task_id=f"mark_{table_type.name}_load_complete_on_clickhouse",
                clickhouse_conn_id="altinity_clickhouse_prd-cluster_connection",
                query_id="relevance-marking-load-complete-query-from-airflow_{{ ti.dag_id }}-{{ ti.task_id }}",
                sql=queries,
            )
        ) if isClickHouse else OpTask(
            op=self.create_sql_sensor(
                task_id=f"mark_{table_type.name}_load_complete",
                conn_id="haystack_loader",
                sql=queries,
                poke_interval=5,
                timeout=600,
            )
        )
