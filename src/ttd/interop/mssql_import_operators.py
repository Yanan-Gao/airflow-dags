from airflow.operators.python import PythonOperator, ShortCircuitOperator
from ttd.interop.logworkflow_callables import open_external_gate_for_log_type, check_task_enabled
from ttd.ttdenv import TtdEnv, TtdEnvFactory
from ttd.tasks.op import OpTask
from ttd.tasks.chain import ChainOfTasks


class MsSqlImportFromCloud(ChainOfTasks):
    """
    MsSqlImportFromCloud operators to import hourly or daily data, presets with log_type_frequency.
    :param name:                     Identifies the operator
    :param gating_type_id:           Get real value from LogWorkflowDB: dbo.GatingType
    :param log_type_id:              LogTypeId in LWDB
    :param log_start_time:           LogStartTime in LWDB
    :param mssql_import_enabled:     Flag to disable VerticaLoad when testing rest of dag separately
    :param job_environment:          prod, prodTest, test
    :param logworkflow_sandbox_connection:
    :param logworkflow_connection:
    """

    def __init__(
        self,
        name: str,
        gating_type_id: int,
        log_type_id: int,
        log_start_time: str,
        mssql_import_enabled: bool = True,
        job_environment: TtdEnv = TtdEnvFactory.get_from_system(),
        logworkflow_sandbox_connection: str = 'sandbox-lwdb',
        logworkflow_connection: str = 'lwdb',
    ):
        self.check_mssql_import_enabled_task = OpTask(
            op=ShortCircuitOperator(
                task_id=f"is_mssql_import_enabled_{name}",
                python_callable=check_task_enabled,
                op_kwargs={"task_enabled": mssql_import_enabled},
                trigger_rule="none_failed"
            )
        )
        self.open_external_gate_task = OpTask(
            op=PythonOperator(
                task_id=f"mssql_import_{name}_ready_logworkflow_entry",
                python_callable=open_external_gate_for_log_type,
                op_kwargs={
                    'mssql_conn_id': logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
                    'sproc_arguments': {
                        'gatingTypeId': gating_type_id,
                        'logTypeId': log_type_id,
                        'logStartTimeToReady': log_start_time,
                    }
                }
            )
        )

        super().__init__(task_id=name, tasks=[self.check_mssql_import_enabled_task, self.open_external_gate_task])
