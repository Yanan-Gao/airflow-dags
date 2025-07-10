from datetime import datetime, timezone

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.hooks.mssql_hook import MsSqlHook


class MaterializedGatingSensor(BaseSensorOperator):
    """
    This object will keep track of the set of materialized gates and simple dependencies that we want to check for together

    Since the sensor controls the period for which the dependencies are checked, it's up to the coder to ensure the period is set to
    run in the same interval as the grains in the dependencies.

    And since all the dependencies listed will be scheduled to be checked together, it should be ensured that all the grains being queried for are the same
    """
    lwdb_conn_id = 'shared-lwdb-datasrvc'

    class Dependency:
        """
        Keeps track of the materialized simple dependencies and gates
        You can view what's materialized by checking workflowengine.vw_MaterializedGatingConfig
        """

        def __init__(self, GatingTypeId="null", TaskId="null", LogTypeId="null", GrainId="null", TaskVariantId="null"):
            self.GatingTypeId = GatingTypeId
            self.TaskId = TaskId
            self.LogTypeId = LogTypeId
            self.GrainId = GrainId  # optional for Gates
            self.TaskVariantId = TaskVariantId  # null will be defaulted to TaskVariant_Default

    def __init__(self, *args, **kwargs):
        self._dependencies = set()
        super(MaterializedGatingSensor, self).__init__(*args, **kwargs)

    # can add gating dependencies (materialized through prc_MaterializeGate)
    # or simple dependencies (materialized through prc_MaterializeNonVirtualTaskCompletion)
    def add_dependency(self, GatingTypeId="null", TaskId="null", LogTypeId="null", GrainId="null", TaskVariantId="null"):
        # check if this represents a valid dependency
        if GatingTypeId == "null" and (TaskId == "null" or LogTypeId == "null" or GrainId == "null"):
            raise ValueError("Simple dependency is not valid. Must have at least GatingTypeId or TaskId/LogTypeId/Grain filled out")

        if GatingTypeId != "null":
            self._dependencies.add(
                MaterializedGatingSensor.Dependency(GatingTypeId=GatingTypeId, GrainId=GrainId, TaskVariantId=TaskVariantId)
            )
        else:
            self._dependencies.add(
                MaterializedGatingSensor.Dependency(TaskId=TaskId, LogTypeId=LogTypeId, GrainId=GrainId, TaskVariantId=TaskVariantId)
            )

    def get_dependencies_as_rows(self):
        dependencyRows = [f"({d.GatingTypeId}, {d.TaskId}, {d.LogTypeId}, {d.GrainId}, {d.TaskVariantId})" for d in self._dependencies]
        return ",\n".join(dependencyRows)

    # the frequency of the task is configured in the dagpipeline's job_schedule_interval.
    # ensure the grain
    def poke(self, context):
        if len(self._dependencies) == 0:
            return True

        interval_start = context['data_interval_start'].strftime("%Y-%m-%d %H:%M")

        sql = f"""
            -- temp table to be used in prc_AreGatesOpen
            drop table if exists #DependenciesToCheck
            create table #DependenciesToCheck
            (
                GatingTypeId bigint,
                TaskId bigint,
                LogTypeId bigint,
                GrainId bigint,
                TaskVariantId bigint
            )

            insert into #DependenciesToCheck (GatingTypeId, TaskId, LogTypeId, GrainId, TaskVariantId)
            values {self.get_dependencies_as_rows()}

            exec WorkflowEngine.prc_AreGatesOpen @GrainStartTime='{interval_start}', @Testing=1
            -- returned table columns:
            -- table(GatingType bigint, TaskId bigint, LogTypeId bigint, GrainId bigint, TaskVariantId bigint, IsGateOpen bit)
        """

        print("Poke the query:", sql)

        sql_hook = MsSqlHook(mssql_conn_id=self.lwdb_conn_id)
        conn = sql_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)

        output = cursor.fetchall()

        # print the gate statuses
        unopened_gates_count = sum(1 for x in output if not x[5])

        print("Dependency Statuses:")
        for row in output:
            print(
                f"GatingType: {row[0]}, TaskId: {row[1]}, LogTypeId: {row[2]}, GrainId {row[3]}, TaskVariantId {row[4]} -- IsOpen: {row[5]}"
            )

        if unopened_gates_count == 0:
            print(f"Poke result: all gates for hour {interval_start} open at {datetime.now(timezone.utc)}")
            return True
        else:
            print(
                f"Poke result: data for hour {interval_start} not yet ready at {datetime.now(timezone.utc)}, waiting on {unopened_gates_count} gates"
            )
            return False
