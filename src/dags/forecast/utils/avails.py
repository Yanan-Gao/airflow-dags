from airflow import DAG

from datasources.sources.common_datasources import CommonDatasources
from ttd.tasks.op import OpTask


def get_wait_avails_operator(avail_stream: str, dag: DAG, graph_name: str = None) -> OpTask:
    if graph_name is not None:
        return OpTask(op=CommonDatasources.avails_open_graph_sampled_iav2.get_wait_for_day_complete_operator(dag))
    else:
        if avail_stream == "deviceSampled":
            return OpTask(op=CommonDatasources.avails_user_sampled.get_wait_for_day_complete_operator(dag))
        elif avail_stream == "householdSampled":
            return OpTask(op=CommonDatasources.avails_household_sampled.get_wait_for_day_complete_operator(dag))
        else:
            raise Exception(f"Unrecognized avail stream: {avail_stream}")
