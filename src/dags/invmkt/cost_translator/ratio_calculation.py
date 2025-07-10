from datetime import timedelta, datetime

from ttd.tasks.op import OpTask
from ttd.eldorado.base import TtdDag

from dags.invmkt.cost_translator.create_cost_translator_dag import propagation_task, create_emr_tasks, create_python_capture_task

DAG_NAME = "cost-translator-ratio-calc"

dag: TtdDag = TtdDag(
    dag_id=DAG_NAME,
    start_date=datetime(2023, 7, 10, 0, 0),
    schedule_interval='30 9 * * 1',
    depends_on_past=False,
    run_only_latest=True,
    max_active_runs=1,
    slack_channel="#scrum-invmkt-alarms",
    retries=4,
    retry_delay=timedelta(minutes=30),
)

(cluster, [cstr, cstrsmape]) = create_emr_tasks(DAG_NAME, {"CSTR": "calc.CostRatioCalcJob", "CSTRSMAPE": "test.SmapeCheckJob"})

propagate_cost_ratios_to_sql_server_partner_to_media_task = propagation_task(
    'CostRatioPartnerToMedia', ['PartnerId', 'Features', 'PartnerToMediaRatio', 'UpdatedAt']
)
propagate_cost_ratios_to_sql_server_advertiser_to_partner_task = propagation_task(
    'CostRatioAdvertiserToPartner', ['PartnerId', 'AdvertiserId', 'AdvertiserToPartnerRatio', 'UpdatedAt']
)
propagate_cost_ratios_to_sql_server_fallbacks_task = propagation_task(
    'CostRatioFallback', ['PartnerId', 'AdvertiserToPartnerRatio', 'PartnerToMediaRatio', 'UpdatedAt']
)

cstr >> cstrsmape
cstr >> propagate_cost_ratios_to_sql_server_partner_to_media_task
cstr >> propagate_cost_ratios_to_sql_server_advertiser_to_partner_task
cstr >> propagate_cost_ratios_to_sql_server_fallbacks_task

dag >> OpTask(op=create_python_capture_task()) >> cluster
adag = dag.airflow_dag
