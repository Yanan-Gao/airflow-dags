import re
import requests
import urllib
import vertica_python

from collections import Counter
from datetime import datetime, timedelta, timezone, time
from requests.auth import HTTPBasicAuth
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from ttd.el_dorado.v2.base import TtdDag
from ttd.tasks.op import OpTask

conn_params = {
    'vertica_ops': {
        # autogenerated session label by default,
        'session_label': 'dpsr:capacity:model',
        # default throw error on invalid UTF-8 results
        'unicode_error': 'strict',
        # SSL is disabled by default
        'ssl': False,
        # autocommit is off by default
        'autocommit': True,
        # using server-side prepared statements is disabled by default
        'use_prepared_statements': False,
        # connection timeout is not enabled by default
        # 30 seconds timeout for a socket operation (Establishing a TCP connection or read/write operation)
        'connection_timeout': 150,
        'connection_load_balance': True,
        'binary_transfer': False
    }
}

alarm_slack_channel = '#scrum-dp-rpts-alerts'
dpsr_capacity_model = 'dpsr-capacity-model'

default_args = {
    'owner': 'DPRPTS',
}


def get_db_connection(conn_name):
    if conn_name not in conn_params:
        raise ValueError(f"Unknown connection name: {conn_name}")
    conn_info = BaseHook.get_connection(conn_name)
    conn_params[conn_name]['host'] = conn_info.host
    conn_params[conn_name]['port'] = conn_info.port
    conn_params[conn_name]['user'] = conn_info.login
    conn_params[conn_name]['password'] = conn_info.password
    conn_params[conn_name]['database'] = conn_info.schema
    return conn_params[conn_name]


class PrometheusResponse(object):
    display_name_by_result_type = {
        'matrix': 'range vectors',
        'scalar': 'scalar',
        'string': 'string',
        'vector': 'instant vector',
    }

    def __init__(self, response):
        self.response = response

    def __str__(self):
        result_type = self.display_name_by_result_type[self.response['data']['resultType']]
        result_count = self.get_timeseries_count()
        result_count_s = '' if result_count == 1 else 's'

        return 'Result type "%s" with %d metric%s' % (result_type, result_count, result_count_s)

    def is_success(self):
        return self.response['status'] == 'success'

    def get_timeseries_count(self):
        return len(self.response['data']['result'])

    def get_timeseries_value_count(self):
        value_counts = map(lambda t: len(t['values']), self.response['data']['result'])
        return max(value_counts)

    def get_result(self):
        return self.response['data']['result']


class CapacityModel(object):

    def __init__(self):
        http_auth = Variable.get('prometheus-http-auth')
        parts = http_auth.split(':')
        self.auth = HTTPBasicAuth(parts[0], parts[1])
        self.promurl = 'https://metric-query.gen.adsrvr.org/api/prom'

    def main(self, interval_start, interval_end):
        vertica_ops = get_db_connection('vertica_ops')

        SQL = """
;with utc_recurring_ids as (
    select
        mes.DateStart::date as TheDate,
        mes.ScheduleExecutionId,
        mes.DateEnd,
        ntile(2000) over (partition by mes.DateStart::date order by mes.DateEnd) as ProcessingSplit
    from ttd_dpsr.metrics_ExecutionStats mes
    where mes.DateStart >= '%s' and mes.DateStart < '%s'
      and hour(mes.DateStart) = 0 and minute(mes.DateStart) = 0
      and mes.IsSingleRun = 0
      and mes.DateEnd::date - mes.DateStart::date < 1
), resolution_ranked as (
    select
        mesh.ScheduleExecutionId,
        mesh.TransitionDate,
        ntile(200) over (partition by uri.TheDate order by mesh.TransitionDate) as ResolutionSplit
    from utc_recurring_ids uri
    join ttd_dpsr.metrics_ExecutionStateHistory mesh on mesh.ScheduleExecutionId = uri.ScheduleExecutionId
    where mesh.NewStateId = 7
)
select
    uri.TheDate,
    min(case when rr.ResolutionSplit >= 67 then rr.TransitionDate else '3000-01-01' end),
    min(case when uri.ProcessingSplit >= 1900 then uri.DateEnd else '3000-01-01' end),
    min(case when uri.ProcessingSplit >= 1990 then uri.DateEnd else '3000-01-01' end)
from utc_recurring_ids uri
join resolution_ranked rr on uri.ScheduleExecutionId = rr.ScheduleExecutionId
group by 1
order by 1
""" % (interval_start.strftime('%Y-%m-%d'), interval_end.strftime('%Y-%m-%d'))

        burndown_ranges = [
            # [ '2024-06-12T00:00:00.000Z', '2024-06-19T00:00:00.000Z' ],
        ]

        with vertica_python.connect(**vertica_ops) as connV:
            with connV.cursor() as cursorV:
                cursorV.execute(SQL)
                result = cursorV.fetchone()
                while result:
                    time_start = re.sub('(\\.\\d\\d\\d)000$', '.000Z', result[1].isoformat())
                    time_end = re.sub('(\\.\\d\\d\\d)000$', '.000Z', result[2].isoformat())
                    burndown_ranges.append([time_start, time_end])
                    result = cursorV.fetchone()

                for (time_start, time_end) in burndown_ranges:
                    self.summarise_burndown(time_start, time_end, cursorV)
                    print()

    def summarise_burndown(self, time_start, time_end, cursorV):
        report_hosts = [['E1S1', 'East01sc01', '581cc7'], ['E1S2', 'East01sc02', '6da31a'], ['E1S3', 'East01sc03', 'a7b57c'],
                        ['E1S4', 'East01sc04', '2b5bc7'], ['E1S5', 'East01sc05', '38dbed'], ['E1S6', 'East01sc06', '51d2a8'],
                        ['E1S9', 'East01sc09', '4c4758'], ['W1S1', 'West01sc01', 'a7c2cb'], ['W1S2', 'West01sc02', 'ddee85'],
                        ['W1S3', 'West01sc03', 'e8226d'], ['W1S4', 'West01sc04', '60a546'], ['W1S5', 'West01sc05', 'da4ab5'],
                        ['W1S6', 'West01sc06', 'bea8e6'], ['W1S8', 'West01sc08', '6251a5'], ['W1S9', 'West01sc09', '(0d4147|310dee)'],
                        ['W1S10', 'West01sc10', 'b3d8e9'], ['W1S11', 'West01sc11', '...'], ['.*', '.*', '.*']]

        timestamp_start = datetime.fromisoformat(time_start).timestamp()
        timestamp_end = datetime.fromisoformat(time_end).timestamp()
        time_tail = datetime.fromtimestamp(timestamp_end - 60, tz=timezone.utc).isoformat().replace("+00:00", ".000Z")
        time_delta = round(timestamp_end - timestamp_start)

        host_stats: dict[str, dict[str, list[float]]] = {}

        stage_filter = ',stage="InProgress"'
        ri_filters = {
            'non-RI': [',isResourceIntensive="False"', ',isResourceIntensive="false"'],
            'RI': [',isResourceIntensive="True"', ',isResourceIntensive="true"'],
            'Total': ['', ''],
        }

        for (host_id, source_id, tsl_id) in report_hosts:
            host_stats[host_id] = {}

            machine_filter = 'machineId=~".*RPT.*%s.*"' % host_id
            source_filter = 'report_provider_source=~"Vertica.*%s.*"' % source_id
            tsl_filter = 'machineId=~".*rpt.*%s.*",instance!~".*9d7717.*"' % tsl_id

            for (ri_label, ri_filter) in ri_filters.items():
                (ri_ucase, ri_lcase) = ri_filter

                time_filter = '[%ss]' % (time_delta + 30)

                if time_end < '2024-09-24T22:00:00.000Z':
                    full_filter = '{%s%s%s}%s' % (machine_filter, stage_filter, ri_ucase, time_filter)
                    slot_filter = '{%s}' % machine_filter if ri_label == 'Total' else '{%s%s}' % (machine_filter, ri_lcase)
                else:
                    full_filter = '{%s%s%s}%s' % (source_filter, stage_filter, ri_ucase, time_filter)
                    slot_filter = '{%s}' % source_filter if ri_label == 'Total' else '{%s%s}' % (source_filter, ri_lcase)

                reports_number_query = 'sum(rate(rptsched_scheduleexecution_timings_count%s) * %s)' % (full_filter, time_delta)
                avg_report_time_query = 'sum(rate(rptsched_scheduleexecution_timings_sum%s)) / sum(rate(rptsched_scheduleexecution_timings_count%s))' % (
                    full_filter, full_filter
                )

                reports_number = self.run_promql_query(time_tail, time_end, reports_number_query)[-1]
                avg_report_time = self.run_promql_query(time_tail, time_end, avg_report_time_query)[-1]

                if avg_report_time != avg_report_time:
                    avg_report_time = -1

                capacity_time = avg_report_time * reports_number

                # Hack: different sources have migrated on different dates, so try all
                slot_machine_filter = '{%s}' % machine_filter if ri_label == 'Total' else '{%s%s}' % (machine_filter, ri_lcase)
                slot_source_filter = '{%s}' % source_filter if ri_label == 'Total' else '{%s%s}' % (source_filter, ri_lcase)
                slot_tsl_filter = '{%s}' % tsl_filter if ri_label == 'Total' else '{%s%s}' % (tsl_filter, ri_lcase)

                slot_count_query = 'sum(scheduleexecutions_inprogress_executions%s or scheduleexecutions_inprogress_executions%s or scheduleexecutions_inprogress_executions%s)' % (
                    slot_machine_filter, slot_source_filter, slot_tsl_filter
                )
                # Get raw values for slot counts, for every point in the time series
                slot_count_raw = self.run_promql_query(time_start, time_end, slot_count_query)
                # Calculate frequencies for all different values
                slot_count_map = Counter([round(value) for value in slot_count_raw])
                # Sort all values by their frequencies
                slot_count_list = [val[0] for val in sorted(slot_count_map.items(), key=lambda key_val: key_val[1])]
                # Take the most frequent value
                slot_count = max(slot_count_list[-1], 0)

                host_stats[host_id][ri_label
                                    ] = [round(reports_number), avg_report_time,
                                         round(slot_count), capacity_time, 0.0, 100.0, 100.0]

        stats_labels = [
            "Reports_completed", "Average_time", "Slots", "Total_time", "Reports_per_hour", "Percentage_reports", "Percentage_time"
        ]

        rikeys = list(ri_filters.keys())
        for (host_id, source_id, tsl_id) in report_hosts:
            for ri_label in rikeys:
                host_stats[host_id][ri_label][4] = host_stats[host_id][ri_label][2] * max(
                    3600.0 / host_stats[host_id][ri_label][1], 0
                )  # reports per hour
                host_stats[host_id][ri_label][5] = host_stats[host_id][ri_label][0] * 100.0 / host_stats[".*"][ri_label][
                    0]  # percent reports
                host_stats[host_id][ri_label][6] = host_stats[host_id][ri_label][3] * 100.0 / host_stats[".*"][ri_label][3]  # percent time

        print("From", time_start, "to", time_end)

        for i, label in enumerate(stats_labels):
            # if i == 4:
            print(label)
            for (host_id, source_id, tsl_id) in report_hosts:
                values = list(map(lambda x: str(round(host_stats[host_id][x][i])), rikeys))
                print(host_id + '\t', "\t".join(values))
            print()

        lines: list[str] = []
        for (host_id, source_id, tsl_id) in report_hosts:
            sc = "null" if source_id == '.*' else "'%s'" % source_id
            for ri_label in rikeys:
                iri = 'true' if ri_label == 'RI' else ('false' if ri_label == 'non-RI' else 'null')
                if host_stats[host_id][ri_label][0] > 0:
                    lines.append(
                        "('{StartDate}', '{EndDate}', {SubCluster}, {IsResourceIntensive}, {ReportsCompleted}, {AverageTimeSeconds}, {Slots}, {TotalTimeSeconds}, {ReportsPerHour}, {PercentageReports}, {PercentageTime})"
                        .format(
                            StartDate=datetime.utcfromtimestamp(timestamp_start).strftime('%Y-%m-%d %H:%M:%S'),
                            EndDate=datetime.utcfromtimestamp(timestamp_end).strftime('%Y-%m-%d %H:%M:%S'),
                            SubCluster=sc,
                            IsResourceIntensive=iri,
                            ReportsCompleted=host_stats[host_id][ri_label][0],
                            AverageTimeSeconds=host_stats[host_id][ri_label][1],
                            Slots=host_stats[host_id][ri_label][2],
                            TotalTimeSeconds=host_stats[host_id][ri_label][3],
                            ReportsPerHour=host_stats[host_id][ri_label][4],
                            PercentageReports=host_stats[host_id][ri_label][5],
                            PercentageTime=host_stats[host_id][ri_label][6]
                        )
                    )

        if len(lines) > 0:
            cursorV.execute(
                "insert into ttd_dpsr.CapacityModel (StartDate, EndDate, SubCluster, IsResourceIntensive, ReportsCompleted, AverageTimeSeconds, Slots, TotalTimeSeconds, ReportsPerHour, PercentageReports, PercentageTime) values {lines}"
                .format(lines=", ".join(lines))
            )

    def run_promql_query(self, time_start, time_end, prom_query):
        r = self.query_range(prom_query, time_start, time_end, '1m')
        if not r.is_success():
            raise RuntimeError("Prometheus query failed %s" % r)

        result_count = r.get_timeseries_count()
        if result_count != 1:
            return [-1]  # raise RuntimeError('Query returned wrong result count: %s' % (result_count))

        result = r.get_result()[0]
        result_values = [float(value[1]) for value in result['values']]

        return result_values

    def query_range(self, query, start, end, step):
        ''' Wrapper around the Prometheus /api/v1/query_range endpoint
            https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries
        '''
        url_params = urllib.parse.urlencode({'query': query, 'start': start, 'end': end, 'step': step})

        url = '%s/api/v1/query_range?%s' % (self.promurl, url_params)
        json = self.query_json(url)
        return PrometheusResponse(json)

    def query_json(self, url):
        try:
            r = requests.get(url, auth=self.auth)
            if r.status_code != requests.codes.ok:
                print("Unexpected HTTP response %s %s" % (r.status_code, r.content))
                return

            return r.json()
        except Exception as e:
            print("Failed to make HTTP request")
            print(e)


def calc_capacity_model(ti):
    dag_run = ti.get_dagrun()
    dag_start = dag_run.data_interval_start
    dag_end = dag_run.data_interval_end
    interval_start = datetime.combine(dag_end, time.min) - timedelta(days=1)
    interval_end = datetime.combine(dag_end, time.min)
    print("Running DAG from ", dag_start, " to ", dag_end, " for interval from ", interval_start, " to ", interval_end)
    program = CapacityModel()
    program.main(interval_start, interval_end)


dag = TtdDag(
    dag_id=dpsr_capacity_model,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=10),
    slack_channel=alarm_slack_channel,
    schedule_interval='0 20 * * *',
    max_active_runs=1,
    run_only_latest=False,  # transforms into Airflow.DAG.catchup=False
    depends_on_past=True,
    start_date=datetime(2024, 6, 12, 0, 0, 0),
    tags=['DPRPTS', 'DPSRInfra', 'CapacityModel'],
)

# DAG Task Flow
calc_capacity = OpTask(
    op=PythonOperator(task_id="calc_capacity_model", execution_timeout=timedelta(minutes=10), python_callable=calc_capacity_model)
)
adag = dag.airflow_dag
dag >> calc_capacity
