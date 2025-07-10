"""Airflow DAG to enumerate AWS IAM users TTD creates for partners to access REDS and Exposure feeds.

It queries all relevant AWS accounts where REDS users are maintained and enumerates each user's permission policies. If
a user has access to any S3 prefix where we may store REDS or Exposure feeds, the user is determined to be a REDS User.

All REDS users are dumped into S3 for further consumption, e.g. by SecOps team for anomalous access detection.

See https://atlassian.thetradedesk.com/confluence/x/35CmE for a design spec.
"""
import fnmatch
import logging
import re
import typing
from datetime import datetime, timedelta
from typing import List

import airflow.models
import boto3
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask

from dags.datasrvc.reds import feed_backfill, exposurefeed
from dags.puma.reds.helpers import redsfeed
from ttd.slack.slack_groups import DATASRVC
from ttd.ttdslack import dag_post_to_slack_callback

job_name = 'reds_list_iam_users'

dag = TtdDag(
    dag_id=job_name,
    start_date=datetime(2024, 2, 1, 0, 0),
    schedule_interval=timedelta(days=1),
    run_only_latest=True,
    slack_channel='#scrum-data-services-alarms',
    tags=[DATASRVC.team.name, DATASRVC.team.jira_team],
    max_active_runs=1,
    on_failure_callback=
    dag_post_to_slack_callback(dag_name=job_name, step_name='parent dagrun', slack_channel='#scrum-data-services-alarms'),
)

adag = dag.airflow_dag  # MUST be explicitly defined for Airflow to parse this file as a DAG

# REDS users live in accounts below:
ttd_production = '003576902480'  # historic account with active REDS users
ttd_tam_accounts = '949199315127'  # new users created via AutoIAM are here
aws_accounts = [ttd_production, ttd_tam_accounts]

# We can ignore below known internal users
_INTERNAL_USERNAMES = {
    'airflow',
    'bamboo.automation',
    'service.logextractor-readonly'
    'service.lwdb.hadoop',
    'service.netflow.etl',
    'service.opsplatform',
    'service.task',
    'ttd-sherpa',
    'ttd-stewardship',
}

# We can ignore any user who belongs to these known internal groups
_INTERNAL_GROUPS = {
    'Admin',
    'Oncall',
    'Monitoring',
    'ttd_developer_base',
}

_REDS_ACCESS_BUCKET = 'thetradedesk-useast-partner-datafeed-reds-access'
_REDS_BUCKET_ARN = 'arn:aws:s3:::thetradedesk-useast-partner-datafeed'


def _get_exposure_feeds() -> List[exposurefeed.ExposureFeed]:
    """Returns Exposure feed definitions from ProvDB."""
    sql_hook = MsSqlHook(mssql_conn_id='schedreportingdb-readonly')
    conn = sql_hook.get_conn()
    return exposurefeed.ExposureFeed.all(conn)


def list_exposure_prefixes() -> List[str]:
    """Lists all S3 prefixes that have Exposure feeds.

    Not all prefixes may actually exist in S3, as some feeds have never been enabled, or existing objects have been
    removed in S3. This does not matter, as we want to find all users that have been given permission to any prefix that
    could be Exposure feeds destination.

    Returns a list of prefixes trimmed at /date=<date>, for example:
        - liveramp/exposure/brand/partner1/advertisers/advertiser1/1/date=<date>
        - liveramp/exposure/brand/partner2/advertisers/advertiser2/1/date=<date>
    """
    # for feeds in Draft status, it may be possible that destination_location isn't set yet
    feeds = [feed for feed in _get_exposure_feeds() if feed.feed_destination is not None]
    logging.info(f'Parsing {len(feeds)} Exposure feeds...')
    # as of Q1'2024, we only care about S3
    s3_feeds = [feed for feed in feeds if feed.feed_destination_type_name == 'S3']
    logging.info(f'Found {len(s3_feeds)} feeds that deliver to S3')

    # Skip unresolvable feeds, only log the warning
    prefixes = list({
        feed.populate_destination_date_prefix(allowed_placeholders=['<date>'], raise_on_unresolved=False)
        for feed in s3_feeds
    })
    logging.info(f'Found {len(prefixes)} relevant S3 prefixes')
    return prefixes


def _get_reds_feeds() -> List[redsfeed.RedsFeed]:
    """Returns REDS feed definitions from ProvDB."""
    sql_hook = MsSqlHook(mssql_conn_id='provdb-readonly')
    conn = sql_hook.get_conn()
    return redsfeed.RedsFeed.all(conn)


def list_reds_prefixes() -> List[str]:
    """Lists all potential S3 prefixes that have REDS feeds.

    Not all prefixes may actually exist in S3, as some feeds have never been enabled, or existing objects have been
    removed in S3. This does not matter, as we want to find all users that have been given permission to any prefix that
    could be REDS feeds destination.

    Returns a list of prefixes trimmed at /date=<date>, for example:
        - whqqw48/redf5
        - frqci16/advertisers/ymxregn/reds/videoevents/1
        - frqci16/advertisers/ymxregn/reds/videoevents/1aggregated
    """
    # for feeds in Draft status, it may be possible that destination_location isn't set yet
    feeds = [feed for feed in _get_reds_feeds() if feed.destination_location is not None]
    logging.info(f'Parsing {len(feeds)} REDS feeds...')
    # as of Q1'2024, we only care about S3
    s3_feeds = [feed for feed in feeds if feed.destination_type == 'S3']
    logging.info(f'Found {len(s3_feeds)} feeds that deliver to S3')

    prefixes = list(
        set([feed.source_path for feed in s3_feeds] + [feed.destination_path for feed in s3_feeds] +  # aggregated feeds
            list(feed_backfill.get_back_fill_prefixes())  # feeds that are not in ProvDB
            )
    )

    logging.info(f'Found {len(prefixes)} relevant S3 prefixes')
    return prefixes


def _is_internal_user(user_name: str, user_groups: List[str]) -> bool:
    """Checks whether a given user is a known internal TTD user."""
    return user_name in _INTERNAL_USERNAMES or any([group in _INTERNAL_GROUPS for group in user_groups])


def _is_get_action(action: typing.Optional[str]) -> bool:
    """Checks whether a given action allows downloading an object from S3."""
    if not action:
        return False
    if '[' in action or ']' in action:
        raise ValueError(f'Unexpected action pattern: {action}')
    return any([fnmatch.fnmatch(x, action) for x in ["s3:GetObject", "s3:GetObjectAcl"]])


def _is_allow_get_action(user, statement: typing.Optional[dict]) -> bool:
    """Checks whether a given statement allows downloading an object from S3."""
    if statement is None:
        return False
    if not isinstance(statement, dict) or 'Effect' not in statement:
        logging.warning(f'Malformed statement for {user}: {statement}')
        return False
    if statement.get('Action') == 'AssumeRole':
        logging.warning(f'{user} can assume role: {statement}')
    if statement.get('Effect') != 'Allow':
        return False
    return any([_is_get_action(action) for action in _as_list(statement.get('Action'))])


def _as_list(element):
    return element if isinstance(element, list) else [element]


def _all_policy_statements(user) -> typing.Generator[dict, None, None]:
    """Generates all policy statements for a given user.

    This includes user's inlined or attached policies, as well as all policies for the groups that the user belongs to.
    """
    for policy in user.policies.all():
        statement = policy.policy_document.get("Statement")
        yield from _as_list(statement)
    for policy in user.attached_policies.all():
        statement = policy.default_version.document.get("Statement")
        yield from _as_list(statement)

    for group in user.groups.all():
        for policy in group.policies.all():
            statement = policy.policy_document.get("Statement")
            yield from _as_list(statement)
        for policy in group.attached_policies.all():
            statement = policy.default_version.document.get("Statement")
            yield from _as_list(statement)


def _is_resource_in_prefixes(resource: typing.Optional[str], prefixes: List[str]) -> bool:
    """Checks whether a given AWS resource belongs to any of the given prefixes.

    Since we are pattern matching on AWS resource ARNs, the expected format for prefixes is:
       <bucket>/<prefix>/
    It should start with an ARN of a bucket, followed by a prefix, and end with a forward slash. For example:
        arn:aws:s3:::thetradedesk-useast-partner-datafeed/partner/redf5/
    """
    if not resource:
        return False
    if '[' in resource or ']' in resource:
        raise ValueError(f'Unexpected resource pattern: {resource}')
    return any([fnmatch.fnmatch(prefix, resource) for prefix in prefixes])


def find_users_in(account: str, task_instance: airflow.models.TaskInstance, run_id: str, execution_date: datetime, **kwargs) -> str:
    """Finds REDS users in the given account and stores intermediate results in S3.

    For every user we parse all permission policies that are granted to the user. If any of the policies allows the user
    to download objects from an S3 prefix that is identified as REDS / Exposure delivery destination, we identify the
    user as a REDS user.

    Returns a key in S3 that stores the found users in the given account.
    """
    # If we pass more metadata than just a list of prefixes or the amount increases, we should stop using XCOM. As of
    # Q1'2024, the total data passed is way below 1MB.
    prefixes = [
        f'{_REDS_BUCKET_ARN}/{prefix}/' for prefix in
        set(task_instance.xcom_pull(task_ids='list_exposure_prefixes') + task_instance.xcom_pull(task_ids='list_reds_prefixes'))
    ]
    logging.info(f'Total of {len(prefixes)} Exposure and REDS prefixes')

    sts = boto3.client('sts')
    session_id = re.sub(':', "_", run_id)
    assumed_role = sts.assume_role(
        RoleArn=f'arn:aws:iam::{account}:role/ro-user-group-role-policies',
        RoleSessionName=f'reds-{account}-{session_id}',  # must be less than 64 chars
    )
    credentials = assumed_role['Credentials']

    iam = boto3.resource(
        'iam',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
    )

    users = list(iam.users.all())
    logging.info(f'Total of {len(users)} users in account {account}')

    rows = []
    for user in users:
        if _is_internal_user(user.user_name, [g.group_name for g in user.groups.all()]):
            continue
        resources = []
        for statement in _all_policy_statements(user):
            if not _is_allow_get_action(user, statement):
                continue
            for resource in _as_list(statement.get('Resource')):
                if _is_resource_in_prefixes(resource, prefixes):
                    resources.append(resource)
        if len(resources) > 0:
            rows.append(f'{user.arn}\t{",".join(resources)}')
            logging.info(f'{user.arn}\t{",".join(resources)}')

    logging.info(f'Found {len(rows)} REDS and Exposure users in {account}.')

    key = f'users/{execution_date.year}/{execution_date.month}/{execution_date.day}/{account}.tsv'
    logging.info(f'Storing results for account {account} in s3://{_REDS_ACCESS_BUCKET}/{key}')

    s3_hook = AwsCloudStorage(conn_id='aws_default')
    s3_hook.load_string('\n'.join(rows), key, bucket_name=_REDS_ACCESS_BUCKET, replace=True)
    return key


def persist_users(task_instance: airflow.models.TaskInstance, execution_date: datetime, **kwargs) -> None:
    """Merges and persists REDS users in S3."""
    s3_hook = AwsCloudStorage(conn_id='aws_default')

    rows: List[typing.Any] = []
    delete_keys = []
    for account in aws_accounts:
        s3_key = task_instance.xcom_pull(task_ids=f'find_users_in_{account}')
        if s3_key:
            rows.extend(s3_hook.read_key(s3_key, _REDS_ACCESS_BUCKET).split('\n'))
            delete_keys.append(s3_key)
            logging.info(f'Adding to delete {s3_key}')
    s3_hook.remove_objects_by_keys(_REDS_ACCESS_BUCKET, delete_keys)

    if len(rows) < 500:
        raise RuntimeError(f'Expected to find more than 500 users, found {len(rows)}.')

    key = f'users/{execution_date.year}/{execution_date.month}/{execution_date.day}/users.tsv'
    logging.info(f'Persisting {len(rows)} users to s3://{_REDS_ACCESS_BUCKET}/{key}.')

    data = '\n'.join(rows)
    s3_hook.load_string(data, key, bucket_name=_REDS_ACCESS_BUCKET, replace=True)


list_prefixes_tasks = [
    OpTask(op=PythonOperator(
        dag=adag,
        python_callable=list_exposure_prefixes,
        task_id='list_exposure_prefixes',
    )),
    OpTask(op=PythonOperator(
        dag=adag,
        python_callable=list_reds_prefixes,
        task_id='list_reds_prefixes',
    ))
]

find_users_tasks = [
    OpTask(
        op=PythonOperator(
            dag=adag,
            provide_context=True,
            python_callable=find_users_in,
            op_kwargs={'account': account},
            task_id=f"find_users_in_{account}",
        )
    ) for account in aws_accounts
]

persist_users = OpTask(op=PythonOperator(
    dag=adag,
    task_id='persist_users',
    python_callable=persist_users,
    provide_context=True,
))

# All list_prefixes_tasks tasks can be run in parallel.
# Once we have all prefixes, we can run find_users_tasks tasks in parallel.
# Persist all users in S3 after we found them.
for list_prefix_task in list_prefixes_tasks:
    for find_users_task in find_users_tasks:
        dag >> list_prefix_task >> find_users_task >> persist_users
