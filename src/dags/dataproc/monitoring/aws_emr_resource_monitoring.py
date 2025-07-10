import logging
from datetime import datetime, timedelta
from typing import Dict

import boto3
from airflow.operators.python import PythonOperator

import ttd.slack.slack_groups
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.metrics.opentelemetry.ttdopentelemetry import get_or_register_gauge, push_all, TtdGauge

job_name = "aws-emr-resource-monitoring"

job_schedule_interval = "*/10 * * * *"
job_start_date = datetime(2024, 4, 1, 0, 0)

volume_types = ["gp2", "gp3"]

dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    run_only_latest=True,
    tags=["Monitoring", ttd.slack.slack_groups.dataproc.jira_team],
    default_args={"owner": ttd.slack.slack_groups.dataproc.jira_team},
    enable_slack_alert=False,
)
adag = dag.airflow_dag


def check_aws_service_quotas() -> None:
    quota_gauge: TtdGauge = get_or_register_gauge(
        job=job_name,
        name="airflow_aws_ebs_quota",
        description="AWS EBS gp2 volume quota in TiB",
    )
    quota_usage_gauge: TtdGauge = get_or_register_gauge(
        job=job_name,
        name="airflow_aws_ebs_quota_usage",
        description="AWS EBS volume quota usage in TiB",
    )
    volume_type_quotas = [("gp2", "L-D18FCD1D"), ("gp3", "L-7A658B76")]
    for volume_type, quota_code in volume_type_quotas:
        ec2_client = boto3.client("ec2")
        service_quotas_client = boto3.client("service-quotas")
        filters = [{"Name": "volume-type", "Values": [volume_type]}]

        quota = service_quotas_client.get_service_quota(ServiceCode="ebs", QuotaCode=quota_code)
        quota_gauge.labels({"volume_type": volume_type}).set(quota["Quota"]["Value"])

        volumes = ec2_client.describe_volumes(Filters=filters)  # type: ignore
        total_usage_tib = sum(volume["Size"] for volume in volumes["Volumes"]) / 1024
        quota_usage_gauge.labels({"volume_type": volume_type}).set(total_usage_tib)

    push_all(job=job_name)


def get_volumes():
    volumes = []
    ec2_client = boto3.client("ec2")
    paginator = ec2_client.get_paginator("describe_volumes")
    filters = [
        {
            "Name": "volume-type",
            "Values": volume_types
        },
        {
            "Name": "tag:Source",
            "Values": ["Airflow"]
        },
        {
            "Name": "tag:Resource",
            "Values": ["EMR"]
        },
    ]
    page_iterator = paginator.paginate(Filters=filters, PaginationConfig={"PageSize": 1000})
    i = 0
    for page in page_iterator:
        i = i + 1
        logging.getLogger().info("Processing page: %d, volumes: %d", i, len(page["Volumes"]))
        volumes.extend(page['Volumes'])
    return volumes


def check_emr_ebs_volumes() -> None:
    volume_usage_gauge: TtdGauge = get_or_register_gauge(
        job=job_name,
        name="airflow_emr_ebs_volume_total_usage",
        description="Total size of EBS volumes in TiB used for EMR clusters",
    )
    volumes = get_volumes()

    for volume_type in volume_types:
        total_usage_tib = (sum(volume["Size"] for volume in volumes if volume["VolumeType"] == volume_type) / 1024)
        volume_usage_gauge.labels({"volume_type": volume_type}).set(total_usage_tib)

    push_all(job=job_name)


def get_subnet_to_available_ip(subnet_ids: str) -> Dict[str, int]:
    subnet_to_available_ip = {}
    ec2 = boto3.client('ec2')
    response = ec2.describe_subnets(SubnetIds=subnet_ids)
    subnets = response['Subnets']
    for subnet in subnets:
        subnet_to_available_ip[subnet['SubnetId']] = subnet['AvailableIpAddressCount']
    return subnet_to_available_ip


def push_metric(subnet_id: str, available_ips: int) -> None:
    available_ips_gauge: TtdGauge = get_or_register_gauge(
        job=job_name,
        name='airflow_aws_emr_subnet_available_ips',
        description='AWS EMR Subnet Available IPs',
    )

    available_ips_gauge.labels({'subnet_id': subnet_id}).set(available_ips)

    push_all(job=job_name)

    print(f"METRIC PUSHED: Subnet {subnet_id} Available IPs {available_ips}")


def check_emr_subnets() -> None:
    from ttd.ec2.ec2_subnet import ProductionAccount

    subnet_ids = ProductionAccount.default_vpc.all()
    subnet_to_available_ip = get_subnet_to_available_ip(subnet_ids)

    for subnet, available_ip in subnet_to_available_ip.items():
        print(f"subnet: {subnet}, available_ip: {available_ip}")
        push_metric(subnet, available_ip)


check_aws_service_quotas = OpTask(
    op=PythonOperator(
        task_id="check_aws_service_quotas",
        python_callable=check_aws_service_quotas,
        dag=adag,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
)

check_emr_ebs_volumes = OpTask(
    op=PythonOperator(
        task_id="check_emr_ebs_volumes",
        python_callable=check_emr_ebs_volumes,
        dag=adag,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
)

check_emr_subnets = OpTask(
    op=PythonOperator(
        task_id="check_emr_subnets",
        python_callable=check_emr_subnets,
        dag=adag,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
)

dag >> check_aws_service_quotas
dag >> check_emr_ebs_volumes
dag >> check_emr_subnets
