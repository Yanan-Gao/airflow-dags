from airflow.models.dag import DAG
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor

from ttd.ec2.ec2_subnet import EmrSubnets
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.operators.ttd_emr_create_job_flow_operator import TtdEmrCreateJobFlowOperator
from ttd.ttdenv import TtdEnvFactory, TtdEnv
from typing import Dict, Optional, Any


def run_python_app_in_emr_operator(
    dag: DAG,
    application_name: str,
    process_name: str,
    s3_tar_file_path: str,
    app_run_cmd: str,
    instance_type: EmrInstanceType,
    logs_uri: str,
    ec2_key_name: Optional[str] = None,
    env: TtdEnv = TtdEnvFactory.get_from_system(),
    user: str = 'airflow',
    cluster_tags: Optional[Dict[str, str]] = None
) -> TtdEmrCreateJobFlowOperator:
    """
    Installs a python3 app on a single EMR master in a virtual-env and runs it. The
    installation and run are both added as job steps.
    :param dag: DAG to attach this operator to
    :param application_name: Application name
    :param process_name: Name of process/component within the application
    :param s3_tar_file_path: The python app scripts, along with requirements.txt
        should be wrapped up in a .tar file and placed at this location. This will
        be unpacked on the server, requirements.txt installed, and the app launched
    :param app_run_cmd: The command(s) to run the app, e.g. python <app-script> <args>.
        Any airflow macros in this string will be expanded.
    :param instance_type: EC2 instance type to use
    :param logs_uri: S3 location where logs will be saved
    :param ec2_key_name: Optional, if provided, EC2 private ssh-key name to use
    :param env: Optional, defaults to Prod - used as a label
    :param user: Optional, defaults to airflow. During testing, you can instead
        provide your TTD username, used as a label on the cluster
    :param cluster_tags: Optional, defaults to None. Tags attached to cluster
    :returns: The EMR job-flow operator that installs and runs of the python app
        as job-steps
    """
    tags = [
        {
            'Key': 'Job',
            'Value': process_name
        },
        {
            'Key': 'Environment',
            'Value': env.execution_env
        },
        {
            'Key': 'Process',
            'Value': process_name
        },
        {
            'Key': 'Source',
            'Value': 'Airflow'
        },
        {
            'Key': 'Resource',
            'Value': 'EMR'
        },
    ]

    if cluster_tags is not None:
        tags.extend([{'Key': key, 'Value': value} for key, value in cluster_tags.items()])

    tar_file_name = s3_tar_file_path.split('/')[-1]
    app_base_name = tar_file_name.split('.tar')[0]
    app_dir = f'/home/hadoop/{app_base_name}'
    setup_app_commands = (
        f'mkdir {app_dir};'
        f'cd {app_dir};'
        f'aws s3 cp {s3_tar_file_path} {tar_file_name};'
        f'tar -xvf {tar_file_name};'
        f'python3 -m venv venv;'
        f'source venv/bin/activate;'
        f'python -m pip install --upgrade pip;'
        f'yes | pip install -r requirements.txt;'
    )
    run_app_commands = (f'cd {app_dir};'
                        f'source venv/bin/activate;'
                        f'{app_run_cmd};')

    failure_action = 'TERMINATE_CLUSTER'  # Change to CANCEL_AND_WAIT to debug issues
    spark_steps = [{
        'Name': 'setup_python_venv',
        'ActionOnFailure': failure_action,
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['bash', '-c', setup_app_commands],
        }
    }, {
        'Name': 'calculate_reach_curves',
        'ActionOnFailure': failure_action,
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['bash', '-c', run_app_commands],
        }
    }]

    job_flow_overrides: Dict[str, Any] = {
        'Name': process_name,
        'ReleaseLabel': 'emr-5.34.0',
        'LogUri': logs_uri,
        'Applications': [{
            'Name': 'Spark'
        }],
        'Instances': {
            'InstanceGroups': [{
                'Name': 'Primary node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': instance_type.instance_name,
                'InstanceCount': 1,
            }],
            'KeepJobFlowAliveWhenNoSteps':
            False,
            'TerminationProtected':
            False,
            'Ec2SubnetIds': [EmrSubnets.Public.useast_emr_1a()]
        },
        'Steps': spark_steps,
        'JobFlowRole': 'DataPipelineDefaultResourceRole',
        'ServiceRole': 'DataPipelineDefaultRole',
        'EbsRootVolumeSize': 50,
        'Tags': tags
    }

    if ec2_key_name:
        job_flow_overrides['Instances']['Ec2KeyName'] = ec2_key_name

    create_job_flow_task_id = f'creation_task_{application_name}'
    create_job_flow_operator = TtdEmrCreateJobFlowOperator(
        dag=dag, task_id=create_job_flow_task_id, environment=env, region_name='us-east-1', job_flow_overrides=job_flow_overrides
    )

    check_job_flow = EmrJobFlowSensor(
        dag=dag,
        task_id=f'monitoring_task_{application_name}',
        job_flow_id='{{ task_instance.xcom_pull(task_ids="%s", key="return_value") }}' % create_job_flow_task_id
    )

    return create_job_flow_operator >> check_job_flow
