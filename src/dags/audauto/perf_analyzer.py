import copy
from dags.audauto.utils import utils
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.docker import DockerEmrClusterTask, DockerRunEmrTask


def download_model(model_path: str) -> str:
    build_hd_directory = "hdfs dfs -mkdir -p /opt"
    build_directory = "mkdir -p opt/ttd/models/kongming/1"
    aws_base_command = "aws s3 cp"
    model_path = model_path if model_path else "s3://thetradedesk-mlplatform-us-east-1/models/prod/kongming/bidrequest_model_onnx_userdata/20250305"
    destination = "opt/ttd/models/kongming/1 --recursive"
    copy_directory = "hdfs dfs -put $(pwd)/opt/* /opt/"
    return f"{build_hd_directory}; {build_directory} && {aws_base_command} {model_path} {destination}; {copy_directory}; "


def download_model_config(config_path: str):
    build_directory = "mkdir -p opt/ttd/models/kongming"
    aws_base_command = "aws s3 cp"
    model_path = config_path if config_path else "s3://thetradedesk-mlplatform-us-east-1/models/prod/kongming/perf_analyzer/config.pbtxt"
    destination = "opt/ttd/models/kongming/"

    copy_file = "hdfs dfs -put $(pwd)/opt/ttd/models/kongming/config.pbtxt /opt/ttd/models/kongming"
    return f"{build_directory} && {aws_base_command} {model_path} {destination} && echo opt/ttd/models/kongming; {copy_file} && hdfs dfs -cat /opt/ttd/models/kongming/config.pbtxt "


def download_model_json(input_path: str):
    build_directory = "mkdir -p opt/ttd/data"
    aws_base_command = "aws s3 cp"
    model_path = input_path if input_path else "s3://thetradedesk-mlplatform-us-east-1/models/prod/kongming/perf_analyzer/kongming.json"
    destination = "opt/ttd/data/"

    copy_directory = "hdfs dfs -put $(pwd)/opt/ttd/data /opt/ttd"
    return f"{build_directory} && {aws_base_command} {model_path} {destination} && echo opt/ttd/data/kongming.json; {copy_directory} && hdfs dfs -cat /opt/ttd/data/kongming.json "


def run_perf_analyzer():
    create_directory = "hdfs dfs -get /opt $(pwd)/; ls -R $(pwd)/opt; cat $(pwd)/opt/ttd/data/kongming.json"

    base_command = "docker run"
    arguments = '--name triton --rm -d --cpuset-cpus="0-7" --memory="20g" -v $(pwd)/opt/ttd/models:/models -p 8000:8000 -p 8001:8001 -p 8002:8002'
    image = "insecure-production-docker.adsrvr.org/ttd/triton/server:2.1.29"
    command = "tritonserver --model-repository=/models --backend-config=onnxruntime,default-max-batch-size=0,disable_auto_batching=1 --model-control-mode=poll --repository-poll-secs=60 --grpc-cq-count=2"

    docker_client = create_client()

    return f"{create_directory}; {base_command} {arguments} {image} {command}; {docker_client}"


def create_client():
    base_command = "docker run"
    arguments = '--rm --cpuset-cpus="8-15" --net host -v $(pwd)/opt/ttd/data:/data'
    image = "nvcr.io/nvidia/tritonserver:25.01-py3-sdk"
    command = "perf_analyzer"
    command_arguments = "-u localhost:8001 -m kongming -i grpc -a --input-data=/data/kongming.json --warmup-request-count=10000 --request-count=100000 --request-rate-range=5000"
    return f"{base_command} {arguments} {image} {command} {command_arguments}"


def create_perf_analyzer_cluster(model_path: str = "", config_path: str = "", input_path: str = ""):
    docker_registry = "insecure-production-docker.adsrvr.org"
    docker_image_name = "ttd/triton/server"
    docker_image_tag = "2.1.29"
    instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[C5.c5_4xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )

    perf_analyzer_cluster = DockerEmrClusterTask(
        name="PerfAnalyzer",
        image_name=docker_image_name,
        image_tag=docker_image_tag,
        docker_registry=docker_registry,
        master_fleet_instance_type_configs=instance_type_configs,
        core_fleet_instance_type_configs=instance_type_configs,
        cluster_tags=utils.cluster_tags,
        emr_release_label="emr-6.9.0",
        enable_prometheus_monitoring=True,
        additional_application_configurations=copy.deepcopy(utils.get_application_configuration()),
    )

    download_model_command = DockerRunEmrTask("DownloadModel", download_model(model_path))
    download_config_command = DockerRunEmrTask("DownloadModelConfig", download_model_config(config_path))
    download_input_command = DockerRunEmrTask("DownloadModelInput", download_model_json(input_path))
    run_command = DockerRunEmrTask("RunPerfAnalyzer", run_perf_analyzer())

    perf_analyzer_cluster.add_sequential_body_task(download_model_command)
    perf_analyzer_cluster.add_sequential_body_task(download_config_command)
    perf_analyzer_cluster.add_sequential_body_task(download_input_command)
    perf_analyzer_cluster.add_sequential_body_task(run_command)

    return perf_analyzer_cluster
