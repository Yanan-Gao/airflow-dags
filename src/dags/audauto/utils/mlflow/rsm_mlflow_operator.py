from typing import Dict, Optional
from airflow.operators.python import PythonVirtualenvOperator
from ttd.tasks.op import OpTask


def get_register_new_prod_version_operator(tags: Optional[Dict[str, str]] = None) -> OpTask:
    return OpTask(
        op=PythonVirtualenvOperator(
            task_id="promote_rsm_prod_model",
            python_callable=_promote_prod_model_version,
            requirements="ttd-mlflow",
            index_urls=[
                "https://pypi.org/simple", "https://nex.adsrvr.org/repository/pypi/simple",
                "https://nex.adsrvr.org/repository/ttd-pypi-dev/simple"
            ],
            # pylint: disable=use-dict-literal
            op_kwargs=dict(tags=tags),
        )
    )


def _promote_prod_model_version(tags: Optional[Dict[str, str]] = None):
    from dags.audauto.utils.mlflow.mlflow_operator import _register_model, _get_latest_prod_model_version
    from dags.audauto.utils.mlflow.rsm_config import RSMConfig
    import json
    import time
    import botocore.exceptions

    # update mapping file, for forecasting service.
    def _update_mapping(mapping_file_key: str, tags: Dict[str, str], mlflow_version: str):
        import boto3
        from dags.audauto.utils.mlflow.rsm_config import RSMConfig

        bucket = RSMConfig.MODEL_MAPPING_LOG_BUCKET

        s3 = boto3.client("s3")

        # Prepare new line
        embedding_path = tags.get(RSMConfig.SEED_EMBEDDING_PATH_TAG_NAME)
        if not embedding_path or not mlflow_version:
            raise ValueError("Missing required embedding_path tag and mlflow_version values to update mapping")

        # Extract the identifier from the path (e.g., '20250520000000' from
        # 'configdata/prod/audience/embedding/RSMV2/v=1/20250520000000/')
        embedding_datetime_version = embedding_path.rstrip("/").split("/")[-1]
        new_line = f"{embedding_datetime_version},{mlflow_version}"

        max_retries = 5
        for attempt in range(max_retries):
            try:
                # Download existing content
                try:
                    response = s3.get_object(Bucket=bucket, Key=mapping_file_key)
                    existing_data = response["Body"].read().decode("utf-8").strip().splitlines()
                except s3.exceptions.NoSuchKey:
                    existing_data = []

                existing_data = [
                    line.strip() for line in existing_data if line.strip() and not line.startswith(embedding_datetime_version + ",")
                ]

                # Prepend new line and keep only latest 30 entries
                updated_lines = [new_line] + existing_data[:29]
                updated_content = "\n".join(updated_lines) + "\n"

                s3.put_object(Bucket=bucket, Key=mapping_file_key, Body=updated_content.encode("utf-8"))
                return
            except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as e:
                if attempt < max_retries - 1:
                    time.sleep(3)
                else:
                    raise RuntimeError(f"Failed to update mapping file after {max_retries} attempts") from e

    prev_emb_paths = {}
    current_version = _get_latest_prod_model_version(RSMConfig.MODEL_NAME)
    if current_version is not None:
        paths_str = current_version.tags[RSMConfig.PREV_SEED_EMBEDDING_PATHS_TAG_NAME]
        current_emb_paths = json.loads(paths_str)
        # keep most recent N - 1 embedding versions
        recent_emb_paths = {
            k: current_emb_paths[k]
            for k in sorted(current_emb_paths, key=int, reverse=True)[:RSMConfig.PREV_EMBEDDING_TRACK_COUNT - 1]
        }
        recent_emb_paths[current_version.version] = current_version.tags[RSMConfig.SEED_EMBEDDING_PATH_TAG_NAME]
        prev_emb_paths = recent_emb_paths

    (tags := tags or {})[RSMConfig.PREV_SEED_EMBEDDING_PATHS_TAG_NAME] = json.dumps(prev_emb_paths)

    mlflow_version = _register_model(
        model_name=RSMConfig.MODEL_NAME, test_name=None, team_name=RSMConfig.TEAM_NAME, stage="Production", tags=tags
    )

    # temporary path, this is used previously. can remove this once the file pointer changed in forecasting side.
    _update_mapping("configdata/test/audience/embedding/RSMV2/RSMv2SensitiveDensityTest/v=1/_MAPPING", tags, mlflow_version)
    _update_mapping("configdata/prod/audience/embedding/RSMV2/v=1/_MAPPING", tags, mlflow_version)
