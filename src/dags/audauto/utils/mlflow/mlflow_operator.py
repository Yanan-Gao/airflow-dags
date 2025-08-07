from typing import Dict, Optional
from airflow.operators.python import PythonVirtualenvOperator
from ttd.tasks.op import OpTask


def get_register_operator(model_name: str, test_name: str, stage: str, tags: Optional[Dict[str, str]] = None) -> OpTask:
    return OpTask(
        op=PythonVirtualenvOperator(
            task_id=f"promote_{model_name}_{test_name}_to_{stage}",
            python_callable=_register_model,
            requirements="ttd-mlflow",
            index_urls=[
                "https://pypi.org/simple", "https://nex.adsrvr.org/repository/pypi/simple",
                "https://nex.adsrvr.org/repository/ttd-pypi-dev/simple"
            ],
            # pylint: disable=use-dict-literal
            op_kwargs=dict(model_name=model_name, test_name=test_name, stage=stage, tags=tags),
        )
    )


# todo model_signature and model_inputs are not needed right now, put dummy values.
# refactor later if needed.
def _register_model(
    model_name: str, test_name: Optional[str], team_name: Optional[str], stage: str, tags: Optional[Dict[str, str]] = None
) -> str:
    # pylint: disable=import-outside-toplevel
    from ttd_mlflow import TtdMlflow, ModelFlavor
    from mlflow.models.signature import ModelSignature
    from mlflow.types.schema import Schema, ColSpec

    mlflow_client = TtdMlflow()
    input_schema = Schema([ColSpec("string", "input_column")])
    output_schema = Schema([ColSpec("string", "output_column")])
    model_signature = ModelSignature(inputs=input_schema, outputs=output_schema)
    with mlflow_client.start_run(
            team_name=team_name,
            model_name=model_name,
            model_flavor=ModelFlavor.SKLEARN,
            model_inputs=["s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/RSM/Seed_None/v=1/"],
            # a place holder, doesn't matter.
            model_signature=model_signature,
            model_obj=None,
            test_name=test_name,
    ) as ttd_model_run:
        pass

    for tag_key, tag_value in (tags or {}).items():
        mlflow_client.set_model_version_tag(model_name, ttd_model_run.model_version, key=tag_key, value=tag_value)

    if stage.lower() == 'production':
        search_result = mlflow_client.search_model_versions(f"name='{model_name}'")
        existing_versions = [v for v in search_result if v.current_stage == 'Production']
    else:
        search_result = mlflow_client.search_model_versions(f"name='{model_name}' and tag.TTD_MLFLOW.TEST_NAME='{test_name}'")
        existing_versions = [v for v in search_result if v.current_stage == 'Staging']

    mlflow_client.transition_model_version_stage(
        name=model_name,
        version=ttd_model_run.model_version,
        stage=stage,
        skip_validation=True,
    )

    # archive all old versions
    existing_versions_exclude_latest = sorted(existing_versions, key=lambda v: int(v.version))[:-1]
    for existing_version in existing_versions_exclude_latest:
        mlflow_client.transition_model_version_stage(
            name=model_name,
            version=existing_version.version,
            stage="Archived",
            skip_validation=True,
        )

    return ttd_model_run.model_version


# find the most largest number prod version
def _get_latest_prod_model_version(model_name: str):
    # pylint: disable=import-outside-toplevel
    from ttd_mlflow import TtdMlflow
    mlflow_client = TtdMlflow()
    model_versions = mlflow_client.get_latest_versions(name=model_name, stages=['Production'])
    if model_versions:
        max_version = max(model_versions, key=lambda x: x.version)
        return max_version
    else:
        return None
