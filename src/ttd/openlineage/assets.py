from typing import Dict, Optional
from ttd.cloud_provider import CloudProvider, DatabricksCloudProvider, AwsCloudProvider
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.ttdenv import TtdEnv, ProdEnv

SHARED_ASSETS_BUCKET = "ttd-dataplatform-infra-useast"

PRODUCTION_OPENLINEAGE_VERSION = "1.19.0"


class AssetsNotSupported(Exception):

    def __init__(self, cloud_provider: CloudProvider):
        self.cloud_provider = cloud_provider
        super().__init__(f"Openlineage assets are not supported for {self.cloud_provider}")


class OpenlineageAssetsConfig:
    assets_prefix_url: str
    classifier: str

    @staticmethod
    def get_assets_path(branch: Optional[str] = None) -> str:
        if branch is None:
            assets_path = f"openlineage/prod/{PRODUCTION_OPENLINEAGE_VERSION}"
        else:
            assets_path = f"openlineage/test/{branch}"
        return assets_path

    @staticmethod
    def get_asset_classifier(branch: Optional[str] = None) -> str:
        if branch is not None:
            return f"-{branch}"
        return ""

    def __init__(self, branch: Optional[str] = None):
        bucket = SHARED_ASSETS_BUCKET
        # determine the path from the branch
        assets_path = self.get_assets_path(branch)
        self.classifier = self.get_asset_classifier(branch)
        self.assets_url_prefix = f"s3://{bucket}/{assets_path}"

    def get_initscript_url(self, provider: CloudProvider) -> str:
        return f"{self.assets_url_prefix}/initscript_{provider}{self.classifier}.sh"

    def get_robust_upload_task_url(self, provider: CloudProvider) -> str:
        if provider == DatabricksCloudProvider():
            return f"{self.assets_url_prefix}/robust_upload_databricks{self.classifier}.py"
        elif provider == AwsCloudProvider():
            return f"{self.assets_url_prefix}/robust_upload_aws{self.classifier}.sh"
        raise AssetsNotSupported(provider)

    def get_initscript_config(self) -> Dict[str, Dict[str, str]]:
        return {"s3": {"destination": self.get_initscript_url(DatabricksCloudProvider()), "region": "us-east-1"}}

    def get_bootstrap_script_config(self, job_name: str, env: TtdEnv, setup_shutdown_script: bool) -> ScriptBootstrapAction:
        script_args = [job_name, "prod" if env == ProdEnv() else "test"]
        if setup_shutdown_script:
            script_args.append(self.get_robust_upload_task_url(AwsCloudProvider()))
        return ScriptBootstrapAction(path=self.get_initscript_url(AwsCloudProvider()), args=script_args)
