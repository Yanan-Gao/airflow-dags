from abc import ABC

from ttd.eldorado.databricks.region import DatabricksRegion


class DatabricksWorkspace(ABC):

    def __init__(
        self,
        account_id: str,
        region: DatabricksRegion,
        policy_id: str,
        conn_id: str,
    ):
        self.account_id = account_id
        self.region = region
        self.policy_id = policy_id
        self.conn_id = conn_id


class DevUsEastDatabricksWorkspace(DatabricksWorkspace):

    def __init__(self):
        region = DatabricksRegion.use()
        super().__init__(
            account_id="503911722519", region=region, policy_id="0006445D057B79B8", conn_id=f"databricks-aws-dev-{region.workspace_region}"
        )
