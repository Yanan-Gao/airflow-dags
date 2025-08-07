class DatabricksRegion:
    """
    This abstraction is here because we do not have consistency in the way we name regions between our instance
    profiles and our workspaces
    """

    @classmethod
    def use(cls) -> "DatabricksRegion":
        return cls("useast", "use", "us-east-1", "s3://ttd-bigdata-logs-useast1-003576902480/databricks")

    @classmethod
    def vai(cls) -> "DatabricksRegion":
        return cls("useast", "vai", "us-east-1", "dbfs:/databricks/cluster-logs")

    @classmethod
    def de4(cls) -> "DatabricksRegion":
        return cls("de4", "de4", "eu-central-1", "dbfs:/databricks/cluster-logs")

    @classmethod
    def jp3(cls) -> "DatabricksRegion":
        return cls("jp3", "jp3", "ap-northeast-1", "dbfs:/databricks/cluster-logs")

    @classmethod
    def or5(cls) -> "DatabricksRegion":
        return cls("or5", "or5", "us-west-2", "dbfs:/databricks/cluster-logs")

    @classmethod
    def sg4(cls) -> "DatabricksRegion":
        return cls("sg4", "sg4", "ap-southeast-1", "dbfs:/databricks/cluster-logs")

    @classmethod
    def ie2(cls) -> "DatabricksRegion":
        return cls("ie2", "ie2", "eu-west-1", "dbfs:/databricks/cluster-logs")

    def __init__(self, instance_profile_region: str, workspace_region: str, aws_region: str, logs_bucket: str):
        self.instance_profile_region = instance_profile_region
        self.workspace_region = workspace_region
        self.aws_region = aws_region
        self.logs_bucket = logs_bucket
