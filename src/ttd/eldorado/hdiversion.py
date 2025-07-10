class HDIVersion:

    def __init__(self, version: str):
        self._version = version

    @property
    def version(self) -> str:
        """
        String representation of the version
        :return:
        """
        return self._version


class HDIClusterVersions:
    """
    List of Azure HDInsight versions.
    Version of HDI designated to each Spark major.minor version will be updated automatically upon release of the new Spark patch version.
    """

    HDInsight51: HDIVersion = HDIVersion("5.1")
    "Azure HDInsight 5.1 (Spark 3.3.1)"

    AzureHdiSpark33: HDIVersion = HDInsight51
    "Azure HDInsight 5.1 (Spark 3.3.1), Spark patch version could be updated automatically"
