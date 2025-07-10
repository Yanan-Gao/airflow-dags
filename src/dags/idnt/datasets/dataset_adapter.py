from ttd.datasets.dataset import Dataset


class DatasetAdapter:
    """
    Dataset adapter that provides additional API for the Dataset class from the ttd folder.

    It might make sense to move to the Dataset class eventually.
    """

    def __init__(self, dataset: Dataset):
        self.dataset = dataset

    def __getattr__(self, name):
        return getattr(self.dataset, name)

    @property
    def location(self) -> str:
        """Location as URL-string."""
        # Include the version in the location.
        version_path_segment: str = "" if self.dataset.version is None else f"/v={self.dataset.version}"
        # The schema determines which S3 client implementation Spark uses. In AWS, "s3" should cause the
        # best one to be used. Note that this is different in Hadoop where "s3a" is for the most up-to-date
        # client implementation.
        # See https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-file-systems.html
        base_location: str = self.dataset.get_dataset_path().replace("s3a:", "s3:")
        return f"{base_location}{version_path_segment}"
