from typing import List


class AwsStorageVolume:

    def __init__(self, size: int, volume_type: str):
        self.size = size
        self.volume_type = volume_type


class AwsInstanceStorage:
    pass


class EbsAwsInstanceStorage(AwsInstanceStorage):
    pass


class SsdAwsInstanceStorage(AwsInstanceStorage):

    def __init__(self, volumes: List[AwsStorageVolume]):
        self.volumes = volumes
