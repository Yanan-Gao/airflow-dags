from ttd.alicloud.alicloud_instance_type import AliCloudInstanceType


class ElDoradoAliCloudInstanceTypes:

    def __init__(self, instance_type: AliCloudInstanceType):
        self.instance_type = instance_type
        self.data_disk_size_gb = 40
        self.data_disk_count = 4
        self.sys_disk_size_gb = 60
        self.node_count = 1

    def with_sys_disk_size_gb(self, sys_disk_size_gb):
        self.sys_disk_size_gb = sys_disk_size_gb
        return self

    def with_data_disk_size_gb(self, data_disk_size_gb):
        self.data_disk_size_gb = data_disk_size_gb
        return self

    def with_data_disk_count(self, data_disk_count):
        self.data_disk_count = data_disk_count
        return self

    def with_node_count(self, node_count):
        self.node_count = node_count
        return self
