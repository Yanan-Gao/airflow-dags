import unittest
from collections import defaultdict
from unittest.mock import Mock, patch

from ttd.eldorado.aws.emr_monitor_cluster_startup import EmrMonitorClusterStartup
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes


class EmrMonitorClusterStartupTest(unittest.TestCase):

    def setUp(self):
        self.emr_monitor = EmrMonitorClusterStartup(
            task_id='task_id',
            cluster_task_name='cluster_task_name',
            job_flow_id='job_flow_id',
            aws_conn_id='aws_conn_id',
            region_name='region_name',
            master_fleet_instance_type_configs=EmrFleetInstanceTypes([], 1),
            core_fleet_instance_type_configs=EmrFleetInstanceTypes([], 1)
        )

    def test__get_instance_counts_by_fleet(self):
        mock_emr_client = Mock()
        mock_paginator = Mock()
        mock_emr_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{
            'Instances': [{
                'InstanceType': 'm5.xlarge',
                'InstanceFleetId': 'fleet-1'
            }, {
                'InstanceType': 'm5.xlarge',
                'InstanceFleetId': 'fleet-2'
            }, {
                'InstanceType': 'm4.large',
                'InstanceFleetId': 'fleet-1'
            }]
        }, {
            'Instances': [{
                'InstanceType': 'm4.large',
                'InstanceFleetId': 'fleet-1'
            }, {
                'InstanceType': 'm5.xlarge',
                'InstanceFleetId': 'fleet-2'
            }]
        }]

        counts_by_fleet, counts = self.emr_monitor._get_instance_counts(mock_emr_client)

        # Expected output
        expected_counts_by_fleet = defaultdict(
            lambda: defaultdict(int), {
                'fleet-1': {
                    'm5.xlarge': 1,
                    'm4.large': 2
                },
                'fleet-2': {
                    'm5.xlarge': 2
                }
            }
        )
        expected_counts = defaultdict(int, {'m5.xlarge': 3, 'm4.large': 2})

        self.assertEqual(counts_by_fleet, expected_counts_by_fleet)
        self.assertEqual(counts, expected_counts)

    list_instances_response = {
        'InstanceFleets': [{
            'Id':
            'fleet-1',
            'InstanceTypeSpecifications': [{
                'InstanceType': 'm5.xlarge',
                'EbsBlockDevices': [{
                    'VolumeSpecification': {
                        'SizeInGB': 100
                    }
                }],
            }]
        }, {
            'Id':
            'fleet-2',
            'InstanceTypeSpecifications': [
                {
                    'InstanceType': 'm4.large',
                    'EbsBlockDevices': [{
                        'VolumeSpecification': {
                            'SizeInGB': 50
                        }
                    }]
                },
                {
                    'InstanceType': 'c4.large',
                    'EbsBlockDevices': [
                        {
                            'VolumeSpecification': {
                                'SizeInGB': 20
                            }
                        },
                        {
                            'VolumeSpecification': {
                                'SizeInGB': 20
                            }
                        },
                    ]
                },
                {
                    'InstanceType': 'm5.xlarge'
                },
            ]
        }]
    }

    @patch('ttd.eldorado.aws.emr_monitor_cluster_startup.EmrMonitorClusterStartup._call_api_with_retry')
    def test__get_ebs_storage__basic_case(self, mock_call_api_with_retry):
        # Arrange
        instance_counts_by_fleet = defaultdict(
            lambda: defaultdict(int), {
                'fleet-1': {
                    'm5.xlarge': 2
                },
                'fleet-2': {
                    'm4.large': 1,
                    'c4.large': 3
                }
            }
        )

        # Assert
        mock_call_api_with_retry.return_value = self.list_instances_response
        result = self.emr_monitor._get_ebs_storage(instance_counts_by_fleet, Mock())

        # Expected output:
        # fleet-1: 2 instances * 100 GB = 200 GB
        # fleet-2: 1 instance * 50 GB + 3 instances * 20 GB * 2= 170 GB
        self.assertEqual(result, 370)

    @patch('ttd.eldorado.aws.emr_monitor_cluster_startup.EmrMonitorClusterStartup._call_api_with_retry')
    def test__get_ebs_storage__missing_instance_type(self, mock_call_api_with_retry):
        # Arrange
        instance_counts_by_fleet = defaultdict(lambda: defaultdict(int), {
            'fleet-1': {
                'm5.xlarge': 2
            },
            'fleet-2': {
                'm9.large': 1,
            }
        })

        # Assert
        mock_call_api_with_retry.return_value = self.list_instances_response
        result = self.emr_monitor._get_ebs_storage(instance_counts_by_fleet, Mock())

        # Expected output:
        # fleet-1: 2 instances * 100 GB = 200 GB
        # fleet-2: No data so 0 GB
        self.assertEqual(result, 200)

    @patch('ttd.eldorado.aws.emr_monitor_cluster_startup.EmrMonitorClusterStartup._call_api_with_retry')
    def test__get_ebs_storage__instance_type_with_no_attached_storage(self, mock_call_api_with_retry):
        # Arrange
        instance_counts_by_fleet = defaultdict(lambda: defaultdict(int), {'fleet-1': {'m5.xlarge': 2}, 'fleet-2': {'m5.xlarge': 1}})

        # Assert
        mock_call_api_with_retry.return_value = self.list_instances_response
        result = self.emr_monitor._get_ebs_storage(instance_counts_by_fleet, Mock())

        # Expected output:
        # fleet-1: 2 instances * 100 GB = 200 GB
        # fleet-2: 1 instance * 0 GB = 0 GB
        self.assertEqual(result, 200)

    @patch('ttd.eldorado.aws.emr_monitor_cluster_startup.EmrMonitorClusterStartup._call_api_with_retry')
    def test__get_ebs_storage__empty_result_set_returns_none(self, mock_call_api_with_retry):
        # Arrange
        instance_counts_by_fleet = defaultdict(lambda: defaultdict(int), {'fleet-1': {'m5.xlarge': 2}})

        mock_call_api_with_retry.return_value = {}

        # Act
        result = self.emr_monitor._get_ebs_storage(instance_counts_by_fleet, Mock())

        # Asser
        self.assertIsNone(result)

    @patch('ttd.eldorado.aws.emr_monitor_cluster_startup.EmrMonitorClusterStartup._call_api_with_retry')
    def test__get_cores_and_memory__basic_case(self, mock_call_api_with_retry):
        # Arrange
        instance_counts_by_fleet = defaultdict(lambda: defaultdict(int), {'fleet-1': {'m5.xlarge': 2, 'c4.large': 3}})
        mock_call_api_with_retry.return_value = {
            'InstanceTypes': [
                {
                    'InstanceType': 'm5.xlarge',
                    'MemoryInfo': {
                        'SizeInMiB': 16384  # 16 * 1024
                    },
                    'VCpuInfo': {
                        'DefaultVCpus': 4
                    }
                },
                {
                    'InstanceType': 'c4.large',
                    'MemoryInfo': {
                        'SizeInMiB': 4096  # 4 * 1024
                    },
                    'VCpuInfo': {
                        'DefaultVCpus': 2
                    }
                }
            ]
        }

        # Act
        result = self.emr_monitor._get_cores_and_memory(instance_counts_by_fleet, Mock())

        # Assert:
        # Expected output: (2 * 4) + (3 * 2) = 14 cores, 2 * 16 GB + 3 * 4 GB = 44 GB
        self.assertEqual(result, (14, 44))

    @patch('ttd.eldorado.aws.emr_monitor_cluster_startup.EmrMonitorClusterStartup._call_api_with_retry')
    def test__get_cores_and_memory__multiple_fleets(self, mock_call_api_with_retry):
        # Arrange
        instance_counts_by_fleet = defaultdict(lambda: defaultdict(int), {'fleet-1': {'m5.xlarge': 2}, 'fleet-2': {'m5.xlarge': 3}})
        mock_call_api_with_retry.return_value = {
            'InstanceTypes': [{
                'InstanceType': 'm5.xlarge',
                'MemoryInfo': {
                    'SizeInMiB': 16384  # 16 * 1024
                },
                'VCpuInfo': {
                    'DefaultVCpus': 4
                }
            }]
        }

        # Act
        result = self.emr_monitor._get_cores_and_memory(instance_counts_by_fleet, Mock())

        # Assert:
        # Expected output: 5 * 4 = 20 cores, 5 * 16 GB = 80 GB
        self.assertEqual(result, (20, 80))

    @patch('ttd.eldorado.aws.emr_monitor_cluster_startup.EmrMonitorClusterStartup._call_api_with_retry')
    def test__get_cores_and_memory__empty_result_set_returns_none(self, mock_call_api_with_retry):
        # Arrange
        instance_counts_by_fleet = defaultdict(lambda: defaultdict(int), {'fleet-1': {'m5.xlarge': 1}})
        mock_call_api_with_retry.return_value = {}

        # Act
        result = self.emr_monitor._get_cores_and_memory(instance_counts_by_fleet, Mock())

        # Assert
        self.assertEqual(result, (None, None))


if __name__ == '__main__':
    unittest.main()
