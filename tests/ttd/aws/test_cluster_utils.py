from ttd.aws.emr.cluster_utils import extract_cluster_log_metadata, ClusterLogMetadata, Container, LogFile


def test_extract_cluster_log_metadata():
    cluster_log_metadata = extract_cluster_log_metadata(
        bucket="bucket",
        file_key_dicts=[{
            "Key":
            "emr-logs/j-1NAOJBVBZSV3B/containers/application_1745297292664_0001/container_1745297292664_0001_01_000001/stdout.gz"
        }, {
            "Key":
            "emr-logs/j-1NAOJBVBZSV3B/containers/application_1745297292664_0002/container_1745297292664_0001_01_000001/stderr.gz"
        }, {
            "Key":
            "emr-logs/j-1NAOJBVBZSV3Z/containers/application_1745297292664_0001/container_1745297292664_0001_01_000001/stderr.gz"
        }]
    )

    expected_cluster_metadata = ClusterLogMetadata(
        bucket="bucket",
        app_container_logs={
            Container(name="application_1745297292664_0001"): {
                LogFile(
                    file_name="stdout.gz",
                    file_path=
                    "emr-logs/j-1NAOJBVBZSV3B/containers/application_1745297292664_0001/container_1745297292664_0001_01_000001/stdout.gz"
                ),
                LogFile(
                    file_name="stderr.gz",
                    file_path=
                    "emr-logs/j-1NAOJBVBZSV3B/containers/application_1745297292664_0002/container_1745297292664_0001_01_000001/stderr.gz"
                )
            },
            Container(name="application_1745297292664_0002"): {
                LogFile(
                    file_name="stderr.gz",
                    file_path=
                    "emr-logs/j-1NAOJBVBZSV3Z/containers/application_1745297292664_0001/container_1745297292664_0001_01_000001/stderr.gz"
                )
            }
        }
    )
    assert cluster_log_metadata == expected_cluster_metadata
