import unittest
from unittest import mock
from unittest.mock import MagicMock, patch

from dags.dataproc.monitoring.cluster_monitoring import ClusterDetails, cluster_groupby, add_duplicate_clusters, \
    DuplicateClusterDetected, MissingTags, DurationExceeded, InvalidTeam, UserNotInTeam, TtdSlackClient, InvalidCreator, \
    CreatorDetails
from ttd.cloud_provider import AwsCloudProvider, AliCloudProvider
from datetime import datetime, timedelta
from dateutil.tz import tzlocal

from ttd.monads.maybe import Just, Nothing

test_identifier_tags = [({
    "ClusterName": "job-jobname-run-2024103108try1-taskid",
    "Environment": "Prod"
}, "job-jobname-run-2024103108-taskid-prod"),
                        ({
                            "ClusterName": "job-jobname-run-2024103108try1213-task1",
                            "Environment": "Prod"
                        }, "job-jobname-run-2024103108-task1-prod"),
                        ({
                            "ClusterName": "job-jobname-run-2024103108-task2",
                            "Environment": "Prod"
                        }, "job-jobname-run-2024103108-task2-prod"), ({
                            "Environment": "Prod"
                        }, "unidentified"), ({
                            "ClusterName": "job-jobname-run-2024103108try1-task3"
                        }, "unidentified"), ({}, "unidentified")]

mock_scrum_teams = {
    "team-1": {
        "members": [{
            "acronym": "TC1",
            "alias": "creator-1",
            "name": "Creator 1 Test",
            "slack_user_id": "CREATOR11111",
            "slack_user_name": "creator-1",
            "gitlab_username": "creator-1"
        }, {
            "acronym": "TC2",
            "alias": "creator-2",
            "name": "Creator 2 Test",
            "slack_user_id": "CREATOR2222",
            "slack_user_name": "creator-2",
            "gitlab_username": "creator-2"
        }]
    },
    "team-2": {
        "members": [{
            "acronym": "TC3",
            "alias": "creator-3",
            "name": "Creator 3 Test",
            "slack_user_id": "CREATOR3333",
            "slack_user_name": "creator-3",
            "gitlab_username": "creator-3"
        }, {
            "acronym": "TC4",
            "alias": "creator-4",
            "name": "Creator 4 Test",
            "slack_user_id": "CREATOR4444",
            "slack_user_name": "creator-4",
            "gitlab_username": "creator-4"
        }]
    }
}


class ClusterMonitoringTest(unittest.TestCase):

    def setUp(self):
        self.patcher = mock.patch('ttd.eng_org_structure_utils.get_scrum_teams', return_value=mock_scrum_teams)
        self.patcher.start()
        self.test_clusters = [
            ClusterDetails(
                cluster_id="cl-1",
                name="cluster-1",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AwsCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "Prod",
                    "ClusterName": "job-jobname-run-2024103108try1-task1",
                    "Job": "job-1",
                    "Resource": "resource-1",
                    "Source": "source-1",
                    "Team": "team-1",
                    "Creator": "creator-1",
                }
            ),
            ClusterDetails(
                cluster_id="cl-duplicate-name-1",
                name="cluster-2",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AwsCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "prodTest",
                    "ClusterName": "some-identical-cluster-name",
                    "Job": "job-2",
                    "Resource": "resource-2",
                    "Source": "source-2",
                    "Team": "team-1",
                    "Creator": "creator-1",
                }
            ),
            ClusterDetails(
                cluster_id="cl-duplicate-name-2",
                name="cluster-3",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AliCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "prodTest",
                    "ClusterName": "some-identical-cluster-name",
                    "Job": "job-3",
                    "Resource": "resource-1",
                    "Source": "source-2",
                    "Team": "team-1",
                    "Creator": "creator-1",
                }
            ),
            ClusterDetails(
                cluster_id="cl-duplicate-cluster-name-different-creator",
                name="cluster-4",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AliCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "prodTest",
                    "ClusterName": "some-identical-cluster-name",
                    "Job": "job-3",
                    "Resource": "resource-1",
                    "Source": "source-2",
                    "Team": "team-1",
                    "Creator": "creator-2",
                }
            ),
            ClusterDetails(
                cluster_id="cl-duplicate-cluster-name-different-env",
                name="cluster-5",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AliCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "Prod",
                    "ClusterName": "some-identical-cluster-name",
                    "Job": "job-3",
                    "Resource": "resource-1",
                    "Source": "source-2",
                    "Team": "team-1",
                }
            ),
            ClusterDetails(
                cluster_id="cl-no-tags",
                name="cluster-6",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AliCloudProvider(),
                region_name="us-east-1",
                tags=None
            ),
            ClusterDetails(
                cluster_id="cl-empty-tags",
                name="cluster-7",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AliCloudProvider(),
                region_name="us-east-1",
                tags={}
            ),
            ClusterDetails(
                cluster_id="cl-some-tags-missing",
                name="cluster-8",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AliCloudProvider(),
                region_name="us-east-1",
                tags={"Source": ""}
            ),
            ClusterDetails(
                cluster_id="cl-adhoc-source-no-creator",
                name="cluster-13",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AwsCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "Prod",
                    "ClusterName": "cl-adhoc-source-no-creator",
                    "Job": "job-2",
                    "Resource": "resource-2",
                    "Source": "adhoc",
                    "Team": "team-2",
                }
            ),
            ClusterDetails(
                cluster_id="cl-invalid-team",
                name="cluster-15",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AwsCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "Prod",
                    "ClusterName": "cl-invalid-team",
                    "Job": "cl-invalid-team",
                    "Resource": "resource-2",
                    "Source": "source-2",
                    "Team": "team-that-doesn't-exist",
                }
            ),
            ClusterDetails(
                cluster_id="cl-running-too-long",
                name="cluster-16",
                creation_date_time=datetime.now(tz=tzlocal()) - timedelta(days=1),
                cloud_provider=AwsCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "Prod",
                    "ClusterName": "cl-running-too-long",
                    "Job": "job-2",
                    "Resource": "resource-2",
                    "Source": "source-2",
                    "Team": "team-2",
                }
            ),
            ClusterDetails(
                cluster_id="cl-long-lived-cluster",
                name="cluster-16",
                creation_date_time=datetime.now(tz=tzlocal()) - timedelta(days=2),
                cloud_provider=AwsCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "Prod",
                    "ClusterName": "cl-long-lived-cluster",
                    "Job": "job-2",
                    "Resource": "resource-2",
                    "Duration": "very_long",
                    "Source": "source-2",
                    "Team": "team-2",
                }
            ),
            ClusterDetails(
                cluster_id="cl-permanent-cluster",
                name="cluster-16",
                creation_date_time=datetime.now(tz=tzlocal()) - timedelta(days=10),
                cloud_provider=AwsCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "Prod",
                    "ClusterName": "cl-permanent-cluster",
                    "Job": "job-2",
                    "Resource": "resource-2",
                    "Duration": "permanent",
                    "Source": "source-2",
                    "Team": "team-2",
                }
            ),
            ClusterDetails(
                cluster_id="cl-user-not-in-team",
                name="cluster-17",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AwsCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "Prod",
                    "ClusterName": "cl-user-not-in-team",
                    "Job": "job-2",
                    "Resource": "resource-2",
                    "Source": "source-2",
                    "Team": "team-2",
                    "Creator": "creator-1",
                }
            ),
            ClusterDetails(
                cluster_id="cl-invalid-creator",
                name="cluster-18",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AwsCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "prodTest",
                    "ClusterName": "cl-invalid-creator",
                    "Job": "job-2",
                    "Resource": "resource-2",
                    "Source": "source",
                    "Team": "team-2",
                    "Creator": "made-up-creator",
                }
            ),
            ClusterDetails(
                cluster_id="cl-valid-creator",
                name="cluster-19",
                creation_date_time=datetime.now(tz=tzlocal()),
                cloud_provider=AwsCloudProvider(),
                region_name="us-east-1",
                tags={
                    "Environment": "prodTest",
                    "ClusterName": "cl-valid-creator",
                    "Job": "job-2",
                    "Resource": "resource-2",
                    "Source": "source",
                    "Team": "team-2",
                    "Creator": "creator-3",
                }
            ),
        ]

        add_duplicate_clusters(self.test_clusters)

    def tearDown(self):
        self.patcher.stop()

    def test_identifier_tags(self):
        for tags, expected_identifier in test_identifier_tags:
            self.assertEqual(
                ClusterDetails(
                    cluster_id="test",
                    name="test",
                    creation_date_time=datetime.now(tz=tzlocal()),
                    cloud_provider=AwsCloudProvider(),
                    region_name="testregion",
                    tags=tags
                ).duplicate_identifier, expected_identifier
            )

    def test_cluster_groupby_identifier(self):
        grouped = cluster_groupby(self.test_clusters, lambda cluster: cluster.duplicate_identifier)
        grouped_ids = {t: [c.cluster_id for c in clusters] for t, clusters in grouped.items()}
        expected = {
            'cl-adhoc-source-no-creator-prod': ['cl-adhoc-source-no-creator'],
            'cl-invalid-team-prod': ['cl-invalid-team'],
            'cl-running-too-long-prod': ['cl-running-too-long'],
            'cl-long-lived-cluster-prod': ['cl-long-lived-cluster'],
            'cl-permanent-cluster-prod': ['cl-permanent-cluster'],
            'cl-user-not-in-team-prodcreator-1': ['cl-user-not-in-team'],
            'cl-invalid-creator-prodtestmade-up-creator': ['cl-invalid-creator'],
            'cl-valid-creator-prodtestcreator-3': ['cl-valid-creator'],
            'job-jobname-run-2024103108-task1-prodcreator-1': ['cl-1'],
            'some-identical-cluster-name-prodtestcreator-1': ['cl-duplicate-name-1', 'cl-duplicate-name-2'],
            'some-identical-cluster-name-prodtestcreator-2': ['cl-duplicate-cluster-name-different-creator'],
            'some-identical-cluster-name-prod': ['cl-duplicate-cluster-name-different-env'],
            'unidentified': ['cl-no-tags', 'cl-empty-tags', 'cl-some-tags-missing']
        }
        self.maxDiff = None
        self.assertDictEqual(grouped_ids, expected)

    def test_missing_tags(self):
        missing_tags = lambda cluster: any(isinstance(reason, MissingTags) for reason in cluster.deletion_reasons)

        self.assertListEqual([c.cluster_id for c in self.test_clusters if missing_tags(c)],
                             ['cl-no-tags', 'cl-empty-tags', 'cl-some-tags-missing', 'cl-adhoc-source-no-creator'])

    def test_duration_exceeded(self):
        duration_exceeded = lambda cluster: any(isinstance(reason, DurationExceeded) for reason in cluster.deletion_reasons)

        self.assertListEqual([c.cluster_id for c in self.test_clusters if duration_exceeded(c)], ['cl-running-too-long'])

    def test_duplicate_cluster(self):
        has_duplicates = lambda cluster: any(isinstance(reason, DuplicateClusterDetected) for reason in cluster.deletion_reasons)

        self.assertListEqual([c.cluster_id
                              for c in self.test_clusters if has_duplicates(c)], ['cl-duplicate-name-1', 'cl-duplicate-name-2'])

    def test_invalid_team(self):
        invalid_team = lambda cluster: any(isinstance(reason, InvalidTeam) for reason in cluster.deletion_reasons)

        self.assertListEqual([c.cluster_id for c in self.test_clusters if invalid_team(c)], ['cl-invalid-team'])

    def test_user_not_in_team(self):
        user_not_in_team = lambda cluster: any(isinstance(reason, UserNotInTeam) for reason in cluster.warning_reasons)

        self.assertListEqual([c.cluster_id
                              for c in self.test_clusters if user_not_in_team(c)], ['cl-user-not-in-team', 'cl-invalid-creator'])

    def test_invalid_creator(self):
        invalid_creator = lambda cluster: any(isinstance(reason, InvalidCreator) for reason in cluster.deletion_reasons)

        self.assertListEqual([c.cluster_id for c in self.test_clusters if invalid_creator(c)], ['cl-invalid-creator'])


class TestGetMaxPermittedDuration(unittest.TestCase):

    def test_no_matching_tags_returns_default(self):
        tags = {"random_tag": "Tag!", "another_tag": "Tog!"}
        self.assertEqual(DurationExceeded.get_max_permitted_duration(tags), DurationExceeded.default_max_duration)

    def test_empty_tags_returns_default(self):
        tags: dict[str, str] = {}
        self.assertEqual(DurationExceeded.get_max_permitted_duration(tags), DurationExceeded.default_max_duration)

    def test_duration_tag_returns_correct_limit(self):
        tags = {"Duration": "long", 'another_tag': 'Teg!'}
        self.assertEqual(DurationExceeded.get_max_permitted_duration(tags), DurationExceeded.duration_values["long"])

    def test_invalid_duration_tag_returns_default(self):
        tags = {"Duration": "SuperSuperLong", 'another_tag': 'Teg!'}
        self.assertEqual(DurationExceeded.get_max_permitted_duration(tags), DurationExceeded.default_max_duration)

    def test_long_running_tag_returns_higher_limit(self):
        tags = {"long_running": "true", 'another_tag': 'Teg!'}
        self.assertEqual(DurationExceeded.get_max_permitted_duration(tags), DurationExceeded.duration_values["long"])

    def test_multiple_matching_tags_returns_most_permissive_value(self):
        tags = {"long_running": "true", "very_long_running": "true"}
        self.assertEqual(DurationExceeded.get_max_permitted_duration(tags), DurationExceeded.duration_values["very_long"])


class TestFindLatestThreadTS(unittest.TestCase):

    def setUp(self):
        self.mock_client = MagicMock()
        self.instance = TtdSlackClient('token')
        self.instance._client = self.mock_client

    def test_single_message_can_be_successfully_matched(self):
        self.mock_client.conversations_history.return_value = {
            "messages": [{
                "ts": "1740070808.857289",
                "attachments": [{
                    "footer": "Message ID: the_id"
                }]
            }]
        }
        result = self.instance.find_latest_thread_ts(timedelta(days=1), "channel_id", "the_id")
        self.assertEqual(result, "1740070808.857289")

    def test_ts_returned_corresponds_to_most_recent_match(self):
        self.mock_client.conversations_history.return_value = {
            "messages": [
                {
                    "ts": "10000",
                    "attachments": [{
                        "footer": "Message ID: abababab"
                    }]
                },
                {
                    "ts": "20000",
                    "attachments": [{
                        "footer": "Message ID: abababab"
                    }]
                },
                {
                    "ts": "30000",
                    "attachments": [{
                        "footer": "Message ID: cdcdcdcd"
                    }]
                },
            ]
        }
        result = self.instance.find_latest_thread_ts(timedelta(days=1), "channel_id", "abababab")
        self.assertEqual(result, "20000")

    def test_none_returned_if_no_matches(self):
        self.mock_client.conversations_history.return_value = {
            "messages": [
                {
                    "ts": "1740070600.123456",
                    "attachments": [{
                        "footer": "Message ID: something"
                    }]
                },
                {
                    "ts": "1740070808.857289",
                    "attachments": [{
                        "footer": "Message ID: unrelated"
                    }]
                },
            ]
        }
        result = self.instance.find_latest_thread_ts(timedelta(days=1), "channel_id", "message_id")
        self.assertIsNone(result)

    def test_none_returned_if_no_messages(self):
        self.mock_client.conversations_history.return_value = {"messages": []}
        result = self.instance.find_latest_thread_ts(timedelta(days=1), "channel_id", "message_id")
        self.assertIsNone(result)

    def test_correct_value_returned_even_if_other_messages_missing_elements(self):
        self.mock_client.conversations_history.return_value = {
            "messages": [
                {
                    "ts": "1000"
                },
                {
                    "ts": "2000",
                    "attachments": [{
                        'different-key': 'val'
                    }]
                },
                {
                    "ts": "3000",
                    "attachments": [{
                        "footer": "Message ID: message_id"
                    }]
                },
            ]
        }
        result = self.instance.find_latest_thread_ts(timedelta(days=1), "channel_id", "message_id")
        self.assertEqual(result, "3000")


class TestCreatorDetails(unittest.TestCase):

    @patch('dags.dataproc.monitoring.cluster_monitoring.get_slack_user_id', return_value=Just('UALFRED'))
    def test_correct_formatting_applied_to_slack_user_id(self, mock_get_slack_user_id):
        tags = {"Creator": "alfred.bullus"}

        creator_details = CreatorDetails(tags)
        formatted_creator = creator_details.format_creator_for_slack()

        self.assertEqual(formatted_creator, Just("<@UALFRED>"))
        mock_get_slack_user_id.assert_called_once_with("alfred.bullus")

    @patch('dags.dataproc.monitoring.cluster_monitoring.get_slack_user_id', return_value=Nothing())
    def test_correct_formatting_applied_to_creator_tag_if_user_id_not_found(self, mock_get_slack_user_id):
        tags = {"Creator": "alfred.bullus"}

        creator_details = CreatorDetails(tags)
        formatted_creator = creator_details.format_creator_for_slack()

        self.assertEqual(formatted_creator, Just("`alfred.bullus`"))
        mock_get_slack_user_id.assert_called_once_with("alfred.bullus")

    def test_slack_user_id_and_creator_tag_are_nothing_when_no_creator_tag_present(self):
        tags = {"ADifferentTag": "SomethingElse"}

        creator_details = CreatorDetails(tags)

        self.assertEqual(creator_details.creator_tag, Nothing())
        self.assertEqual(creator_details.slack_user_id, Nothing())

    @patch('dags.dataproc.monitoring.cluster_monitoring.get_slack_user_id', return_value=Just("UALFRED"))
    def test_get_slack_user_id_not_called_when_creator_tag_not_present(self, mock_get_slack_user_id):
        tags = {"Environment": "prodTest", "Source": "airflow"}

        CreatorDetails(tags)

        mock_get_slack_user_id.assert_not_called()

    def test_return_nothing_for_slack_if_no_creator_tag_present(self):
        tags = {"ADifferentTag": "SomethingElse"}

        creator_details = CreatorDetails(tags)

        self.assertEqual(creator_details.format_creator_for_slack(), Nothing())

    def test_require_creator_tag_if_source_is_adhoc(self):
        tags = {"Source": "adhoc"}

        creator_details = CreatorDetails(tags)

        self.assertTrue(creator_details.require_creator_tag)

    def test_require_creator_tag_if_environment_is_prodTest(self):
        tags = {"Environment": "prodTest", "Source": "airflow"}

        creator_details = CreatorDetails(tags)

        self.assertTrue(creator_details.require_creator_tag)

    def test_require_creator_tag_if_source_is_missing(self):
        tags = {"Environment": "prod"}

        creator_details = CreatorDetails(tags)

        self.assertTrue(creator_details.require_creator_tag)

    def test_creator_tag_not_required_if_environment_is_prod_and_source_set(self):
        tags = {"Environment": "prod", "Source": "airflow"}

        creator_details = CreatorDetails(tags)

        self.assertFalse(creator_details.require_creator_tag)


if __name__ == "__main__":
    unittest.main()
