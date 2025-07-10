import unittest
from unittest import mock
from unittest.mock import Mock
import requests

import ttd.eng_org_structure_utils
from ttd.eng_org_structure_utils import is_valid_team, is_valid_user, get_slack_user_id
from ttd.monads.maybe import Just, Nothing

teams_eng_org_structure = {
    "TEAM1": {
        "members": [
            {
                "acronym": "alb",
                "alias": "alfred.bullus",
                "name": "Alfred Bullus",
                "slack_user_id": "UALFRED"
            },
            {
                "acronym": "jhd",
                "alias": "joost.duisters",
                "gitlab_username": "joost.duisters",
                "name": "Joost Duisters",
                "slack_user_id": "UJOOST",
                "slack_username": "joost.duisters"
            },
        ]
    },
    "TEAM2": {
        "members": [
            {
                "acronym": "dzh",
                "alias": "dias.zhassanbay",
                "name": "Dias Zhassanbay",
            },
            {
                "acronym": "knm",
                "alias": "konstantin.mitckevic",
                "gitlab_username": "konstantin.mitckevich",
                "name": "Konstantin Mitckevich",
                "slack_user_id": "UKONSTANTIN",
                "slack_username": "konstantin.mitckevich"
            },
        ]
    },
}

scrum_team_params = [
    ("TEAM0", teams_eng_org_structure, False),
    ("TEAM1", teams_eng_org_structure, True),
    ("TEAM2", {}, False),
]

slack_user_tests = [
    ("alias_matched_to_user_id", "alfred.bullus", Just("UALFRED")),
    ("gitlab_username_matched_to_slack_id", "konstantin.mitckevich", Just("UKONSTANTIN")),
    ("absent_slack_user_id_returns_nothing", "dias.zhassanbay", Nothing()),
    ("absent_user_returns_nothing", "maria.nosareva", Nothing()),
]

is_valid_user_tests = [
    ("alias_matched_successfully_returns_true", "alfred.bullus", True),
    ("gitlab_username_matched_successfully_returns_true", "konstantin.mitckevich", True),
    ("absent_slack_user_id_still_returns_true", "dias.zhassanbay", True),
    ("absent_user_returns_false", "maria.nosareva", False),
]


class TestEngOrgStructureUtils(unittest.TestCase):

    def setUp(self):
        ttd.eng_org_structure_utils.cache.clear()

    @mock.patch.object(requests, "get")
    def test_is_valid_team(self, mock_get):
        with self.subTest():
            for name, scrum_teams, is_valid in scrum_team_params:
                mock_response = Mock()
                mock_response.json.return_value = scrum_teams
                mock_get.return_value = mock_response
                self.assertEqual(is_valid_team(name), is_valid)

                ttd.eng_org_structure_utils.cache.clear()

    @mock.patch.object(requests, "get")
    def test_is_valid_user(self, mock_get):
        for test_name, name, expected_validity in is_valid_user_tests:
            with self.subTest(test_name=test_name):
                # Arrange
                mock_response = Mock()
                mock_response.json.return_value = teams_eng_org_structure
                mock_get.return_value = mock_response

                # Act
                is_valid = is_valid_user(name)

                # Assert
                self.assertEqual(is_valid, expected_validity)

                ttd.eng_org_structure_utils.cache.clear()

    @mock.patch.object(requests, "get")
    def test_is_valid_user_returns_true_when_api_fails(self, mock_get):
        # Arrange
        mock_get.side_effect = Exception("API request failed")

        # Act
        result = is_valid_user("alfred.bullus")

        # Assert
        self.assertEqual(result, True)

        ttd.eng_org_structure_utils.cache.clear()

    @mock.patch.object(requests, "get")
    def test_get_slack_user_id(self, mock_get):
        for test_name, name, expected_user_id in slack_user_tests:
            with self.subTest(test_name=test_name):
                # Arrange
                mock_response = Mock()
                mock_response.json.return_value = teams_eng_org_structure
                mock_get.return_value = mock_response

                # Act
                slack_user_id = get_slack_user_id(name)

                # Assert
                self.assertEqual(slack_user_id, expected_user_id)

                ttd.eng_org_structure_utils.cache.clear()

    @mock.patch.object(requests, "get")
    def test_get_slack_user_id_returns_nothing_when_api_fails(self, mock_get):
        # Arrange
        mock_get.side_effect = Exception("API request failed")

        # Act
        result = get_slack_user_id("alfred.bullus")

        # Assert
        self.assertEqual(result, Nothing())

        ttd.eng_org_structure_utils.cache.clear()


if __name__ == '__main__':
    unittest.main()
