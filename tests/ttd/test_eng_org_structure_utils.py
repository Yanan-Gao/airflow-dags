import unittest
from unittest.mock import patch
from ttd.eng_org_structure_utils import (
    cache,
    is_valid_team,
    is_valid_user,
    get_slack_user_id,
    is_user_in_team,
    get_alarm_channel_id,
)
import json
from ttd.monads.maybe import Just, Nothing
from requests import Response

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
        ],
        "team": {
            "slack": {
                "channels": {
                    "alerts_id": "team2-alerts-channel"
                }
            }
        }
    },
    "TEAM3": {
        "members": [
            {
                "acronym": "mvn",
                "alias": "maria.nosareva",
                "name": "Maria Nosareva",
            },
        ],
        "team": {
            "slack": {
                "channels": {
                    "alerts": "not-the-right-channel"
                }
            }
        }
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
    ("absent_user_returns_nothing", "chris.ng", Nothing()),
]

is_valid_user_tests = [
    ("alias_matched_successfully_returns_true", "alfred.bullus", True),
    ("gitlab_username_matched_successfully_returns_true", "konstantin.mitckevich", True),
    ("absent_slack_user_id_still_returns_true", "dias.zhassanbay", True),
    ("absent_user_returns_false", "chris.ng", False),
]

user_in_team_params = [
    ("user_alias_in_team", "alfred.bullus", "TEAM1", teams_eng_org_structure, True),
    ("user_gitlab_in_team", "konstantin.mitckevich", "TEAM2", teams_eng_org_structure, True),
    ("user_alias_not_in_team", "chris.ng", "TEAM1", teams_eng_org_structure, False),
    ("user_alias_in_other_team", "dias.zhassanbay", "TEAM1", teams_eng_org_structure, False),
    ("team_doesnt_exist", "dias.zhassanbay", "TEAM5", teams_eng_org_structure, False),
    ("empty_response", "dias.zhassanbay", "TEAM1", {}, False),
]

team_slack_channel_params = [
    ("slack_channel_base_case", "TEAM2", teams_eng_org_structure, "team2-alerts-channel"),
    ("slack_channel_team_missing", "TEAM1", teams_eng_org_structure, None),
    ("slack_channel_channel_missing", "TEAM3", teams_eng_org_structure, None),
    ("slack_channel_empty_response", "TEAM2", {}, None),
]


class TestEngOrgStructureUtils(unittest.TestCase):

    @staticmethod
    def _create_mocked_response(body: dict, status: int):
        mocked_response = Response()
        mocked_response.status_code = status
        mocked_response._content = json.dumps(body).encode('utf-8')
        return mocked_response

    def setUp(self):
        cache.clear()

    @patch("ttd.eng_org_structure_utils.Session")
    def test_is_valid_team(self, mocked_sesh):
        with self.subTest():
            for name, scrum_teams, is_valid in scrum_team_params:
                mocked_sesh.return_value.get.return_value = self._create_mocked_response(scrum_teams, status=200)
                self.assertEqual(is_valid_team(name), is_valid)
                cache.clear()

    @patch("ttd.eng_org_structure_utils.Session")
    def test_is_valid_user(self, mocked_sesh):
        for test_name, name, expected_validity in is_valid_user_tests:
            with self.subTest(test_name=test_name):
                mocked_sesh.return_value.get.return_value = self._create_mocked_response(teams_eng_org_structure, status=200)
                is_valid = is_valid_user(name)
                self.assertEqual(is_valid, expected_validity)
                cache.clear()

    @patch("ttd.eng_org_structure_utils.Session")
    def test_is_valid_user_returns_true_when_api_fails(self, mocked_sesh):
        mocked_sesh.return_value.get.side_effect = Exception("API request failed")
        result = is_valid_user("alfred.bullus")
        self.assertEqual(result, True)
        cache.clear()

    @patch("ttd.eng_org_structure_utils.Session")
    def test_is_user_in_team(self, mocked_sesh):
        for test_name, name, team, response, expected_validity in user_in_team_params:
            with self.subTest(test_name=test_name):
                mocked_sesh.return_value.get.return_value = self._create_mocked_response(response, status=200)
                is_in_team = is_user_in_team(name, team)
                self.assertEqual(is_in_team, expected_validity)
                cache.clear()

    @patch("ttd.eng_org_structure_utils.Session")
    def test_get_alarm_channel_id(self, mocked_sesh):
        for test_name, team, response, expected_channel in team_slack_channel_params:
            with self.subTest(test_name=test_name):
                mocked_sesh.return_value.get.return_value = self._create_mocked_response(response, status=200)
                channel = get_alarm_channel_id(team)
                self.assertEqual(channel, expected_channel)
                cache.clear()

    @patch("ttd.eng_org_structure_utils.Session")
    def test_get_slack_user_id(self, mocked_sesh):
        for test_name, name, expected_user_id in slack_user_tests:
            with self.subTest(test_name=test_name):
                mocked_sesh.return_value.get.return_value = self._create_mocked_response(teams_eng_org_structure, status=200)
                slack_user_id = get_slack_user_id(name)
                self.assertEqual(slack_user_id, expected_user_id)
                cache.clear()

    @patch("ttd.eng_org_structure_utils.Session")
    def test_get_slack_user_id_returns_nothing_when_api_fails(self, mocked_sesh):
        # Arrange
        mocked_sesh.return_value.get.side_effect = Exception("API request failed")
        result = get_slack_user_id("alfred.bullus")
        self.assertEqual(result, Nothing())
        cache.clear()


if __name__ == '__main__':
    unittest.main()
