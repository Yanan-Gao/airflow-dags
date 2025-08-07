import unittest
from unittest import mock
from typing import Dict
from os import environ

from ttd.ttdenv import ProdTestEnv, ProdEnv, TtdEnvFactory
from ttd.eldorado.tag_utils import (replace_tag_invalid_chars, add_task_tag, add_creator_tag, ClusterTagError)

tag_name_params = [
    ("test-tag@name1", "test-tag@name1"),
    ("test_tag/name.2", "test_tag/name.2"),
    ("test_tag, name - #3", "test_tag_ name - _3"),
    ("", ""),
]

creator_tag_basic_cases = [
    # Basic cases -> must add
    ({
        "key1": "val1",
        "key2": "val2"
    }, ProdTestEnv, {
        "AIRFLOW_VAR_USER_LOGIN": "test_creator"
    }, {
        "key1": "val1",
        "key2": "val2",
        "Creator": "test_creator"
    }),
    (None, ProdTestEnv, {
        "AIRFLOW_VAR_USER_LOGIN": "test_creator"
    }, {
        "Creator": "test_creator"
    }),
    ({}, ProdTestEnv, {
        "AIRFLOW_VAR_USER_LOGIN": "test_creator"
    }, {
        "Creator": "test_creator"
    }),
    # User provides as well -> must override
    ({
        "key1": "val1",
        "key2": "val2",
        "Creator": "creator_user_provided"
    }, ProdTestEnv, {
        "AIRFLOW_VAR_USER_LOGIN": "creator_from_env"
    }, {
        "key1": "val1",
        "key2": "val2",
        "Creator": "creator_from_env"
    }),
    # Not prod-test -> must return input unchanged
    ({
        "key1": "val1",
        "key2": "val2"
    }, ProdEnv, {
        "AIRFLOW_VAR_USER_LOGIN": "test_creator"
    }, {
        "key1": "val1",
        "key2": "val2"
    }),
    ({
        "key1": "val1",
        "key2": "val2"
    }, ProdEnv, {}, {
        "key1": "val1",
        "key2": "val2"
    }),
    (None, ProdEnv, {}, None),
    ({}, ProdEnv, {}, {}),
]

cluster_tags_params = [
    (
        "task_name1",
        [{
            "Key": "Job",
            "Value": "dag_id"
        }],
        [
            {
                "Key": "Job",
                "Value": "dag_id"
            },
            {
                "Key": "Tasks",
                "Value": "1"
            },
            {
                "Key": "Task1",
                "Value": "task_name1"
            },
        ],
    ),
    (
        "task_name2",
        [
            {
                "Key": "Job",
                "Value": "dag_id"
            },
            {
                "Key": "Tasks",
                "Value": "1"
            },
            {
                "Key": "Task1",
                "Value": "task_name1"
            },
        ],
        [
            {
                "Key": "Job",
                "Value": "dag_id"
            },
            {
                "Key": "Tasks",
                "Value": "2"
            },
            {
                "Key": "Task1",
                "Value": "task_name1"
            },
            {
                "Key": "Task2",
                "Value": "task_name2"
            },
        ],
    ),
]


class TagUtilsTest(unittest.TestCase):

    def test_replace_tag_invalid_chars(self):
        with self.subTest():
            for name, expected_name in tag_name_params:
                processed_name = replace_tag_invalid_chars(name)
                self.assertEqual(expected_name, processed_name)

    def test_add_creator_tag_basic(self):
        with self.subTest():
            for tags_in, env, env_vars, tags_exp in creator_tag_basic_cases:
                with mock.patch.object(TtdEnvFactory, 'get_from_system', env), mock.patch.dict(environ, env_vars):
                    self.assertEqual(add_creator_tag(tags_in), tags_exp)

    def test_add_creator_tag_error(self):
        # Test whether it throws an error if the expected env var is missing in ProdTest
        tags_in: Dict[str, str] = {}
        env = ProdTestEnv
        env_vars: Dict[str, str] = {}
        with self.subTest():
            with mock.patch.object(TtdEnvFactory, 'get_from_system', env), mock.patch.dict(environ, env_vars):
                with self.assertRaises(ClusterTagError):
                    add_creator_tag(tags_in)

    def test_add_task_tag(self):
        with self.subTest():
            for task_name, cluster_tags, expected_tags in cluster_tags_params:
                add_task_tag(cluster_tags, task_name)
                self.assertListEqual(cluster_tags, expected_tags)


if __name__ == "__main__":
    unittest.main()
