import unittest

from ttd.rbac.util import map_airflow_role


class TestRbacUtils(unittest.TestCase):

    def test_dataproc_get_admin_permissions(self):
        airflow_role = map_airflow_role('prod-airflow-dataproc-edit')
        self.assertEqual(airflow_role, 'Admin')

    def test_oct_get_super_access_permissions(self):
        airflow_role = map_airflow_role('prod-airflow-oct-edit')
        self.assertEqual(airflow_role, 'OCT-OperationalAccess')

    def test_oncall_users_get_oncall_permissions(self):
        airflow_role = map_airflow_role('OnCall')
        self.assertEqual(airflow_role, 'OnCall')

    def test_other_team_get_team_specific_permissions(self):
        airflow_role = map_airflow_role('prod-airflow-uid2-edit')
        self.assertEqual(airflow_role, 'SCRUM-UID2-EDIT')

    def test_irrelevant_role_returns_no_airflow_role(self):
        airflow_role = map_airflow_role('TaskServiceDeveloperElevated')
        self.assertEqual(airflow_role, '')
