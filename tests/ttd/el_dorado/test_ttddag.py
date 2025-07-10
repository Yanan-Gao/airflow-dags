import unittest

from ttd.eldorado.base import TtdDag


class TestTtdDag(unittest.TestCase):

    def test__map_teams_to_access_control__maps_correctly(self):
        result = TtdDag.map_teams_to_access_control(['AIFUN'])
        self.assertEqual(result, {'SCRUM-AIFUN-EDIT': {'can_read', 'can_edit'}})


if __name__ == '__main__':
    unittest.main()
