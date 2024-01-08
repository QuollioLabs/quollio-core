import os
import sys
import unittest

sys.path.append("quollio_core")

from core.setup import setup_dbt_profile
from repository.snowflake import SnowflakeConnectionConfig


class TestSetup(unittest.TestCase):
    def setUp(self):
        self.tests_dir = "{cur_dir}/tests".format(cur_dir=os.getcwd())
        self.profile_file = "{dir}/profiles.yml".format(dir=self.tests_dir)
        with open(self.profile_file, "w") as _:
            pass
        self.template_file = "{dir}/test_template.j2".format(dir=self.tests_dir)
        self.template_content = """
quollio_data_profiler:
  outputs:
    project:
      account: {{ account_id }}
      database: "QUOLLIO_DATA_PROFILER"
      role: {{ account_role }}
      schema: "PUBLIC"
      type: snowflake
      user: {{ account_user }}
      password: {{ account_password }}
      warehouse: {{ account_warehouse }}
  target: project
"""
        with open(self.template_file, "w") as f:
            f.write(self.template_content)

    def tearDown(self):
        if os.path.exists(self.profile_file):
            os.remove(self.profile_file)
        if os.path.exists(self.template_file):
            os.remove(self.template_file)

    def test_setup_dbt_profile(self):
        sf_conn_config = SnowflakeConnectionConfig(
            account_id="test-endpoint",
            account_role="TESTROLE",
            account_user="TESTUSER",
            account_password="TESTPASSWORD",
            account_warehouse="TEST_WH",
        )
        setup_dbt_profile(
            connections=sf_conn_config,
            project_path=self.tests_dir,
            template_path=self.tests_dir,
            template_name="test_template.j2",
        )
        self.assertTrue(os.path.exists(self.profile_file))

        with open(self.profile_file, "r") as f:
            expected = """quollio_data_profiler:
  outputs:
    project:
      account: test-endpoint
      database: QUOLLIO_DATA_PROFILER
      password: TESTPASSWORD
      role: TESTROLE
      schema: PUBLIC
      type: snowflake
      user: TESTUSER
      warehouse: TEST_WH
  target: project
"""
            content = f.read()
            self.assertEqual(content, expected)
