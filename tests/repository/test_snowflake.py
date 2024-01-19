import os
import sys
import unittest

sys.path.insert(0, os.path.abspath("../.."))


class TestSetup(unittest.TestCase):
    def setUp(self):
        self.tests_dir = "{cur_dir}/tests".format(cur_dir=os.getcwd())
        self.profile_file = "{dir}/profiles.yml".format(dir=self.tests_dir)
        with open(self.profile_file, "w") as _:
            pass
        self.template_file = "{dir}/test_template.j2".format(dir=self.tests_dir)
        self.template_content = """
quollio_intelligence_snowflake:
  outputs:
    project:
      account: {{ account_id }}
      database: {{ account_database }}
      role: {{ account_build_role }}
      schema: {{ account_schema }}
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
