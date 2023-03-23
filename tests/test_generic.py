# import subprocess
import requests
import sys
import os
from shutil import rmtree
from command import run, CommandException

from hunabku.Hunabku import Hunabku
from hunabku.Config import ConfigGenerator

import unittest


class TestHunabku(unittest.TestCase):
    """
    Class to tests hunabku server options
    """

    def setUp(self):
        print('############################ running setUp ############################')
        res = run(['hunabku_server', '--generate_config', 'config.py','--overwrite'])
        print(res.output.decode())
        if res.exit != 0:
            print("ERROR: in generate_config ")
            sys.exit(res.exit)

    def test__load_plugins(self):
        print('############################ running load plugins tests ############################')
        res = run(['hunabku_server', '--config', 'config.py'])
        print(res.output.decode())
        if res.exit != 0:
            print("ERROR: loading plugins")
            sys.exit(res.exit)

    def tearDown(self):
        print('############################ running tearDown ############################')
        if os.path.exists("config.py"):
            os.remove("config.py")


if __name__ == '__main__':
    unittest.main()
