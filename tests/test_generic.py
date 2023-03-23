# import subprocess
import sys
import os
from shutil import rmtree
from time import sleep
import importlib

from command import run, CommandException

from hunabku.Hunabku import Hunabku
from hunabku.Config import ConfigGenerator

import unittest

import multiprocessing

class TestHunabku(unittest.TestCase):
    """
    Class to tests hunabku server options
    """

    def setUp(self):
        print('############################ running setUp ############################')
        try:
            res = run(['hunabku_server', '--generate_config', './config.py','--overwrite'])
        except CommandException as e:
            print(f"\33[41m {e.output} \33[0m")
            sys.exit(e.exit)
        print(res.output.decode())
        if res.exit != 0:
            print("ERROR: in generate_config ")
            sys.exit(res.exit)
        loader = importlib.machinery.SourceFileLoader('config', './config.py')
        spec = importlib.util.spec_from_loader(loader.name, loader)
        config = importlib.util.module_from_spec(spec)
        loader.exec_module(config)
        self.config = config.config


    def test__load_plugins(self):
        print('############################ running load plugins tests ############################')
        def hunabku():
            self.config.use_reloader = False
            server = Hunabku(self.config)
            server.apidoc_setup()
            server.load_plugins()
            server.generate_doc()
            server.start()
        proc = multiprocessing.Process(target=hunabku, args=())
        proc.start()
        # Terminate the process
        sleep(5)  # Corre el proceso de Flask durante 5 segundos
        proc.terminate()

    def tearDown(self):
        print('############################ running tearDown ############################')
        if os.path.exists("config.py"):
            os.remove("config.py")


if __name__ == '__main__':
    unittest.main()
