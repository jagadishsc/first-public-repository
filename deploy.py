import argparse
import base64
import json
import os
import random
import string
import subprocess
import tempfile
from datetime import datetime

import gspread
from pytz import timezone
from termcolor import cprint

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))


class PlatformDeployment(object):
    NEEDED_CONFIG_KEYS = ["subdomain", "sa_path", "private_key", "mode",
                          "fixtures_path", "ga_id", "region", "training_cron", "poller_host", "poller_password",
                          "poller_user", "poller_port", "database_name"]
    NEEDED_SIMPLE_KEYS = ["builds", "jenkins_user", "jenkins_api_key"]
    NEEDED_MULTI_KEYS = ["images", ]

    DEFAULT_MAP = {"region": "us-west1", "poller_cron": "0 * * * *", "training_cron": "0 4 * * 0",
                   "poller_host": "Not Specified", "poller_password": "Not Specified",
                   "poller_user": "Not Specified", "poller_port": "Not Specified",
                   "database_name": "pluto"}

    def __init__(self, config_path):
        self.config_path = config_path
        self.log_path = "build_log"
        self._load_config()
        self._load_service_account()
        self._create_or_load_additional_secrets()
        self.config["platform_name"] = "platform-{}".format(self.config["subdomain"])
        frontend_subdomain = self.config.get('frontend_subdomain', self.config['subdomain'])
        self.config["server_name"] = "{}.plutoshift.com".format(frontend_subdomain)
        self.config["frontend"] = "https://{}".format(self.config["server_name"])
        # frontend=https://orca4.plutoshift.com
        # self.config["frontend"]="https://34.111.54.246"

        if not self.config.get('sql_subdomain'):
            self.config['sql_subdomain'] = self.config["subdomain"]
        self.config["sql_platform_name"] = f"platform-{self.config['sql_subdomain']}"
        print ("sql_subdomain-"+self.config.get('sql_subdomain'))
        print ("sql_platform-"+self.config["sql_platform_name"])

    def _load_config(self):
        if not os.path.exists(self.config_path):
            raise PlatformDeploymentException("Deploy config does not exist")
        with open(self.config_path, "r") as json_file:
            self.config = json.load(json_file)
            with open(self.log_path, 'a') as logfile:
                logfile.write(f"\n{json_file.read()}")

        if self.config["mode"] in ["simple", "simple_osu", "simple_osu_restart"]:
            check_keys = self.NEEDED_CONFIG_KEYS + self.NEEDED_SIMPLE_KEYS
        elif self.config["mode"].startswith("multi"):
            check_keys = self.NEEDED_CONFIG_KEYS + self.NEEDED_MULTI_KEYS
        else:
            check_keys = self.NEEDED_CONFIG_KEYS + self.NEEDED_MULTI_KEYS

        for key in check_keys:
            if key not in self.config.keys():
                if key in self.DEFAULT_MAP:
                    self.config[key] = self.DEFAULT_MAP[key]
                else:
                    raise PlatformDeploymentException("Missing key: {}".format(key))

    def _load_service_account(self):
        with open(self.config["sa_path"], "r") as json_file:
            sa_config_raw = json_file.read()
        sa_config = json.loads(sa_config_raw)
        self.config["sa_client_email"] = sa_config["client_email"]
        self.config["gcp_project_id"] = sa_config["project_id"]
        raw_gcr = "_json_key: {}".format(sa_config_raw)
        gcr_encoded = base64.b64encode(raw_gcr.encode())
        gcr_secret = {"auths": {"https://gcr.io": {"username": "_json_key",
                                                   "password": sa_config_raw, "email": sa_config["client_email"],
                                                   "auth": gcr_encoded.decode()}}}
        self.config["gcr_secret"] = json.dumps(gcr_secret)

    def _create_secret_key(self, count=50):
        choices = string.ascii_letters + string.digits
        return "".join([random.SystemRandom().choice(choices) for i in range(count)])

    def _create_lower_secret_key(self, count=50):
        choices = string.ascii_lowercase + string.digits
        return "".join([random.SystemRandom().choice(choices) for i in range(count)])

    def _create_or_load_additional_secrets(self):
        subdomain = self.config.get('sql_subdomain', self.config['subdomain'])
        json_file_name = f"{subdomain}_deployment.json"
        if os.path.exists(json_file_name):
            with open(json_file_name, "r") as secrets_file:
                deploy_secrets = json.load(secrets_file)
        else:
            deploy_secrets = {"backend_secret_key": self._create_secret_key()}
            if self.config["mode"].startswith("multi") or self.config["mode"] == "microservices":
                deploy_secrets["sql_password"] = self._create_secret_key()
                deploy_secrets["sql_tag"] = self._create_lower_secret_key(count=8)
            with open(json_file_name, "w") as secrets_file:
                json.dump(deploy_secrets, secrets_file)

        # backend_secret_key is a reCaptcha secret key coming from  orca4_deployment.json
        for key, val in deploy_secrets.items():
            print ("key-"+key)
            print ("value-"+val)
            self.config[key] = val

    def run_playbook(self, operation, live_client, changes, verbosity):
        if operation == "fix_mysql":
            playbook = "multi_alerts_fix_mysql.yml"
        elif operation == "launch_postgres":
            playbook = "launch_postgres.yml"
        elif operation == "microservices":
            playbook = "microservices.yml"
        else:
            playbook = "{}_{}.yml".format(self.config["mode"], operation)

        print (playbook)

        if verbosity:
            verbosity = f"-{verbosity}"
        with tempfile.NamedTemporaryFile(mode='w') as json_config_file:
            json.dump(self.config, json_config_file)
            json_config_file.flush()
            command = ["/usr/local/bin/ansible-playbook",
                       "--private-key", self.config["private_key"],
                       "-e", "@{}".format(json_config_file.name), playbook]
            if verbosity:
                command.append(verbosity)
            cprint("Executing: {}".format(subprocess.list2cmdline(command)),
                   "yellow")
            proc = subprocess.Popen(command, cwd=CURRENT_DIR,
                                    universal_newlines=True)
            proc.communicate()
            if proc.returncode != 0:
                msg = "Bad Return Code: {}".format(proc.returncode)
                print(msg)
                self._print_log(msg, live_client, changes)
                raise PlatformDeploymentException(msg)
            else:
                self._print_log('Success', live_client, changes)

    def _print_log(self, status, live_client, changes):
        gc = gspread.service_account(filename="abednarek.json")
        log_spreadsheet = gc.open('Deployment Logs')
        logs = log_spreadsheet.sheet1
        # Adding new row at second row each time
        # Subdomain, Build numbers, Time, Mode
        subdomain = self.config["subdomain"]
        time_pacific = datetime.now(timezone('US/Pacific'))
        backend = self.config['images']['backend']['tag']
        frontend = self.config['images']['frontend']['tag']
        mode = self.config['mode']
        name = os.popen('logname').read()[:-1]
        # module_number, build_type = self._get_module_number()
        module_number, build_type = "Disabled", "Disabled"
        new_row = [subdomain, frontend, backend, module_number, build_type, str(time_pacific), mode, status, name,
                   str(bool(live_client)), changes]
        logs.insert_row(new_row, 2)

    # call to this  function is commented
    def _get_module_number(self):
        wget_url = ''
        build_type = ''
        if 'backend-testing' in self.config['images']['backend']['image_url']:
            wget_url = 'http://35.236.104.21:8080/job/Backend%20Redesign/job/Testing%20(Branch%20Selection)/'
            build_type = 'Testing'
        else:
            wget_url = 'http://35.236.104.21:8080/job/Backend%20Redesign/job/Master/'
            build_type = 'Testing'

        wget_url = f"{wget_url}{self.config['images']['backend']['tag']}/consoleText"
        print (wget_url)
        command = ['wget', '--auth-no-challenge', '--user=patrickbagot',
                   '--password=114771fa5c5f47e9085c4b10c5c6c6bde2', wget_url]

        proc = subprocess.Popen(command, cwd=CURRENT_DIR,
                                universal_newlines=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        proc.communicate()
        module_number = "Not found"
        if proc.returncode != 0:
            print("Failed to find interface module build number")
        else:
            f = open('consoleText', 'r')
            for r in f.readlines():
                if 'Copied 1 artifact from "Interface Module' in r:
                    module_number = str((r[-3:-1]))
            f.close()
            os.remove("consoleText")
        return module_number, build_type


class PlatformDeploymentException(Exception):
    pass


def build_parser():
    parser = argparse.ArgumentParser(description='Deploy the Plutoshift Platform.')
    parser.add_argument('operation', type=str, choices=["absent", "present", "fix_mysql", "launch_postgres"],
                        help="The type of operation for deployment.")
    parser.add_argument('config_path', type=str, help="Path to config.")
    parser.add_argument('live_client', type=int, help="Is this a customer facing deployment?", choices=[0, 1])
    parser.add_argument('-m', type=str, default='', help="Description of changes being made in this deployment")
    parser.add_argument('--verbosity', type=str, default='', help='Ansible verbosity')
    return vars(parser.parse_args())


if __name__ == '__main__':
    args = build_parser()
    print (args["config_path"])
    print (args["operation"])
    print (args['live_client'])
    print (args['m'])
    print (args['verbosity'])
    pd = PlatformDeployment(args["config_path"])
    pd.run_playbook(args["operation"], args['live_client'], args['m'],
        args['verbosity'])
        # 'vvv')
