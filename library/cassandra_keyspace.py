#!/usr/bin/python3

DOCUMENTATION = '''
---
module: cassandra_keyspace
short_description: Manage your Cassandra keyspaces (SimpleStrategy only)
options:
  name:
    description:
      - name of the keyspace to add or remove
    required: true
  host:
    description:
      - host to run the Cassandra commands against
    required: false
    default: 127.0.0.1
  port:
    description:
      - port to run the Cassandra commands against
    required: false
    default: 9042
  cql_version:
    description:
      - 'cqlversion' to check if Cassandra connection is available
    required: false
    default: 3.4.4
  replication_factor:
    description:
      - replication factor to set for the keyspace
    required: false
    default: 1
  state:
    description:
      - The keyspace state
    required: false
    default: present
    choices: [ "present", "absent" ]
author: "Geoffrey Neal"
requirements:
  - cassandra (apt install)
'''

EXAMPLES = '''
- name: Create Cassandra keyspace
  cassandra_keyspace:
    name: "test_keyspace"
    replication_factor: 3
    host: 10.0.0.5
    state: present
- name: Delete Cassandra keyspace
  cassandra_keyspace:
    name: "test_keyspace"
    state: absent
'''

CREATE_CMD = """CREATE KEYSPACE {} WITH replication = \
{{'class':'SimpleStrategy', 'replication_factor' : {}}};"""
ALTER_CMD = """ALTER KEYSPACE {} WITH REPLICATION = \
{{'class': 'SimpleStrategy', 'replication_factor': {}}};"""


from ansible.module_utils.basic import *
from subprocess import Popen, PIPE

import ast
import re


def keyspace_already_exists(keyspace, host, port, cql_version):
    print(host)
    print(port)
    process = Popen(["cqlsh", host, str(port), "--cqlversion", cql_version, "-e",
                     "DESC KEYSPACES;"],
                    stdout=PIPE, stderr=PIPE, universal_newlines=True)
    out, err = process.communicate()
    if process.returncode != 0:
        msg = "Bad returncode: {}; Error: {}".format(process.returncode, err)
        raise Exception(msg)
    return keyspace in [k for k in out.split()]


def create_keyspace(keyspace, host, port, cql_version, replication_factor):
    cmd = CREATE_CMD.format(keyspace, replication_factor)
    process = Popen(["sudo", "cqlsh", host, str(port), "--cqlversion", cql_version, "-e",
                     cmd], stdout=PIPE, stderr=PIPE, universal_newlines=True)
    out, err = process.communicate()
    if process.returncode != 0:
        msg = "Bad returncode: {}; Error: {}".format(process.returncode, err)
        raise Exception(msg)


def get_current_replication_factor(keyspace, host, port, cql_version):
    process = Popen(["sudo", "cqlsh", host, str(port), "--cqlversion", cql_version, "-e",
                     "Describe keyspace {};".format(keyspace)],
                    stdout=PIPE, stderr=PIPE, universal_newlines=True)
    out, err = process.communicate()
    if process.returncode != 0:
        msg = "Bad returncode: {}; Error: {}".format(process.returncode, err)
        raise Exception(msg)
    replication_regex = re.search("{(.*?)}", out).group(1)
    replication_dict = ast.literal_eval("{{{}}}".format(replication_regex))
    return int(replication_dict["replication_factor"])


def update_keyspace_replication(keyspace, host, port,
                                cql_version, replication_factor):
    cmd = ALTER_CMD.format(keyspace, replication_factor)
    process = Popen(["sudo", "cqlsh", host, str(port), "--cqlversion", cql_version, "-e",
                     cmd], stdout=PIPE, stderr=PIPE, universal_newlines=True)
    out, err = process.communicate()
    if process.returncode != 0:
        msg = "Bad returncode: {}; Error: {}".format(process.returncode, err)
        raise Exception(msg)


def drop_keyspace(keyspace, host, port, cql_version):
    cmd = "DROP KEYSPACE {};".format(keyspace)
    process = Popen(["sudo", "cqlsh", host, str(port), "--cqlversion", cql_version, "-e",
                     cmd], stdout=PIPE, stderr=PIPE, universal_newlines=True)
    out, err = process.communicate()
    if process.returncode != 0:
        msg = "Bad returncode: {}; Error: {}".format(process.returncode, err)
        raise Exception(msg)


def main():
    fields = {
        "name": {"required": True, "type": "str"},
        "host": {"required": False, "type": "str", "default": "127.0.0.1"},
        "port": {"required": False, "type": "int", "default": 9042},
        "cql_version": {"required": False, "type": "str", "default": "3.4.4"},
        "replication_factor": {"required": False, "type": "int", "default": 1},
        "state": {
            "default": "present",
            "choices": ['present', 'absent'],
            "type": 'str'
        },
    }

    module = AnsibleModule(argument_spec=fields)
    state = module.params["state"]
    keyspace = module.params["name"]
    host = module.params["host"]
    port = module.params["port"]
    cql_version = module.params["cql_version"]
    replication_factor = module.params["replication_factor"]

    try:
        exists = keyspace_already_exists(keyspace, host, port, cql_version)
    except Exception as err:
        module.fail_json(msg=err)

    if state == "present":
        if exists:
            current_rep_factor = get_current_replication_factor(keyspace, host,
                                                                port, cql_version)
            if current_rep_factor != replication_factor:
                try:
                    update_keyspace_replication(keyspace, host, replication_factor)
                except Exception as err:
                    msg = "Error updating keyspace: {}".format(err)
                    module.fail_json(msg=msg)
                else:
                    module.exit_json(changed=True)
            else:
                module.exit_json(changed=False)
        else:
            try:
                create_keyspace(keyspace, host, port, cql_version,
                                replication_factor)
            except Exception as err:
                msg = "Error creating keyspace: {}".format(err)
                module.fail_json(msg=msg)
            else:
                module.exit_json(changed=True)
    elif state == "absent":
        if exists:
            try:
                delete_keyspace(keyspace, host, port, cql_version)
            except Exception as err:
                msg = "Error deleting keyspace: {}".format(err)
                module.fail_json(msg=msg)
            else:
                module.exit_json(changed=True)
        else:
            module.exit_json(changed=False)


if __name__ == '__main__':
    main()
