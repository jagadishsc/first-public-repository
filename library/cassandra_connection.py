#!/usr/bin/python3

DOCUMENTATION = '''
---
module: cassandra_connection
short_description: Check if connection to cassandra is valid
options:
  host:
    description:
      - host to check if Cassandra connection is available
    required: false
    default: 127.0.0.1
  port:
    description:
      - port to check if Cassandra connection is available
    required: false
    default: 9042
  cql_version:
    description:
      - 'cqlversion' to check if Cassandra connection is available
    required: false
    default: 3.4.4
  retries:
    description:
      - Number of retries to try
    required: false
    default: 60
author: "Geoffrey Neal"
requirements:
  - cassandra (apt install)
'''

EXAMPLES = '''
- name: Create Cassandra table
  cassandra_keyspace:
    host: 10.0.0.5
    retries: 30
'''


from ansible.module_utils.basic import *
from subprocess import Popen, PIPE

import ast
import re
import time


def attempt_connection(host, port, cql_version):
    process = Popen(["sudo", "cqlsh", host, str(port), "--cqlversion", cql_version, "-e",
                     "SELECT now() FROM system.local;"],
                    stdout=PIPE, stderr=PIPE, universal_newlines=True)
    out, err = process.communicate()
    if process.returncode != 0:
        raise Exception(err)


def main():
    fields = {
        "host": {"required": False, "type": "str", "default": "127.0.0.1"},
        "port": {"required": False, "type": "int", "default": 9042},
        "cql_version": {"required": False, "type": "str", "default": "3.4.4"},
        "retries": {"required": False, "type": "int", "default": 60},
    }

    module = AnsibleModule(argument_spec=fields)
    host = module.params["host"]
    port = module.params["port"]
    cql_version = module.params["cql_version"]
    retries = module.params["retries"]

    count = 0
    errors_set = set()
    while count < retries:
        try:
            attempt_connection(host, port, cql_version)
        except Exception as current_err:
            errors_set.add(current_err)
            time.sleep(1)
            count += 1
        else:
            module.exit_json(changed=False)
    else:
        module.fail_json(msg=list(errors_set))


if __name__ == '__main__':
    main()
