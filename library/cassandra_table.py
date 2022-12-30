#!/usr/bin/python3

DOCUMENTATION = '''
---
module: cassandra_keyspace
short_description: Manage your Cassandra tables
options:
  name:
    description:
      - name of the table to add or remove
    required: true
  keyspace:
    description:
      - name of the keyspace to use
    required: true
  columns:
    description:
      - A list of 'name' and 'type' of columns to create
    required: true
  order_by:
    description:
      - The column(s) the table will be ordered by
    required: false
  order_by_direction:
    description:
      - The direction the table will be ordered with
    required: false
    default: ASC
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
  state:
    description:
      - The table state
    required: false
    default: present
    choices: [ "present", "absent" ]
author: "Geoffrey Neal"
requirements:
  - cassandra (apt install)
'''

EXAMPLES = '''
- name: Create Cassandra table
  cassandra_keyspace:
    name: "test_table"
    keyspace: pluto
    columns:
      - name: datastream
        type: text
      - name: time
        type: timestamp
      - name: value
        type: float
    primary_keys:
      - datastream
      - time
    order_by:
      - time
    order_by_direction: "DESC"
    host: 10.0.0.5
    state: present
- name: Delete Cassandra table
  cassandra_keyspace:
    name: "test_table"
    keyspace: pluto
    columns:
      - name: datastream
        type: text
      - name: time
        type: timestamp
      - name: value
        type: float
    primary_keys:
      - datastream
      - time
    order_by:
      - time
    host: 10.0.0.5
    state: absent
'''


from ansible.module_utils.basic import *
from subprocess import Popen, PIPE

import ast
import re


def table_already_exists(keyspace, table, host, port, cql_version):
    process = Popen(["sudo","cqlsh", host, str(port), "--cqlversion", cql_version, "-k",
                     keyspace, "-e", "Describe tables;"],
                    stdout=PIPE, stderr=PIPE, universal_newlines=True)
    out, err = process.communicate()
    if process.returncode != 0:
        msg = "Bad returncode: {}; Error: {}".format(process.returncode, err)
        raise Exception(msg)
    return table in [t for t in out.split()]


def create_table(keyspace, table, columns, primary_keys, order_by,
                 order_by_direction, host, port, cql_version):
    cmd = "CREATE TABLE {}({}, PRIMARY KEY ({}))".format(
        table,
        ", ".join("{} {}".format(col["name"], col["type"]) for col in columns),
        ", ".join(primary_keys)
    )
    if order_by:
        cmd += "WITH CLUSTERING ORDER BY ({} {});".format(", ".join(order_by),
                                                          order_by_direction)

    process = Popen(["sudo", "cqlsh", host, str(port), "--cqlversion", cql_version, "-k",
                     keyspace, "-e", cmd], stdout=PIPE, stderr=PIPE,
                    universal_newlines=True)
    out, err = process.communicate()
    if process.returncode != 0:
        msg = "Bad returncode: {}; Error: {}".format(process.returncode, err)
        raise Exception(msg)


def drop_table(keyspace, table, host, port, cql_version):
    cmd = "DROP TABLE {};".format(table)
    process = Popen(["sudo","cqlsh", host, str(port), "--cqlversion", cql_version, "-k",
                     keyspace, "-e", cmd], stdout=PIPE, stderr=PIPE,
                    universal_newlines=True)
    out, err = process.communicate()
    if process.returncode != 0:
        msg = "Bad returncode: {}; Error: {}".format(process.returncode, err)
        raise Exception(msg)


def validate_columns(columns):
    errors = []
    for col in columns:
        if "name" not in col:
            errors.append("'name' missing from current column: {}".format(col))
        if "type" not in col:
            errors.append("'type' missing from current column: {}".format(col))

    return errors

def main():
    fields = {
        "name": {"required": True, "type": "str"},
        "keyspace": {"required": True, "type": "str"},
        "columns": {"required": True, "type": "list"},
        "primary_keys": {"required": True, "type": "list"},
        "order_by": {"required": False, "type": "list", "default": None},
        "order_by_direction": {"required": False, "type": "str", "default": "ASC"},
        "host": {"required": False, "type": "str", "default": "127.0.0.1"},
        "port": {"required": False, "type": "int", "default": 9042},
        "cql_version": {"required": False, "type": "str", "default": "3.4.4"},
        "state": {
            "default": "present",
            "choices": ['present', 'absent'],
            "type": 'str'
        },
    }

    module = AnsibleModule(argument_spec=fields)
    state = module.params["state"]
    table = module.params["name"]
    keyspace = module.params["keyspace"]
    host = module.params["host"]
    port = module.params["port"]
    cql_version = module.params["cql_version"]
    columns = module.params["columns"]
    primary_keys = module.params["primary_keys"]
    order_by = module.params["order_by"]
    order_by_direction = module.params["order_by_direction"]

    column_errors = validate_columns(columns)
    if len(column_errors) > 0:
        module.fail_json(msg=column_errors)

    try:
        exists = table_already_exists(keyspace, table, host, port, cql_version)
    except Exception as err:
        module.fail_json(msg=err)

    if state == "present":
        if exists:
            module.exit_json(changed=False)
        else:
            try:
                create_table(keyspace, table, columns, primary_keys,
                             order_by, order_by_direction, host, port, cql_version)
            except Exception as err:
                msg = "Error creating table: {}".format(err)
                module.fail_json(msg=msg)
            else:
                module.exit_json(changed=True)
    elif state == "absent":
        if exists:
            try:
                drop_table(keyspace, table, host, port, cql_version)
            except Exception as err:
                msg = "Error deleting table: {}".format(err)
                module.fail_json(msg=msg)
            else:
                module.exit_json(changed=True)
        else:
            module.exit_json(changed=False)


if __name__ == '__main__':
    main()
