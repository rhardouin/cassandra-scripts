#!/usr/bin/env python
"""usage: vnodes_token_generator.py [-h] [-i INDENT] [-n NUM | -s SERVERS] vnodes

Vnodes Murmur3 tokens generator: generate evenly distributed initial tokens for a vnodes Cassandra cluster.

`initial_token` can be set in cassandra.yaml whith vnodes by using comma separated values.

Here is an example usage::

    ./vnodes_token_generator.py --json --indent 2 --servers hosts.txt 4
    {
      "192.168.128.1": "-9223372036854775808,-4611686018427387905,-2,4611686018427387901",
      "192.168.128.2": "-7686143364045646507,-3074457345618258604,1537228672809129299,6148914691236517202",
      "192.168.128.3": "-6148914691236517206,-1537228672809129303,3074457345618258600,7686143364045646503"
    }

"""
from collections import defaultdict
import json


def generate_tokens(vnodes, num=None, server=None):
    """Generate evenly distributed interleaved tokens for each server.

    :param vnodes: int Number of vnodes per server
    :param num: int Number of Cassandra servers
    :param servers: list List of Cassandra IP/Hostname
    :return: defaultdict key=server number, value=its tokens
    """

    assert any([num, servers]), "Number of servers or list of servers is mandatory"

    num = len(servers)
    num_tokens = vnodes * num
    tokens_per_srv = defaultdict(list)
    current_srv = 0

    for i in range(num_tokens):
        token = str(((2**64 / num_tokens) * i) - 2**63)
        tokens_per_srv[current_srv].append(token)
        current_srv += 1

        if current_srv % servers == 0:
            current_srv = 0
    return tokens_per_srv


def generate_tokens(vnodes, num_srv):
    """Generate evenly distributed interleaved tokens for each server.

    :param vnodes: int Number of vnodes per server
    :param num_srv: int Number of Cassandra servers
    :return: defaultdict key=server number, value=its tokens
    """
    num_tokens = vnodes * num_srv
    srv2tokens = defaultdict(list)
    current_srv = 0

    for i in range(num_tokens):
        token = str(((2**64 / num_tokens) * i) - 2**63)
        srv2tokens[current_srv].append(token)
        current_srv += 1

        if current_srv % num_srv == 0:
            current_srv = 0
    return srv2tokens


def show_cass_yaml(tokens_per_srv):
    """Print tokens for cassandra.yaml

    :param tokens_per_srv: dict key=server number, value=its tokens
    :return:
    """
    for index_or_hostname, srv_tokens in tokens_per_srv.items():
        print("Server %s -> initial_token: %s\n" % (index_or_hostname, ','.join(srv_tokens)))


def show_json(tokens_per_srv, indent=None):
    tokens_databag = {}

    for srv, srv_tokens in tokens_per_srv.items():
        tokens_databag[srv] = ','.join(srv_tokens)
    print(json.dumps(tokens_databag, sort_keys=True, indent=indent))

if __name__ == '__main__':
    import argparse
    import json.tool
    import sys

    parser = argparse.ArgumentParser(usage=__doc__)
    parser.add_argument('vnodes', type=int, help='Number of vnodes per server')
    parser.add_argument('-i', '--indent', type=int, help='JSON indentation spaces (e.g. 4)')

    format_group = parser.add_mutually_exclusive_group()
    format_group.add_argument('-j', '--json', action='store_true', help='JSON output')
    format_group.add_argument('-y', '--yaml', action='store_true', help='initial_token for cassandra.yaml')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-n', '--num', type=int, help='Number of Cassandra servers')
    group.add_argument('-s', '--servers', type=str, help='Cassandra servers file. One IP/hostname per line.')

    args = parser.parse_args()

    if args.num:
        num_servers = args.num
    elif args.servers:
        with open(args.servers) as f:
            servers = [line.strip() for line in f if line.strip() != '']
        num_servers = len(servers)
    else:
        sys.exit("Specify number of servers or file")

    tokens_per_srv = generate_tokens(args.vnodes, num_servers)

    if args.servers:
        tokens = tokens_per_srv.values()
        tokens_per_srv = dict(zip(servers, tokens))

    if args.json:
        show_json(tokens_per_srv, indent=args.indent)
    else:
        show_cass_yaml(tokens_per_srv)
