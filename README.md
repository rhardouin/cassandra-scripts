# cassandra-scripts


## vnodes_token_generator.py

Vnodes Murmur3 tokens generator: generate evenly distributed initial tokens for a vnodes Cassandra cluster.
`initial_token` can be set in cassandra.yaml whith vnodes by using comma separated values.

### Usage

    vnodes_token_generator.py [-h] [-i INDENT] [-n NUM | -s SERVERS] vnodes

### Examples

#### Json output

First, create a file which contains one host per line:

    $ cat hosts.txt
    192.168.128.21
    192.168.128.22
    192.168.128.23

Then run `vnodes_token_generator.py` with the number of vnodes you want. Here, just `4` to keep the example readable:

    ./vnodes_token_generator.py --json --indent 2 --servers hosts.txt 4
    {
      "192.168.128.1": "-9223372036854775808,-4611686018427387905,-2,4611686018427387901",
      "192.168.128.2": "-7686143364045646507,-3074457345618258604,1537228672809129299,6148914691236517202",
      "192.168.128.3": "-6148914691236517206,-1537228672809129303,3074457345618258600,7686143364045646503"
    }

#### cassandra.yaml `initial_token` output

    ./vnodes_token_generator.py --servers hosts.txt 4
    Server 192.168.128.22 -> initial_token: -7686143364045646507,-3074457345618258604,1537228672809129299,6148914691236517202
    Server 192.168.128.23 -> initial_token: -6148914691236517206,-1537228672809129303,3074457345618258600,7686143364045646503
    Server 192.168.128.21 -> initial_token: -9223372036854775808,-4611686018427387905,-2,461168601842738790

#### No servers file

Only the number of servers is specified:


    ./vnodes_token_generator.py -n 3 4
    Server 0 -> initial_token: -9223372036854775808,-4611686018427387905,-2,4611686018427387901
    Server 1 -> initial_token: -7686143364045646507,-3074457345618258604,1537228672809129299,6148914691236517202
    Server 2 -> initial_token: -6148914691236517206,-1537228672809129303,3074457345618258600,7686143364045646503
