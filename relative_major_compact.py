#!/usr/bin/env python2

import argparse
import glob
import logging
import os
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format='%(message)s')

log = logging.getLogger(__name__)


def find_all_sstables(table_path):
    sstables = []
    path = os.path.join(table_path, '*Data.db')

    for sstable in glob.iglob(path):
        if 'tmp' not in sstable:
            sstables.append((os.path.getsize(sstable), os.path.basename(sstable)))
        else:
            log.debug("Ignoring temp file: %s" % sstable)
    sstables.sort()
    return sstables


def find_candidates(all_sstables, target_size):
    candidates = []
    candidates_size = 0
    target_size = int(target_size)

    for size, sstable in all_sstables:
        # we prefer to be under the target instead of over
        if candidates_size + size > target_size:
            break
        candidates.append(sstable)
        candidates_size += size
    log.info("Found %s sstables candidates i.e. %s bytes to compact. Target: %s bytes" %
             (len(candidates), candidates_size, target_size))
    return candidates


def parse_size(size_str):
    """Basic size parser, supports K, M, G and T suffix

    >>> parse_size('1024M')
    1073741824
    >>> parse_size('1G')
    1073741824
    >>> parse_size(1073741824)
    1073741824

    :param size_str: Size in bytes (int) or string that ends with a letter: 1.5G, 500M
    :return int: size in bytes
    """
    powers = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4}

    try:
        return int(size_str)
    except ValueError:
        try:
            size_str = size_str.upper()
            for unit, power in powers.items():
                if size_str.endswith(unit):
                    num = size_str[:-1]
                    return int(float(num) * power)
            raise ValueError("Unknown size representation: '%s'" % size_str)
        except ValueError:
            log.exception("Size must be specified as integer (bytes) "
                          "or must ends with a letter (K, M, G, T): 1.5G, 500M. Got: '%s'", size_str)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('table_path', help='Path to sstables to compact: e.g /var/lib/cassandra/data/ks/table/')
    parser.add_argument('target_size', help='Size in bytes of the sum of sstables to compact. '
                                            'M and G could be used: e.g. 1073741824 or 1G')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help="Verbose output. Print each sstable name that will participate in the compaction.")
    parser.add_argument('--dry-run', '-d', action='store_true', help="Simulation. Useful with --verbose.")
    parser.add_argument('--java', help="Path to Java. By default the java command is assumed to be on the path.",
                        default='java')
    parser.add_argument('--jmxterm', help="Path to JmxTerm. By default looks for jmxterm.jar in the current directory.",
                        default='jmxterm.jar')
    parser.add_argument('--host', '-l',
                        help="Location of Cassandra MBean service. "
                             "It can be <host>:<port> full service URL. "
                             "Default: 127.0.0.1:7199",
                        metavar='HOST:PORT',
                        default='127.0.0.1:7199')
    parser.add_argument('--user', '-u', help="JMX username")
    parser.add_argument('--password', '-p', help="JMX password")
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    all_sstables = find_all_sstables(args.table_path)
    size = parse_size(args.target_size)
    candidates = find_candidates(all_sstables, size)

    if len(candidates) == 0:
        log.error("No SSTables found in directory %s", args.table_path)
        sys.exit(1)
    log.debug("The following SSTables will be compacted: \n%s" % '\n'.join(candidates))
    candidates_csv = ','.join(candidates)
    jmx_cmd = ('echo run -b org.apache.cassandra.db:type=CompactionManager '
               'forceUserDefinedCompaction %s' % candidates_csv)
    java_cmd = '%(java_path)s -jar %(jmxterm_path)s -l %(jmx_host)s' % {
        'java_path': args.java,
        'jmxterm_path': args.jmxterm,
        'jmx_host': args.host
    }
    if args.user:
        java_cmd = '%s --user %s --password %s' % (
            java_cmd, args.user, args.password)
    cmd_to_run = '%s | %s' % (jmx_cmd, java_cmd)

    if args.dry_run:
        log.info('=' * 80)
        log.info("DRY RUN MODE. No compactions will be run.\n"
                 "Would have run the following command line:\n%s" % cmd_to_run)
    else:
        subprocess.check_call(cmd_to_run, shell=True)

