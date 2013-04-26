#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2013 Paul Tremberth, Newlynn Labs
# See LICENSE for details.

import optparse
import ConfigParser
import os
import traceback

# ----------------------------------------------------------------------

def main():

    option_parser = optparse.OptionParser()
    option_parser.add_option("-c", "--config", dest="configfile", help="configuration file", default=None)
    (options, args) = option_parser.parse_args()

    config_parser = ConfigParser.RawConfigParser()
    if options.configfile:
        config_parser.read(options.configfile)
    else:
        print "you must provide a config file"
        sys.exit(0)

    if config_parser.has_section('INDEX_FILES'):
        index_files = dict((
                (index_name, index_file)
                    for index_name, index_file in config_parser.items('INDEX_FILES')
            ))
    else:
        print "no INDEX_FILES section"
        index_files = None

    args = ['java']
    args.append('-server')

    args.append('-Xmx1G')

    if config_parser.has_option('BATCHIMPORT_SETTINGS', 'jar_location'):
        args.append('-jar')
        args.append(config_parser.get('BATCHIMPORT_SETTINGS', 'jar_location'))

    if config_parser.has_option('BATCHIMPORT_SETTINGS', 'dbfile_location'):
        args.append(config_parser.get('BATCHIMPORT_SETTINGS', 'dbfile_location'))

    if config_parser.has_option('BATCHIMPORT_SETTINGS', 'nodes_file'):
        args.append(config_parser.get('BATCHIMPORT_SETTINGS', 'nodes_file'))
    if config_parser.has_option('BATCHIMPORT_SETTINGS', 'relations_file'):
        args.append(config_parser.get('BATCHIMPORT_SETTINGS', 'relations_file'))
    if config_parser.has_section('INDEX_FILES'):
        for index_name, index_file in config_parser.items('INDEX_FILES'):
            args.append('node_index')
            args.append(index_name)
            args.append('fulltext')
            args.append(index_file)

    # check what's being executed
    print " ".join(args)

    os.execvp("java", args)


if __name__ == '__main__':
    main()

