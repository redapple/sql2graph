#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2013 Paul Tremberth, Newlynn Labs
# See LICENSE for details.

import optparse
import ConfigParser
import os
import traceback
import pprint

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

    if not config_parser.has_option('TABLE_DUMPS_DIR', 'dir'):
        print "you must provide TABLE_DUMPS_DIR/dir"
        sys.exit(0)
    table_dumps_dir = config_parser.get('TABLE_DUMPS_DIR', 'dir')

    if config_parser.has_section('TABLE_DUMPS'):
        dump_tables = dict((
                (entity, "%s/%s" % (table_dumps_dir, dump_file))
                    for entity, dump_file in config_parser.items('TABLE_DUMPS')
            ))
    else:
        print "no TABLE_DUMPS section"
        raise SystemExit

    if config_parser.has_option('IMPORT_ORDER', 'order'):
        entity_order = [entity.strip()
            for entity in config_parser.get('IMPORT_ORDER', 'order').split(',')]
    else:
        print "no IMPORT_ORDER/order"
        raise SystemExit


    args = ['java']
    args.append('-server')

    args.append('-Xmx1G')

    if config_parser.has_option('BATCHIMPORT_SETTINGS', 'jar_location'):
        args.append('-jar')
        args.append(config_parser.get('BATCHIMPORT_SETTINGS', 'jar_location'))

    if config_parser.has_option('BATCHIMPORT_SETTINGS', 'dbfile_location'):
        args.append(config_parser.get('BATCHIMPORT_SETTINGS', 'dbfile_location'))

    #pprint.pprint(dump_tables)
    #pprint.pprint(entity_order)
    dumped_entities = dump_tables.keys()
    if config_parser.has_option('BATCHIMPORT_SETTINGS', 'nodes_file_prefix'):
        nodes_files_prefix = config_parser.get('BATCHIMPORT_SETTINGS', 'nodes_file_prefix')
        nodes_files = ["%s%s.csv" % (nodes_files_prefix, e) for e in entity_order if e in dumped_entities]
        args.append(','.join(nodes_files))
        
    if config_parser.has_option('BATCHIMPORT_SETTINGS', 'relations_file_prefix'):
        relations_files_prefix = config_parser.get('BATCHIMPORT_SETTINGS', 'relations_file_prefix')
        relations_files = ["%s%s.csv" % (relations_files_prefix, e) for e in entity_order if e in dumped_entities]
        args.append(','.join(relations_files))

    # check what's being executed
    print " ".join(args)
    os.execvp("java", args)


if __name__ == '__main__':
    main()

