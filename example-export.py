#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2013 Paul Tremberth, Newlynn Labs
# See LICENSE for details.

import optparse
import ConfigParser
import os
import sys
import traceback
import csv
import sql2graph.export
from sql2graph.schema import Field, IntField, Relation, Reference, Column, Entity, Property

# ----------------------------------------------------------------------

SCHEMA = (
    Entity('car',
        fields = [
            IntField('pk', Column('id'), primary_key=True, index='cars'),
            Field('name', Column('name'), index='cars'),
            Field('begin', Column('begin'), index='cars'),
            Field('end', Column('end'), index='cars'),
        ],
        relations = [
            Relation(Reference('car', 'id'),
                     Reference('manufacturer', 'manufacturer'),
                     [Property('rel_type', 'makes/made')])
        ]
    ),
    Entity('manufacturer',
        fields = [
            IntField('pk', Column('id'), primary_key=True, index='manufacturers'),
            Field('name', Column('name'), index='manufacturers'),
        ],
    )
)

DEFAULT_NODES_FILE = 'nodes.csv'
DEFAULT_RELATIONS_FILE = 'relations.csv'

def main():

    option_parser = optparse.OptionParser()
    option_parser.add_option("-c", "--config", dest="configfile", help="configuration file", default=None)
    option_parser.add_option("-p", "--pretend", action="store_true", dest="pretend", help="just pretend; do not write anthing", default=False)
    option_parser.add_option("-N", "--nodes", dest="nodes_file", help="Nodes file", default=None)
    option_parser.add_option("-R", "--relations", dest="relations_file", help="Relations file", default=None)
    (options, args) = option_parser.parse_args()

    config_parser = ConfigParser.RawConfigParser()
    if options.configfile:
        config_parser.read(options.configfile)
    else:
        print "you must provide a config file"
        sys.exit(0)

    if config_parser.has_section('TABLE_DUMPS'):
        dump_tables = dict((
                (entity, dump_file)
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

    if config_parser.has_section('INDEX_FILES'):
        index_files = dict((
                (index_name, index_file)
                    for index_name, index_file in config_parser.items('INDEX_FILES')
            ))
    else:
        print "no INDEX_FILES section"
        index_files = None


    # check if all dump files exist
    for entity in entity_order:
        if dump_tables.get(entity):
            if not os.path.isfile(dump_tables.get(entity)):
                print "file %s does not exist" % dump_tables.get(entity)
                raise RuntimeError

    # For TAB-delimited CSV files, use dialect=csv.excel_tab
    # In this cars example, the CSV files use commas
    exporter = sql2graph.export.GraphExporter(
        schema=SCHEMA, format='neo4j', dialect=csv.excel)

    exporter.set_output_nodes_file(
        entity=sql2graph.export.MERGED,
        filename=options.nodes_file or DEFAULT_NODES_FILE)

    exporter.set_output_relations_file(
        entity=sql2graph.export.MERGED,
        filename=options.relations_file or DEFAULT_RELATIONS_FILE)

    for index_name, index_file in index_files.iteritems():
        exporter.set_output_indexes_file(entity=index_name, filename=index_file)

    for entity_name in entity_order:
        if dump_tables.get(entity):
            exporter.feed_dumpfile(entity=entity_name, filename=dump_tables.get(entity_name))

    exporter.run()


if __name__ == '__main__':
    main()

