#!/usr/bin/env python

import os
import sys
import optparse
from sql2graph.export2 import SQL2GraphExporter
#from musicbrainz_schema import mbschema, mbentities
from musicbrainz_schema__20131014 import mbschema, mbentities

# ----------------------------------------------------------------------

DEFAULT_NODES_FILE='/tmp/musicbrainz__nodes__full.csv'
DEFAULT_RELS_FILE='/tmp/musicbrainz__rels__full.csv'
MULTIPLE_FILES = False
option_parser = optparse.OptionParser()
option_parser.add_option("--nodes", dest="nodes_filename",
    help="Nodes file", default=DEFAULT_NODES_FILE)
option_parser.add_option("--relations", dest="relations_filename",
    help="Relationships file", default=DEFAULT_RELS_FILE)
option_parser.add_option("--multiple", action="store_true",
    dest="multiple_files",
    help="whether to output multiple nodes files and relationships files",
    default=MULTIPLE_FILES)
option_parser.add_option("--limit", type="int", dest="limit", default=None)
(options, args) = option_parser.parse_args()


class MusicBrainzExporter(SQL2GraphExporter):
    nodes_header_override = {
            "mbid": '"mbid:string:mbid"',
            "kind": '"kind:string:mbid"',
            "pk":   '"pk:long:mbid"',
            "name": '"name:string:mb"',
            "type": '"typ:string:mbid"',
            "latitude": '"latitude:float"',
            "longitude": '"longitude:float"',
        }

exporter = MusicBrainzExporter(mbschema, mbentities)

exporter.set_nodes_filename(options.nodes_filename)
exporter.set_rels_filename(options.relations_filename)

if options.limit:
    exporter.set_entity_export_limit(options.limit)

print r"""
-- Change TABs to spaces in "name" column for "track" and "work" tables
-- somehow these TABs can make batch-import CSV parsing choked
UPDATE track SET name=translate(name, E'\t', ' ') WHERE name LIKE E'%\t%';
UPDATE work SET name=translate(name, E'\t', ' ') WHERE name LIKE E'%\t%';
"""
print exporter.create_mapping_table_query(multiple=options.multiple_files)
print exporter.create_nodes_query(multiple=options.multiple_files)
print exporter.create_relationships_query(multiple=options.multiple_files)
