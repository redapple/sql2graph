#!/usr/bin/env python

import os
import sys
import optparse
from sql2graph.export2 import SQL2GraphExporter
from musicbrainz_schema import mbschema, mbentities, make_link_entity_list

# ----------------------------------------------------------------------

DEFAULT_NODES_FILE='/tmp/musicbrainz__nodes__full.csv'
DEFAULT_RELS_FILE='/tmp/musicbrainz__rels__full.csv'
MULTIPLE_FILES = False

option_parser = optparse.OptionParser()

option_parser.add_option("--exclude", dest="excluded_entities",
    help="Entities to exclude", default=None)
option_parser.add_option("--include", dest="included_entities",
    help="Entities to include", default=None)

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

# this is used to concatenate some columns as a comma-separated
# list of Labels for batch-import (branch Neo4j 2.0)
def concat_translate(*args):
    return """translate(
    array_to_string(
        %s,
        ','),
    ' _"/-', '')""" % (
                "ARRAY[\n            %s]" % ",\n            ".join(
                    ["initcap(%s)" % col
                    for col in args])
            )

class MusicBrainzExporter(SQL2GraphExporter):
    nodes_header_override = {
            "mbid": '"mbid:string:mb_exact"',
            "pk":   '"pk:long:mb_exact"',
            "name": '"name:string:mb_fulltext"',
            "kind": None,
            "type": None,
            "format": None,
            "gender": None,
            "status": None,
            "packaging": None,
            (
                "kind",
                "type",
                "format",
                "gender",
                "status",
                "packaging",
            ): (concat_translate, '"l:label"',),
            #"latitude": '"latitude:float"',
            #"longitude": '"longitude:float"',
            "latitude": None,
            "longitude": None,
            "code": None,
            "pk": None,
            "locale": None,
        }

if all([options.included_entities, options.excluded_entities]):
    raise RuntimeError("You must choose between including or exluding entities")
if options.excluded_entities is not None:
    options.excluded_entities = options.excluded_entities.split(',')
    entities = [e for e in mbentities if e not in options.excluded_entities]

elif options.included_entities is not None:
    options.included_entities = options.included_entities.split(',')
    entities = options.included_entities
    entities += make_link_entity_list(entities)
else:
    entities = mbentities
exporter = MusicBrainzExporter(mbschema, entities, strict=False)

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
