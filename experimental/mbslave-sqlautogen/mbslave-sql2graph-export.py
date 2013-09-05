#!/usr/bin/env python

import os
import sys
from lxml import etree as ET
from mbslave import Config, connect_db
from mbslave.search import fetch_all, fetch_all_fields, fetch_all_relations, generate_iter_query, fetch_all_relations_properties

def indent(s,indentation):
    return "\n".join(["%s%s" % (indentation, line) for line in s.split("\n")])


def generate_union_query(queries):
    return """

UNION

""".join(queries)


def generate_tsvfile_output_query(query, output_filename, modify_headers={}):

    if modify_headers:
        select_lines = ",\n".join(
            ["wrapped.%s AS %s" % (k, v)
                for k, v in modify_headers.iteritems()]
        )
        query= """
SELECT
    %(fields)s
FROM (
    %(query)s
)
AS wrapped
        """ % dict(query=indent(query, '   '), fields=select_lines)
    return """
COPY(
    %(query)s
)
TO '%(filename)s' CSV HEADER DELIMITER E'\\t';
""" % dict(query=indent(query, '   '), filename=output_filename)

# ----------------------------------------------------------------------

cfg = Config(os.path.join(os.path.dirname(__file__), 'mbslave.conf'))
db = connect_db(cfg, True)


all_properties = fetch_all_fields(cfg, db)
#print "--", all_properties


# --- create temporary mapping table

print """
-- Create the mapping table
-- between (entity, pk) tuples and incrementing node IDs
"""

node_queries = []
for columns, joins in fetch_all(cfg, db, [(n,t) for n, t in all_properties if n in ('kind', 'pk')]):
    if columns and joins:
        node_queries.append(generate_iter_query(columns, joins))


mapping_query = """
SELECT
    kind AS entity,
    pk,
    row_number() OVER (ORDER BY kind, pk) as node_id
FROM
(
%s
)
AS entity_union
""" % indent(generate_union_query(node_queries), '    ')


temp_mapping_table = """
DROP TABLE IF EXISTS entity_mapping;

CREATE TEMPORARY TABLE entity_mapping AS
(
%s
);

-- create index to speedup lookups
CREATE INDEX ON entity_mapping (entity, pk);

""" % indent(mapping_query, '    ')

print temp_mapping_table


# --- save the full nodes tables to file


node_queries = []
for columns, joins in fetch_all(cfg, db, all_properties):
    if columns and joins:
        node_queries.append(generate_iter_query(columns, joins))

ordered_union_query = """
%s
ORDER BY kind, pk
""" % generate_union_query(node_queries)

headers = dict([(name, name) for (name, maptype) in all_properties])
headers.update({
        "mbid": '"mbid:string:mbid"',
        "kind": '"kind:string:mbid"',
        "pk":   '"pk:int:mbid"',
        "name": '"name:string:mb"',
    })
print generate_tsvfile_output_query(
    ordered_union_query,
    '/home/paul/tmp/musicbrainz__nodes__full.csv',
    headers)


# ---------------------------------
#sys.exit(0)

rels_queries = []
all_relations_properties = fetch_all_relations_properties(cfg, db)
for relations in fetch_all_relations(cfg, db, all_relations_properties):
    if not relations:
        continue
    for columns, joins in relations:
        rels_queries.append(generate_iter_query(columns, joins))

print generate_tsvfile_output_query(
    generate_union_query(rels_queries), '/home/paul/tmp/musicbrainz__rels__full.csv')

