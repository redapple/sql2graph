#!/usr/bin/env python

import os
import sys
import optparse
from mbslave import Config, connect_db
from mbslave.search import generate_iter_query
from mbslave.search import SchemaHelper, Schema, Column, ForeignColumn, \
    Property, IntegerProperty, Relation, Entity, Reference
from mbslave.search import indent, generate_union_query

# ----------------------------------------------------------------------
class SQL2GraphExporter(object):
    def __init__(self, cfgfilename, schema, entities):
        self.cfg = Config(os.path.join(os.path.dirname(__file__), cfgfilename))
        self.db = connect_db(self.cfg, True)

        self.schema = SchemaHelper(schema, entities)

        self.all_properties = self.schema.fetch_all_fields(self.cfg, self.db)
        self.all_relations_properties = self.schema.fetch_all_relations_properties(self.cfg, self.db)

    def set_nodes_filename(self, filename):
        self.nodes_filename = filename

    def set_rels_filename(self, filename):
        self.relations_filename = filename



    @classmethod
    def generate_tsvfile_output_query(cls, query, output_filename, modify_headers={}):

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
        """ % dict(query=indent(query, '   '), fields=indent(select_lines, '   '))

        return """
COPY(
%(query)s
)
TO '%(filename)s' CSV HEADER DELIMITER E'\\t';
""" % dict(query=indent(query, '   '), filename=output_filename)


    # --- create temporary mapping table
    def create_mapping_table_query(self, multiple=False):
        print """
        -- Create the mapping table
        -- between (entity, pk) tuples and incrementing node IDs
        """

        node_queries = []
        for columns, joins in self.schema.fetch_all(self.cfg, self.db,
                            [(n,t) for n, t in self.all_properties if n in ('kind', 'pk')]):
            if columns and joins:
                node_queries.append(generate_iter_query(columns, joins))


        if multiple:

            query = """
CREATE TEMPORARY TABLE entity_mapping
(
    node_id             SERIAL,
    entity              TEXT,
    pk                  BIGINT
);
"""

            insert_entity_query = """
INSERT INTO entity_mapping
    (entity, pk)
%s
ORDER BY pk;\n"""
            for q in node_queries:
                query += insert_entity_query % indent(q, '    ')

            query += """-- create index to speedup lookups
CREATE INDEX ON entity_mapping (entity, pk);

ANALYZE entity_mapping;
"""
            return query

        else:

            mapping_query = """
SELECT
    kind AS entity,
    pk,
    row_number() OVER (ORDER BY kind, pk) as node_id
FROM
(
%s
)
AS entity_union \n""" % indent(generate_union_query(node_queries), '    ')

            temp_mapping_table = """
DROP TABLE IF EXISTS entity_mapping;

CREATE TEMPORARY TABLE entity_mapping AS
(
%s
);

-- create index to speedup lookups
CREATE INDEX ON entity_mapping (entity, pk);

ANALYZE entity_mapping;

""" % indent(mapping_query, '    ')

            return temp_mapping_table


    # --- save the full nodes tables to file
    def create_nodes_query(self, multiple=False):

        node_queries = []
        for columns, joins in self.schema.fetch_all(self.cfg, self.db,
            self.all_properties if not multiple else []):
            if columns and joins:
                node_queries.append(generate_iter_query(columns, joins))

        headers = dict([(name, name) for (name, maptype) in self.all_properties])
        headers.update({
                "mbid": '"mbid:string:mbid"',
                "kind": '"kind:string:mbid"',
                "pk":   '"pk:int:mbid"',
                "name": '"name:string:mb"',
            })

        if multiple:
            qs = []
            for i, q in enumerate(node_queries, start=1):
                qs.append(
                    self.generate_tsvfile_output_query(
                        """\n%s\nORDER BY pk\n""" % q,
                        self.nodes_filename.replace('.csv', '.%04d.csv' % i),
                        headers)
                )
            return "\n".join(qs)
        else:
            ordered_union_query = """\n%s\nORDER BY kind, pk\n""" % generate_union_query(node_queries)

            return self.generate_tsvfile_output_query(
                ordered_union_query,
                self.nodes_filename,
                headers)


    def create_relationships_query(self, multiple=False):

        rels_queries = []

        if multiple:
            for relations in self.schema.fetch_all_relations(self.cfg, self.db):
                if not relations:
                    continue
                for columns, joins in relations:
                    rels_queries.append(generate_iter_query(columns, joins))
            qs = []
            for i, q in enumerate(rels_queries, start=1):
                qs.append(
                    self.generate_tsvfile_output_query(q,
                        self.relations_filename.replace('.csv', '.%04d.csv' % i)))
            return "\n".join(qs)
        else:
            for relations in self.schema.fetch_all_relations(self.cfg, self.db, self.all_relations_properties):
                if not relations:
                    continue
                for columns, joins in relations:
                    rels_queries.append(generate_iter_query(columns, joins))
            return self.generate_tsvfile_output_query(
                generate_union_query(rels_queries),
                self.relations_filename)

# ----------------------------------------------------------------------
def text_to_rel_type(s):
    return "translate(upper(%s), ' ', '_')" % s

def make_link_entity(start_entity, end_entity):
    return Entity('l_%s_%s' % (start_entity, end_entity),
        fields=[],
        relations = [
            Relation(
                Column('link',
                    ForeignColumn('link', 'link_type',
                        ForeignColumn('link_type', 'name')),
                    function = text_to_rel_type),
                start=Reference(start_entity, Column('entity0')),
                end=Reference(end_entity, Column('entity1'),),
                properties=[])
        ]
    )

entities = [
    'area',
    'area_alias',
    'area_type',
    'artist',
    'artist_alias',
    'artist_type',
    'artist_credit',
    'artist_credit_name',
    'gender',
    'label',
    'label_type',
    'url',
    'release_group',
    'release_group_primary_type',
    'release',
    'release_country',
    'release_packaging',
    'release_status',
    'release_label',
    'recording',
    'track',
    'medium',
    'medium_format',
    'work',
    'work_type',

]

linked_entities = (
        ('area', 'area'),
        ('area', 'artist'),
        ('area', 'label'),
        ('area', 'work'),
        ('area', 'url'),
        ('area', 'recording'),
        ('area', 'release'),
        ('area', 'release_group'),

        ('artist', 'artist'),
        ('artist', 'label'),
        ('artist', 'recording'),
        ('artist', 'release'),
        ('artist', 'release_group'),
        ('artist', 'url'),
        ('artist', 'work'),

        ('label', 'label'),
        ('label', 'recording'),
        ('label', 'release'),
        ('label', 'release_group'),
        ('label', 'url'),
        ('label', 'work'),

        ('recording', 'recording'),
        ('recording', 'release'),
        ('recording', 'release_group'),
        ('recording', 'url'),
        ('recording', 'work'),

        ('release', 'release'),
        ('release', 'release_group'),
        ('release', 'url'),
        ('release', 'work'),

        ('release_group', 'release_group'),
        ('release_group', 'url'),
        ('release_group', 'work'),

        ('url', 'url'),
        ('url', 'work'),

        ('work', 'work'),
)

entities.extend(["l_%s_%s" % (e0, e1) for e0, e1 in linked_entities])

schema = Schema([
    Entity('area_type', [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name')),
        ],
    ),
    Entity('area', [
            IntegerProperty('pk', Column('id')),
            Property('mbid', Column('gid')),
            Property('name', Column('name')),
            #Property('type', Column('type', ForeignColumn('area_type', 'name', null=True))),
        ],
        [
            Relation(
                'AREA_TYPE',
                start=Reference('area', Column('id')),
                end=Reference('area_type', Column('type')),
                properties=[]
            ),
        ],
    ),
    Entity(
        'area_alias',
        [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name')),
            Property('type', Column('type', ForeignColumn('area_alias_type', 'name', null=True))),
            Property('locale', Column('locale')),
        ],
        [
            Relation(
                'HAS_ALIAS',
                start=Reference('area', Column('area')),
                end=Reference('area_alias', Column('id')),
                properties=[]
            ),
        ]
    ),
    Entity('artist',
        [
            IntegerProperty('pk', Column('id')),
            Property('mbid', Column('gid')),
            Property('disambiguation', Column('comment')),
            Property('name', Column('name', ForeignColumn('artist_name', 'name'))),
            #Property('sort_name', Column('sort_name', ForeignColumn('artist_name', 'name'))),
            #Property('country', Column('country', ForeignColumn('country', 'name', null=True))),
            #Property('country', Column('country', ForeignColumn('country', 'iso_code', null=True))),
            #Property('gender', Column('gender', ForeignColumn('gender', 'name', null=True))),
            #Property('type', Column('type', ForeignColumn('artist_type', 'name', null=True))),
        ],
        [
            Relation(
                'FROM',
                start=Reference('artist', Column('id')),
                end=Reference('area', Column('area')),
                properties=[]
            ),
            Relation(
                'BEGAN_IN',
                start=Reference('artist', Column('id')),
                end=Reference('area', Column('begin_area')),
                properties=[]
            ),
            Relation(
                'ENDED_IN',
                start=Reference('artist', Column('id')),
                end=Reference('area', Column('end_area')),
                properties=[]
            ),
            Relation(
                'HAS_GENDER',
                start=Reference('artist', Column('id')),
                end=Reference('gender', Column('gender')),
                properties=[]
            ),
            Relation(
                'ARTIST_TYPE',
                start=Reference('artist', Column('id')),
                end=Reference('artist_type', Column('type')),
                properties=[]
            ),
        ],
    ),
    Entity('artist_alias',
        [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name', ForeignColumn('artist_name', 'name'))),
            Property('type', Column('type', ForeignColumn('artist_alias_type', 'name', null=True))),
        ],
        [
            Relation(
                'HAS_ALIAS',
                start=Reference('artist', Column('artist')),
                end=Reference('artist_alias', Column('id')),
                properties=[]
            ),
        ]
    ),
    Entity('artist_type',
        [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name')),
        ],
    ),
    Entity('artist_credit',
        fields = [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name', ForeignColumn('artist_name', 'name'))),
        ]
    ),
    Entity('artist_credit_name',
        fields=[],
        relations = [
            Relation(
                'CREDITED_AS',
                start=Reference('artist', Column('artist')),
                end=Reference('artist_credit', Column('artist_credit')),
                properties=[
                    IntegerProperty('position', Column('position')),
                    Property('join', Column('join_phrase')),
                ]
            ),
        ]
    ),
    Entity('gender', [
        IntegerProperty('pk', Column('id')),
        Property('name', Column('name')),
    ]),
    Entity('label',
        [
            IntegerProperty('pk', Column('id')),
            Property('mbid', Column('gid')),
            Property('disambiguation', Column('comment')),
            IntegerProperty('code', Column('label_code')),
            Property('name', Column('name', ForeignColumn('label_name', 'name'))),
            #Property('sort_name', Column('sort_name', ForeignColumn('label_name', 'name'))),
            #Property('country', Column('country', ForeignColumn('country', 'name', null=True))),
            #Property('country', Column('country', ForeignColumn('country', 'iso_code', null=True))),
            #Property('type', Column('type', ForeignColumn('label_type', 'name', null=True))),
        ],
        [
            Relation(
                'LABEL_TYPE',
                start=Reference('label', Column('id')),
                end=Reference('label_type', Column('type')),
                properties=[]
            ),
            Relation(
                'FROM',
                start=Reference('label', Column('id')),
                end=Reference('area', Column('area')),
                properties=[]
            ),
        ],
    ),
    Entity('label_type',
        [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name')),
        ]
    ),
    Entity('work',
        [
            IntegerProperty('pk', Column('id')),
            Property('mbid', Column('gid')),
            Property('disambiguation', Column('comment')),
            Property('name', Column('name', ForeignColumn('work_name', 'name'))),
            #Property('type', Column('type', ForeignColumn('work_type', 'name', null=True))),
        ],
        [
            Relation(
                'WORK_TYPE',
                start=Reference('work', Column('id')),
                end=Reference('work_type', Column('type')),
                properties=[]
            ),
        ]
    ),
    Entity('work_type',
        [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name')),
        ]
    ),
    Entity('release_group',
        [
            IntegerProperty('pk', Column('id')),
            Property('mbid', Column('gid')),
            Property('disambiguation', Column('comment')),
            Property('name', Column('name', ForeignColumn('release_name', 'name'))),
            #Property('type', Column('type', ForeignColumn('release_group_primary_type', 'name', null=True))),
            #Property('artist', Column('artist_credit', ForeignColumn('artist_credit', 'name', ForeignColumn('artist_name', 'name')))),
        ],
        [
            Relation(
                'RELEASE_GROUP_TYPE',
                start=Reference('release_group', Column('id')),
                end=Reference('release_group_primary_type', Column('type')),
                properties=[]
            ),
            Relation(
                'CREDITED_ON',
                start=Reference('artist_credit', Column('artist_credit')),
                end=Reference('release_group', Column('id')),
                properties=[]
            ),
        ]
    ),
    Entity('release_group_primary_type',
        [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name')),
        ]
    ),
    Entity('release',
        [
            IntegerProperty('pk', Column('id')),
            Property('mbid', Column('gid')),
            Property('disambiguation', Column('comment')),
            #Property('barcode', Column('barcode')),
            Property('name', Column('name', ForeignColumn('release_name', 'name'))),
            #Property('status', Column('status', ForeignColumn('release_status', 'name', null=True))),
            #Property('type', Column('release_group', ForeignColumn('release_group', 'type', ForeignColumn('release_group_primary_type', 'name', null=True)))),
            #Property('artist', Column('artist_credit', ForeignColumn('artist_credit', 'name', ForeignColumn('artist_name', 'name')))),
            #Property('country', Column('country', ForeignColumn('country', 'name', null=True))),
            #Property('country', Column('country', ForeignColumn('country', 'iso_code', null=True))),
            #Property('alias', Column('release_group', ForeignColumn('release_group', 'name', ForeignColumn('release_name', 'name')))),
        ],
        [
            Relation(
                'HAS_STATUS',
                start=Reference('release', Column('id')),
                end=Reference('release_status', Column('status')),
                properties=[]
            ),
            Relation(
                'CREDITED_ON',
                start=Reference('artist_credit', Column('artist_credit')),
                end=Reference('release', Column('id')),
                properties=[]
            ),
            Relation(
                'PART_OF',
                start=Reference('release', Column('id')),
                end=Reference('release_group', Column('release_group')),
                properties=[]
            ),
            Relation(
                'PACKAGING',
                start=Reference('release', Column('id')),
                end=Reference('release_packaging', Column('packaging')),
                properties=[]
            ),
        ]
    ),
    Entity('release_status',
        [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name')),
        ]
    ),
    Entity('release_label',
        [],
        [
            Relation(
                'RELEASED_ON',
                start=Reference('release', Column('release')),
                end=Reference('label', Column('label')),
                properties=[
                    Property('catalog_number', Column('catalog_number')),
                ]
            ),
        ]
    ),
    Entity('release_packaging',
        [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name')),
        ]
    ),
    Entity('release_country',
        # do not create nodes
        [],
        [
            Relation(
                'RELEASED_IN',
                start=Reference('release', Column('release')),
                end=Reference('area',
                                Column('country',
                                    ForeignColumn('country_area', 'area'))),
                properties=[
                    Property('year', Column('date_year')),
                    Property('month', Column('date_month')),
                    Property('day', Column('date_day')),
                ]
            ),
        ]
    ),
    Entity('medium',
        [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name')),
        ],
        [
            Relation(
                'MEDIUM',
                start=Reference('release', Column('release')),
                end=Reference('medium', Column('id')),
                properties=[]
            ),
            Relation(
                'HAS_FORMAT',
                start=Reference('medium', Column('id')),
                end=Reference('medium_format', Column('format')),
                properties=[]
            ),
        ]
    ),
    Entity('medium_format',
        [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name')),
        ],
        [
            Relation(
                'PARENT_FORMAT',
                start=Reference('medium_format', Column('id')),
                end=Reference('medium_format', Column('parent')),
                properties=[]
            ),
        ]
    ),
    Entity('recording',
        [
            IntegerProperty('pk', Column('id')),
            Property('mbid', Column('gid')),
            Property('disambiguation', Column('comment')),
            Property('name', Column('name', ForeignColumn('track_name', 'name'))),
            #Property('artist', Column('artist_credit', ForeignColumn('artist_credit', 'name', ForeignColumn('artist_name', 'name')))),
        ],
        [
            Relation(
                'CREDITED_ON',
                start=Reference('artist_credit', Column('artist_credit')),
                end=Reference('recording', Column('id')),
                properties=[]
            ),
        ]
    ),
    Entity('track',
        [
            IntegerProperty('pk', Column('id')),
            Property('mbid', Column('gid')),
            Property('name', Column('name', ForeignColumn('track_name', 'name'))),
        ],
        [
            Relation(
                'IS_RECORDING',
                start=Reference('track', Column('id')),
                end=Reference('recording', Column('recording')),
                properties=[]
            ),
            Relation(
                'APPEARS_ON',
                start=Reference('track', Column('id')),
                end=Reference('medium', Column('medium')),
                properties=[]
            ),
            Relation(
                'CREDITED_ON',
                start=Reference('artist_credit', Column('artist_credit')),
                end=Reference('track', Column('id')),
                properties=[]
            ),
        ],
    ),
    Entity('url',
        [
            IntegerProperty('pk', Column('id')),
            Property('mbid', Column('gid')),
            Property('name', Column('url')),
        ],
    ),
    ]
    +
    [make_link_entity(e0, e1) for (e0, e1) in linked_entities]
)


# --------------------

option_parser = optparse.OptionParser()
option_parser.add_option("--nodes", dest="nodes_filename",
    help="Nodes file", default='/tmp/musicbrainz__nodes__full.csv')
option_parser.add_option("--relations", dest="relations_filename",
    help="Relationships file", default='/tmp/musicbrainz__rels__full.csv')
option_parser.add_option("--multiple", action="store_true",
    dest="multiple_files",
    help="whether to output multiple nodes files and relationships files",
    default=False)
(options, args) = option_parser.parse_args()


exporter = SQL2GraphExporter('mbslave.conf', schema, entities)
exporter.set_nodes_filename(options.nodes_filename)
exporter.set_rels_filename(options.relations_filename)

print exporter.create_mapping_table_query(multiple=options.multiple_files)
print exporter.create_nodes_query(multiple=options.multiple_files)
print exporter.create_relationships_query(multiple=options.multiple_files)
