#!/usr/bin/env python

import os
import sys
from lxml import etree as ET
from mbslave import Config, connect_db
from mbslave.search import generate_iter_query
from mbslave.search import SchemaHelper, Schema, Column, ForeignColumn, \
    Field, IntegerField, Relation, Entity, Reference
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
        for columns, joins in self.schema.fetch_all(self.cfg, self.db, self.all_properties):
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

        for relations in self.schema.fetch_all_relations(self.cfg, self.db, self.all_relations_properties):
            if not relations:
                continue
            for columns, joins in relations:
                rels_queries.append(generate_iter_query(columns, joins))

        if multiple:
            qs = []
            for i, q in enumerate(rels_queries, start=1):
                qs.append(
                    self.generate_tsvfile_output_query(q,
                        self.relations_filename.replace('.csv', '.%04d.csv' % i)))
            return "\n".join(qs)
        else:
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
                    function = text_to_rel_type)
                        ,
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
            IntegerField('pk', Column('id')),
            Field('name', Column('name')),
        ],
    ),
    Entity('area', [
            IntegerField('pk', Column('id')),
            Field('mbid', Column('gid')),
            Field('name', Column('name')),
            #Field('type', Column('type', ForeignColumn('area_type', 'name', null=True))),
        ],
        [
            Relation(
                'OF_TYPE',
                start=Reference('area', Column('id')),
                end=Reference('area_type', Column('type')),
                properties=[]
            ),
        ],
    ),
    Entity(
        'area_alias',
        [
            IntegerField('pk', Column('id')),
            Field('name', Column('name')),
            Field('type', Column('type', ForeignColumn('area_alias_type', 'name', null=True))),
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
            IntegerField('pk', Column('id')),
            Field('mbid', Column('gid')),
            Field('disambiguation', Column('comment')),
            Field('name', Column('name', ForeignColumn('artist_name', 'name'))),
            #Field('sort_name', Column('sort_name', ForeignColumn('artist_name', 'name'))),
            #Field('country', Column('country', ForeignColumn('country', 'name', null=True))),
            #Field('country', Column('country', ForeignColumn('country', 'iso_code', null=True))),
            #Field('gender', Column('gender', ForeignColumn('gender', 'name', null=True))),
            #Field('type', Column('type', ForeignColumn('artist_type', 'name', null=True))),
            #MultiField('mbid', ForeignColumn('artist_gid_redirect', 'gid', backref='new_id')),
            #MultiField('ipi', ForeignColumn('artist_ipi', 'ipi')),
            #MultiField('alias', ForeignColumn('artist_alias', 'name', ForeignColumn('artist_name', 'name'))),
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
                'OF_TYPE',
                start=Reference('artist', Column('id')),
                end=Reference('artist_type', Column('type')),
                properties=[]
            ),
        ],
    ),
    Entity('artist_alias',
        [
            IntegerField('pk', Column('id')),
            Field('name', Column('name', ForeignColumn('artist_name', 'name'))),
            Field('type', Column('type', ForeignColumn('artist_alias_type', 'name', null=True))),
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
            IntegerField('pk', Column('id')),
            Field('name', Column('name')),
        ],
    ),
    Entity('artist_credit',
        fields = [
            IntegerField('pk', Column('id')),
            Field('name', Column('name', ForeignColumn('artist_name', 'name'))),
        ]
    ),
    Entity('artist_credit_name',
        fields=[],
        relations = [
            Relation(
                'CREDITED_AS',
                start=Reference('artist', Column('artist')),
                end=Reference('artist_credit', Column('artist_credit')),
                properties=[]
            ),
        ]
    ),
    Entity('gender', [
        IntegerField('pk', Column('id')),
        Field('name', Column('name')),
    ]),
    Entity('label',
        [
            IntegerField('pk', Column('id')),
            Field('mbid', Column('gid')),
            Field('disambiguation', Column('comment')),
            IntegerField('code', Column('label_code')),
            Field('name', Column('name', ForeignColumn('label_name', 'name'))),
            #Field('sort_name', Column('sort_name', ForeignColumn('label_name', 'name'))),
            #Field('country', Column('country', ForeignColumn('country', 'name', null=True))),
            #Field('country', Column('country', ForeignColumn('country', 'iso_code', null=True))),
            #Field('type', Column('type', ForeignColumn('label_type', 'name', null=True))),
            #MultiField('mbid', ForeignColumn('label_gid_redirect', 'gid', backref='new_id')),
            #MultiField('ipi', ForeignColumn('label_ipi', 'ipi')),
            #MultiField('alias', ForeignColumn('label_alias', 'name', ForeignColumn('label_name', 'name'))),
        ],
        [
            Relation(
                'OF_TYPE',
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
            IntegerField('pk', Column('id')),
            Field('name', Column('name')),
        ]
    ),
    Entity('work',
        [
            IntegerField('pk', Column('id')),
            Field('mbid', Column('gid')),
            Field('disambiguation', Column('comment')),
            Field('name', Column('name', ForeignColumn('work_name', 'name'))),
            #Field('type', Column('type', ForeignColumn('work_type', 'name', null=True))),
            #MultiField('mbid', ForeignColumn('work_gid_redirect', 'gid', backref='new_id')),
            #MultiField('iswc', ForeignColumn('iswc', 'iswc')),
            #MultiField('alias', ForeignColumn('work_alias', 'name', ForeignColumn('work_name', 'name'))),
        ],
        [
            Relation(
                'OF_TYPE',
                start=Reference('work', Column('id')),
                end=Reference('work_type', Column('type')),
                properties=[]
            ),
        ]
    ),
    Entity('work_type',
        [
            IntegerField('pk', Column('id')),
            Field('name', Column('name')),
        ]
    ),
    Entity('release_group',
        [
            IntegerField('pk', Column('id')),
            Field('mbid', Column('gid')),
            Field('disambiguation', Column('comment')),
            Field('name', Column('name', ForeignColumn('release_name', 'name'))),
            #Field('type', Column('type', ForeignColumn('release_group_primary_type', 'name', null=True))),
            #MultiField('mbid', ForeignColumn('release_group_gid_redirect', 'gid', backref='new_id')),
            #MultiField('type',
                #ForeignColumn('release_group_secondary_type_join', 'secondary_type',
                    #ForeignColumn('release_group_secondary_type', 'name'))),
            #Field('artist', Column('artist_credit', ForeignColumn('artist_credit', 'name', ForeignColumn('artist_name', 'name')))),
            #MultiField('alias', ForeignColumn('release', 'name', ForeignColumn('release_name', 'name'))),
        ],
        [
            Relation(
                'OF_TYPE',
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
            IntegerField('pk', Column('id')),
            Field('name', Column('name')),
        ]
    ),
    Entity('release',
        [
            IntegerField('pk', Column('id')),
            Field('mbid', Column('gid')),
            Field('disambiguation', Column('comment')),
            #Field('barcode', Column('barcode')),
            Field('name', Column('name', ForeignColumn('release_name', 'name'))),
            #Field('status', Column('status', ForeignColumn('release_status', 'name', null=True))),
            #Field('type', Column('release_group', ForeignColumn('release_group', 'type', ForeignColumn('release_group_primary_type', 'name', null=True)))),
            #Field('artist', Column('artist_credit', ForeignColumn('artist_credit', 'name', ForeignColumn('artist_name', 'name')))),
            #Field('country', Column('country', ForeignColumn('country', 'name', null=True))),
            #Field('country', Column('country', ForeignColumn('country', 'iso_code', null=True))),
            #MultiField('mbid', ForeignColumn('release_gid_redirect', 'gid', backref='new_id')),
            #MultiField('catno', ForeignColumn('release_label', 'catalog_number')),
            #MultiField('label', ForeignColumn('release_label', 'label', ForeignColumn('label', 'name', ForeignColumn('label_name', 'name')))),
            #Field('alias', Column('release_group', ForeignColumn('release_group', 'name', ForeignColumn('release_name', 'name')))),
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
            IntegerField('pk', Column('id')),
            Field('name', Column('name')),
        ]
    ),
    Entity('release_label',
        [],
        [
            Relation(
                'RELEASED_ON',
                start=Reference('release', Column('release')),
                end=Reference('label', Column('label')),
                properties=[]
            ),
        ]
    ),
    Entity('release_packaging',
        [
            IntegerField('pk', Column('id')),
            Field('name', Column('name')),
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
                properties=[]
            ),
        ]
    ),
    Entity('medium',
        # do not create nodes
        [
            IntegerField('pk', Column('id')),
            Field('name', Column('name')),
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
            IntegerField('pk', Column('id')),
            Field('name', Column('name')),
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
            IntegerField('pk', Column('id')),
            Field('mbid', Column('gid')),
            Field('disambiguation', Column('comment')),
            Field('name', Column('name', ForeignColumn('track_name', 'name'))),
            #Field('artist', Column('artist_credit', ForeignColumn('artist_credit', 'name', ForeignColumn('artist_name', 'name')))),
            #MultiField('mbid', ForeignColumn('recording_gid_redirect', 'gid', backref='new_id')),
            #MultiField('alias', ForeignColumn('track', 'name', ForeignColumn('track_name', 'name'))),
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
            IntegerField('pk', Column('id')),
            Field('mbid', Column('gid')),
            Field('name', Column('name', ForeignColumn('track_name', 'name'))),
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
            IntegerField('pk', Column('id')),
            Field('mbid', Column('gid')),
            Field('name', Column('url')),
        ],
    ),
    ]
    +
    [make_link_entity(e0, e1) for (e0, e1) in linked_entities]
)


# --------------------
nodes_filename = '/tmp/musicbrainz__nodes__full.csv'
relations_filename = '/tmp/musicbrainz__rels__full.csv'

exporter = SQL2GraphExporter('mbslave.conf', schema, entities)
exporter.set_nodes_filename(nodes_filename)
exporter.set_rels_filename(relations_filename)

multiple=False
print exporter.create_mapping_table_query(multiple=multiple)
print exporter.create_nodes_query(multiple=multiple)

print exporter.create_relationships_query(multiple=False)
