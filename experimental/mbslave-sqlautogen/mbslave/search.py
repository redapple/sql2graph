import itertools
from collections import namedtuple
from lxml.builder import E

#Entity = namedtuple('Entity', ['name', 'fields', 'relations'])
DefaultField = namedtuple('Field', ['name', 'column'])
Relation = namedtuple('Relation', ['rtype', 'start', 'end', 'properties'])
Reference = namedtuple('Reference', ['entity', 'key_column'])
#MultiField = namedtuple('MultiField', ['name', 'column'])


class Field(DefaultField):
    maptype = str


class IntegerField(Field):
    maptype = int


class BooleanField(Field):
    maptype = bool


class Schema(object):

    def __init__(self, entities):
        self.entities = entities
        self.entities_by_id = dict((e.name, e) for e in entities)

    def __getitem__(self, name):
        return self.entities_by_id[name]


class Entity(object):

    def __init__(self, name, fields=[], relations=[]):
        self.name = name
        self.fields = fields
        self.relations = relations
        self.fields_by_name = dict(((field.name, field) for field in fields))

    def iter_relations(self, name=None):
        for rel in self.relations:
            if name is not None and rel.name != name:
                continue
            yield rel

    def iter_single_fields(self, name=None):
        for field in self.fields:
            if isinstance(field, Field):
                if name is not None and field.name != name:
                    continue
                yield field

    def iter_multi_fields(self, name=None):
        for field in self.fields:
            if isinstance(field, MultiField):
                if name is not None and field.name != name:
                    continue
                yield field


class Column(object):

    def __init__(self, name, foreign=None):
        self.name = name
        self.foreign = foreign


class ForeignColumn(Column):

    def __init__(self, table, name, foreign=None, null=False, backref=None):
        super(ForeignColumn, self).__init__(name, foreign=foreign)
        self.table = table
        self.null = null
        self.backref = backref

def make_link_entity(start_entity, end_entity):
    return Entity('l_%s_%s' % (start_entity, end_entity),
        fields=[],
        relations = [
            Relation(
                Column('link',
                    ForeignColumn('link', 'link_type',
                        ForeignColumn('link_type', 'name'))),
                start=Reference(start_entity, Column('entity0')),
                end=Reference(end_entity, Column('entity1'),),
                properties=[])
        ]
    )

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
        ]
    ),
    Entity('label_type',
        [
            IntegerField('pk', Column('id')),
            Field('name', Column('name')),
        ]
    ),
    Entity('work', [
        Field('mbid', Column('gid')),
        Field('disambiguation', Column('comment')),
        Field('name', Column('name', ForeignColumn('work_name', 'name'))),
        Field('type', Column('type', ForeignColumn('work_type', 'name', null=True))),
        #MultiField('mbid', ForeignColumn('work_gid_redirect', 'gid', backref='new_id')),
        #MultiField('iswc', ForeignColumn('iswc', 'iswc')),
        #MultiField('alias', ForeignColumn('work_alias', 'name', ForeignColumn('work_name', 'name'))),
    ]),
    Entity('release_group', [
        Field('mbid', Column('gid')),
        Field('disambiguation', Column('comment')),
        Field('name', Column('name', ForeignColumn('release_name', 'name'))),
        Field('type', Column('type', ForeignColumn('release_group_primary_type', 'name', null=True))),
        #MultiField('mbid', ForeignColumn('release_group_gid_redirect', 'gid', backref='new_id')),
        #MultiField('type',
            #ForeignColumn('release_group_secondary_type_join', 'secondary_type',
                #ForeignColumn('release_group_secondary_type', 'name'))),
        #Field('artist', Column('artist_credit', ForeignColumn('artist_credit', 'name', ForeignColumn('artist_name', 'name')))),
        #MultiField('alias', ForeignColumn('release', 'name', ForeignColumn('release_name', 'name'))),
    ]),
    Entity('release', [
        Field('mbid', Column('gid')),
        Field('disambiguation', Column('comment')),
        #Field('barcode', Column('barcode')),
        Field('name', Column('name', ForeignColumn('release_name', 'name'))),
        Field('status', Column('status', ForeignColumn('release_status', 'name', null=True))),
        Field('type', Column('release_group', ForeignColumn('release_group', 'type', ForeignColumn('release_group_primary_type', 'name', null=True)))),
        #Field('artist', Column('artist_credit', ForeignColumn('artist_credit', 'name', ForeignColumn('artist_name', 'name')))),
        #Field('country', Column('country', ForeignColumn('country', 'name', null=True))),
        #Field('country', Column('country', ForeignColumn('country', 'iso_code', null=True))),
        #MultiField('mbid', ForeignColumn('release_gid_redirect', 'gid', backref='new_id')),
        #MultiField('catno', ForeignColumn('release_label', 'catalog_number')),
        #MultiField('label', ForeignColumn('release_label', 'label', ForeignColumn('label', 'name', ForeignColumn('label_name', 'name')))),
        #Field('alias', Column('release_group', ForeignColumn('release_group', 'name', ForeignColumn('release_name', 'name')))),
    ]),
    Entity('recording', [
        Field('mbid', Column('gid')),
        Field('disambiguation', Column('comment')),
        Field('name', Column('name', ForeignColumn('track_name', 'name'))),
        #Field('artist', Column('artist_credit', ForeignColumn('artist_credit', 'name', ForeignColumn('artist_name', 'name')))),
        #MultiField('mbid', ForeignColumn('recording_gid_redirect', 'gid', backref='new_id')),
        #MultiField('alias', ForeignColumn('track', 'name', ForeignColumn('track_name', 'name'))),
    ]),
    Entity('url',
        [
            IntegerField('pk', Column('id')),
            Field('mbid', Column('gid')),
            Field('name', Column('url')),
        ],
    ),
    # link_artist_*
    make_link_entity('artist', 'artist'),
    make_link_entity('artist', 'label'),
    make_link_entity('artist', 'url'),
    make_link_entity('label', 'url'),
    #make_link_entity('link_artist_recording',
        #'artist', 'artist_fk',
        #'recording', 'recording_fk'),
    #make_link_entity('link_artist_release',
        #'artist', 'artist_fk',
        #'release', 'release_fk'),
    #make_link_entity('link_artist_release_group',
        #'artist', 'artist_fk',
        #'release_group', 'release_group_fk'),
    #make_link_entity('link_artist_url',
        #'artist', 'artist_fk',
        #'url', 'url_fk'),
    #make_link_entity('link_artist_work',
        #'artist', 'artist_fk',
        #'work', 'work_fk'),
])


SQL_SELECT_TPL = "SELECT\n%(columns)s\nFROM\n%(joins)s\nORDER BY %(sort_column)s"


SQL_TRIGGER = """
CREATE OR REPLACE FUNCTION mbslave_solr_%(op1)s_%(table)s() RETURNS trigger AS $$
BEGIN
    %(code)s
    RETURN NULL;
END;
$$ LANGUAGE 'plpgsql';

DROP TRIGGER IF EXISTS mbslave_solr_tr_%(op1)s_%(table)s ON musicbrainz.%(table)s;
CREATE TRIGGER mbslave_solr_tr_%(op1)s_%(table)s AFTER %(op2)s ON musicbrainz.%(table)s FOR EACH ROW EXECUTE PROCEDURE mbslave_solr_%(op1)s_%(table)s();
"""


def distinct_values(columns):
    return ' OR\n       '.join('OLD.%(c)s IS DISTINCT FROM NEW.%(c)s' % dict(c=c)
                               for c in columns)


def generate_trigger_update(path):
    condition = None
    for table, column in path[1:]:
        if not condition:
            condition = 'FROM musicbrainz.%s WHERE %s = NEW.id' % (table, column)
        else:
            condition = 'FROM musicbrainz.%s WHERE %s IN (SELECT id %s)' % (table, column, condition)
    return path[0][0], path[0][1], condition




def generate_iter_query(columns, joins, ids=()):
    if not columns or not joins:
        return ""

    id_column = columns[0]
    tpl = ["SELECT", "%(columns)s", "FROM", "%(joins)s"]
    if ids:
        tpl.append("WHERE %(id_column)s IN (%(ids)s)")
    #tpl.append("ORDER BY %(id_column)s")
    sql_columns = ',\n'.join('  %s' % (i, ) for i in columns)
    sql_joins = '\n'.join('  ' + i for i in joins)
    sql = "\n".join(tpl) % dict(columns=sql_columns, joins=sql_joins,
                                id_column=id_column, ids=placeholders(ids))
    return sql


def iter_entity_nodes(db, kind, properties=[]):
    entity = schema[kind]
    if not entity.fields:
        return [], []
    joins = [kind]
    tables = set([kind])
    #columns = ['%s.id' % (kind,)]
    columns = ["'%s' as kind" % (kind,)]
    names = []

    for p, ptype in properties:
        if not entity.fields_by_name.get(p):
            if ptype == int:
                columns.append('0 AS "%s"' % p)
            else:
                columns.append('NULL AS "%s"' % p)
            continue
        field = entity.fields_by_name.get(p)
        table = kind
        column = field.column
        while column.foreign is not None:
            foreign_table = table + '__' + column.name + '__' + column.foreign.table
            if foreign_table not in tables:
                join = 'LEFT JOIN' if column.foreign.null else 'JOIN'
                joins.append('%(join)s %(parent)s AS %(label)s ON %(label)s.id = %(child)s.%(child_column)s' % dict(
                    join=join, parent=column.foreign.table, child=table, child_column=column.name, label=foreign_table))
                tables.add(foreign_table)
            table = foreign_table
            column = column.foreign

        columns.append('%s.%s%s' % (table, column.name, ' AS "%s"' % field.name if field.name else ''))
        names.append(field.name)

    return columns, joins


def iter_entity_relations(db, kind, properties=[]):
    entity = schema[kind]

    tables = set([kind])
    fields = []
    #columns = ['%s.id' % (kind,)]
    names = []
    relations = list(entity.iter_relations())

    output_relations = []
    for rel in relations:

        columns = []
        joins = [kind]

        columns.append('start_entity.node_id AS start')
        columns.append('end_entity.node_id AS end')

        if isinstance(rel.rtype, Column):
            table = kind
            column = rel.rtype

            while column.foreign is not None:
                foreign_table = table + '__' + column.name + '__' + column.foreign.table
                if foreign_table not in tables:
                    join = 'LEFT JOIN' if column.foreign.null else 'JOIN'
                    joins.append('%(join)s %(parent)s AS %(label)s ON %(label)s.id = %(child)s.%(child_column)s' % dict(
                        join=join, parent=column.foreign.table, child=table, child_column=column.name, label=foreign_table))
                    tables.add(foreign_table)
                table = foreign_table
                column = column.foreign

            columns.append('%s.%s AS rel_type' % (table, column.name))
        else:
            columns.append("'%s' AS rel_type" % rel.rtype)

        # FIXME: add other properties

        # start/end entities
        start, end = rel.start, rel.end

        start_entity, end_entity = start.entity, end.entity
        start_column, end_column = start.key_column, end.key_column

        joins.append("""
JOIN entity_mapping start_entity
    ON (
        start_entity.pk = %(kind)s.%(start_column)s
        AND
        start_entity.entity = '%(start_entity)s'
    )""" % dict(start_entity=start_entity, start_column=start_column.name, kind=kind))
        joins.append("""
JOIN entity_mapping end_entity
    ON (
        end_entity.pk = %(kind)s.%(end_column)s
        AND
        end_entity.entity = '%(end_entity)s'
    )""" % dict(end_entity=end_entity, end_column=end_column.name, kind=kind))




        output_relations.append((columns, joins))

    return output_relations


def iter_sub(db, kind, subtable, ids=()):
    entity = schema[kind]
    joins = []
    tables = set()
    columns = []
    names = []
    for field in entity.iter_multi_fields():
        if field.column.table != subtable:
            continue
        last_column = column = field.column
        table = column.table
        while True:
            if last_column is column:
                if table not in tables:
                    joins.append(table)
                    tables.add(table)
                    columns.append('%s.%s' % (table, column.backref or kind))
            else:
                foreign_table = table + '__' + last_column.name + '__' + column.table
                if foreign_table not in tables:
                    join = 'LEFT JOIN' if column.null else 'JOIN'
                    joins.append('%(join)s %(parent)s AS %(label)s ON %(label)s.id = %(child)s.%(child_column)s' % dict(
                        join=join, parent=column.table, child=table, child_column=last_column.name, label=foreign_table))
                    tables.add(foreign_table)
                table = foreign_table
            if column.foreign is None:
                break
            last_column = column
            column = column.foreign
        columns.append('%s.%s' % (table, column.name))
        names.append(field.name)

    query = generate_iter_query(columns, joins, ids)
    cursor = db.cursor('cursor_' + kind + '_' + subtable)
    cursor.itersize = 100 * 1000
    cursor.execute(query, ids)
    fields = []
    last_id = None
    for row in cursor:
        id = row[0]
        if last_id != id:
            if fields:
                yield last_id, fields
            last_id = id
            fields = []
        for name, value in zip(names, row[1:]):
            if not value:
                continue
            if isinstance(value, str):
                value = value.decode('utf8')
            elif not isinstance(value, unicode):
                value = unicode(value)
            try:
                fields.append(E.field(value, name=name))
            except ValueError:
                continue # XXX
    if fields:
        yield last_id, fields


def placeholders(ids):
    return ", ".join(["%s" for i in ids])


def grab_next(iter):
    try:
        return iter.next()
    except StopIteration:
        return None


def merge(main, *extra):
    current = map(grab_next, extra)
    for id, fields in main:
        for i, extra_item in enumerate(current):
            if extra_item is not None:
                if extra_item[0] == id:
                    fields.extend(extra_item[1])
                    current[i] = grab_next(extra[i])
        yield id, E.doc(*fields)


def fetch_entities(db, kind, properties=[]):
    return iter_entity_nodes(db, kind, properties)

def fetch_relations(db, kind, properties=[]):
    return iter_entity_relations(db, kind, properties)

def fetch_areas(db, ids=()):
    return fetch_entities(db, 'area', ids)


def fetch_artists(db, ids=()):
    return fetch_entities(db, 'artist', ids)


def fetch_labels(db, ids=()):
    return fetch_entities(db, 'label', ids)


def fetch_release_groups(db, ids=()):
    return fetch_entities(db, 'release_group', ids)


def fetch_recordings(db, ids=()):
    return fetch_entities(db, 'recording', ids)


def fetch_releases(db, ids=()):
    return fetch_entities(db, 'release', ids)


def fetch_works(db, ids=()):
    return fetch_entities(db, 'work', ids)


gentities = (
    'area',
    'area_alias',
    'area_type',
    'artist',
    'artist_alias',
    'artist_type',
    'gender',
    'label',
    'url',
    #'release_group',
    #'release',
    'l_artist_artist',
    'l_artist_label',
    'l_artist_url',
    'l_label_url',
)
def fetch_all_fields(cfg, db):
    fields = set([])
    for kind in gentities:
        entity = schema[kind]
        fields.update(
            set([(f.name, f.maptype) for f in entity.iter_single_fields()])
        )
    return list(fields)

def fetch_all_relations_properties(cfg, db):
    relations = set([])
    for kind in gentities:
        entity = schema[kind]
        relations.update(
            set([p for r in entity.iter_relations() for p in r.properties])
        )
    return list(relations)

def fetch_all(cfg, db, properties):
    for entity in gentities:
        yield fetch_entities(db, entity, properties)

def fetch_all_relations(cfg, db, properties):
    for entity in gentities:
        yield fetch_relations(db, entity, properties)

