import itertools
from collections import namedtuple

DefaultProperty = namedtuple('DefaultProperty', ['name', 'column'])
Relation = namedtuple('Relation', ['rtype', 'start', 'end', 'properties'])
Reference = namedtuple('Reference', ['entity', 'key_column'])

class Property(DefaultProperty):
    maptype = str


class IntegerProperty(Property):
    maptype = int


class BooleanProperty(Property):
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
            if isinstance(field, Property):
                if name is not None and field.name != name:
                    continue
                yield field


class Column(object):

    def __init__(self, name, foreign=None, function=None):
        self.name = name
        self.foreign = foreign
        self.function = function


class ForeignColumn(Column):

    def __init__(self, table, name, foreign=None, null=False, backref=None):
        super(ForeignColumn, self).__init__(name, foreign=foreign)
        self.table = table
        self.null = null
        self.backref = backref



def placeholders(ids):
    return ", ".join(["%s" for i in ids])


def generate_iter_query(columns, joins, ids=(), limit=None, order_by=None):
    if not columns or not joins:
        return ""

    id_column = columns[0]
    tpl = ["SELECT", "%(columns)s", "FROM", "%(joins)s"]
    if ids:
        tpl.append("WHERE %(id_column)s IN (%(ids)s)")
    #tpl.append("ORDER BY %(id_column)s")
    if order_by is not None:
        tpl.append(u"ORDER BY %s" % order_by)
    if limit is not None:
        tpl.append("LIMIT %d" % limit)

    sql_columns = ',\n'.join('  %s' % (i, ) for i in columns)
    sql_joins = '\n'.join('  ' + i for i in joins)
    sql = "\n".join(tpl) % dict(columns=sql_columns, joins=sql_joins,
                                id_column=id_column, ids=placeholders(ids))

    return "(%s)" % sql


def indent(s, indentation):
    return "\n".join(["%s%s" % (indentation, line) for line in s.split("\n")])


def generate_union_query(queries):
    return "\n\nUNION\n\n""".join(queries)


class SchemaError(RuntimeError):
    _dummy = True

class SchemaHelper(object):

    def __init__(self, schema, entities):
        self.schema = schema
        self.entities = entities

        self.check_schema()

    def check_schema(self):
        for kind in self.entities:
            try:
                self.schema[kind]
            except:
                raise SchemaError("Not all entities have a defined schema: %s" % kind)

        rel_entities = []
        for kind in self.entities:
            entity = self.schema[kind]
            for r in entity.iter_relations():
                rel_entities.append(r.start.entity)
                rel_entities.append(r.end.entity)

        missing = set(rel_entities) - set(self.entities)
        if missing:
            raise SchemaError("Some relations need additional entities: %s" % str(missing))

    def iter_entity_nodes(self, db, kind, properties=[]):
        entity = self.schema[kind]
        if not entity.fields:
            return [], []
        joins = [kind]
        tables = set([kind])
        #columns = ['%s.id' % (kind,)]
        columns = ["'%s' as kind" % (kind,)]

        if not properties:
            properties = [(f.name, f.maptype) for f in entity.iter_single_fields()]
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

        return columns, joins


    def iter_entity_relations(self, db, kind, properties=[]):

        entity = self.schema[kind]

        tables = set([kind])
        fields = []

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

                # do we need to convert the column value somehow?
                function = rel.rtype.function

                while column.foreign is not None:
                    foreign_table = table + '__' + column.name + '__' + column.foreign.table
                    if foreign_table not in tables:
                        #join = 'LEFT JOIN' if column.foreign.null else 'JOIN'
                        join = 'JOIN'
                        joins.append('%(join)s %(parent)s AS %(label)s ON %(label)s.id = %(child)s.%(child_column)s' % dict(
                            join=join, parent=column.foreign.table, child=table, child_column=column.name, label=foreign_table))
                        tables.add(foreign_table)
                    table = foreign_table
                    column = column.foreign

                if function:
                    columns.append("%s AS rel_type" % function('%s.%s' % (table, column.name)))
                else:
                    columns.append('%s.%s AS rel_type' % (table, column.name))
            else:
                columns.append("'%s' AS rel_type" % rel.rtype)


            # FIXME: that's really ugly...
            relation_properties = [(p.name, p) for p in rel.properties]
            if not properties:
                properties = relation_properties

            relation_properties = dict(relation_properties)
            for prop_name, prop_type in properties:

                if prop_name not in relation_properties.keys():
                    if prop_type == int:
                        columns.append('0 AS "%s"' % prop_name)
                    else:
                        columns.append('NULL AS "%s"' % prop_name)
                    continue

                table = kind
                column = relation_properties[prop_name].column
                while column.foreign is not None:
                    foreign_table = table + '__' + column.name + '__' + column.foreign.table
                    if foreign_table not in tables:
                        join = 'LEFT JOIN' if column.foreign.null else 'JOIN'
                        joins.append('%(join)s %(parent)s AS %(label)s ON %(label)s.id = %(child)s.%(child_column)s' % dict(
                            join=join, parent=column.foreign.table, child=table, child_column=column.name, label=foreign_table))
                        tables.add(foreign_table)
                    table = foreign_table
                    column = column.foreign

                columns.append('%s.%s%s' % (table, column.name, ' AS "%s"' % prop_name if prop_name else ''))

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

    def fetch_entities(self, db, kind, properties=[]):
        return self.iter_entity_nodes(db, kind, properties)

    def fetch_relations(self, db, kind, properties=[]):
        return self.iter_entity_relations(db, kind, properties)

    def fetch_all_fields(self, cfg, db):
        fields = set([])
        for kind in self.entities:
            entity = self.schema[kind]
            fields.update(
                set([(f.name, f.maptype) for f in entity.iter_single_fields()])
            )
        return list(fields)

    def fetch_all_relations_properties(self, cfg, db):
        relations = set([])
        for kind in self.entities:
            entity = self.schema[kind]
            relations.update(
                set([(p.name, p.maptype) for r in entity.iter_relations() for p in r.properties])
            )
        return list(relations)

    def fetch_all(self, cfg, db, properties):
        for entity in self.entities:
            yield self.fetch_entities(db, entity, properties)

    def fetch_all_relations(self, cfg, db, properties=[]):
        for entity in self.entities:
            yield self.fetch_relations(db, entity, properties)

