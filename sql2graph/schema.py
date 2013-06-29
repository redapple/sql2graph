# -*- coding: utf-8 -*-
#
# Copyright 2013 Paul Tremberth, Newlynn Labs
# See LICENSE for details.

# inspired by https://bitbucket.org/lalinsky/mbslave
#       mbslave/search.py

class Column(object):

    def __init__(self, name, default=None):
        self.name = name
        self.default = default

    def __repr__(self):
        return "Column(%s, default=%s)" % (self.name, self.default)

class Field(object):

    db_field_type = None

    def __init__(self, name, value, primary_key=False, index=None):
        self.name = name
        if isinstance(value, Column):
            self.column = value
            self.value = None
        else:
            self.value = value
        self.primary_key = primary_key
        self.index=index

    def __repr__(self):
        if self.value:
            return "Field(%s, value=%s)" % (self.name, self.value)
        elif self.column:
            return "Field(%s, column=%s)" % (self.name, self.column)


class IntField(Field):
    db_field_type = 'int'


class BoolField(Field):
    db_field_type = 'bool'

class BooleanField(Field):
    pass

class IntegerField(IntField):
    pass

class PrimaryKeyField(IntField):
    pass

class CharField(Field):
    db_field_type = 'string'

class TextField(Field):
    db_field_type = 'string'

class DateTimeField(Field):
    db_field_type = 'string'

class Property(object):

    def __init__(self, name, value, index=None):
        self.name = name
        self.value = value
        self.index = index


class Reference(object):

    def __init__(self, entity, db_column, null=False):
        self.entity = entity
        self.db_column = db_column
        self.null = null

    def __repr__(self):
        return "Reference(entity(%s), column(%s))" % (self.entity, self.db_column)


class Relation(object):

    def __init__(self, origin, target, properties):
        self.origin = origin
        self.target = target
        self.properties = properties

    def __repr__(self):
        return "Relation(origin(%s), target(%s))" % (self.origin, self.target)


class Entity(object):

    def __init__(self, name, fields=None, relations=None):
        self.name = name
        self.fields = fields
        self.relations = relations
        self.primary_key_field = None
        self.indexed_fields = None

    def get_primary_key_field(self):
        if not self.primary_key_field:
            if self.fields:
                for field in self.fields:
                    if field.primary_key:
                        self.primary_key_field = field
                        break

        return self.primary_key_field

    def get_indexed_fields(self):
        if not self.indexed_fields:
            indexes = {}
            if self.fields:
                for field in self.fields:
                    if field.index:
                        if field.index not in indexes:
                            indexes[field.index] = []
                        indexes[field.index].append(field)
            self.indexed_fields = indexes

        return self.indexed_fields
