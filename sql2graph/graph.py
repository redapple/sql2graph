# -*- coding: utf-8 -*-
#
# Copyright 2013 Paul Tremberth, Newlynn Labs
# See LICENSE for details.

import collections
import schema

# ----------------------------------------------------------------------

class DictLookup(object):
    def __init__(self):
        self.container = {}

    def append(self, entity, entity_pk, node_position):
        if not self.container.get(entity):
            self.container[entity] = {}
        self.container[entity][entity_pk] = node_position

    def lookup(self, entity, entity_pk):
        if self.container.get(entity):
            if self.container[entity].get(entity_pk):
                return self.container[entity][entity_pk]

    def start_entity(self, entity):
        pass

    def stop_entity(self, entity):
        pass


class Node(object):
    def __init__(self, record, entity):
        if not isinstance(record, dict):
            raise TypeError
        self.record = record

        if not isinstance(entity, schema.Entity) or not entity.get_primary_key_field():
            raise TypeError
        self.entity = entity

        # convert record to properties per entity schema definition
        self.properties = {}
        for field in entity.fields:
            if field.column and isinstance(field.column, schema.Column):
                self.properties[field.name] = record.get(field.column.name)
            else:
                self.properties[field.name] = field.value

        #self.pk_column = pk_column
        #self.entity_name = entity.name
        #print entity.get_primary_key_field().column.name
        self.entity_pk = int(record.get(entity.get_primary_key_field().column.name))

        self.properties['kind'] = self.entity.name
        self.properties['node_id'] = None

    def get_dict(self, fields):
        return dict((e, self.properties.get(e, '')) for e in fields)

    def save_node_id(self, node_id):
        self.properties['node_id'] = node_id

    def get_node_id(self):
        return self.properties['node_id']

    def get_entity_pk(self):
        return self.entity_pk

    def __repr__(self):
        return "entity [%s/pk=%s]: %s" % (self.entity.name, self.entity_pk, str(self.properties))


class Relation(object):
    def __init__(self, start, end, properties):
        self.start = start
        self.end = end
        self.properties = properties

        # used when we could not resolve on the fly
        self.start_fk = None
        self.start_target_entity = None

    def get_dict(self, fields):

        if self.end:
            values = dict((
                ('start', self.start),
                ('end', self.end),
            ))
            values.update(self.properties)
            return dict((e, values.get(e, '')) for e in fields)
        else:
            return None

    def set_deferred_resolution(self, fk, target_entity):
        self.start_fk = fk
        self.start_target_entity = target_entity

    def __repr__(self):
        return "(%d)-[%s]->(%d)" % (
            self.start, self.property, self.end or 0,)


class NodeList(object):

    def __init__(self):
        self.node_list_size = 0

        self.reverse_lookup = DictLookup()

        self.entity_fields = set()

        self.last_lookup = None
        self.last_lookup_result = None

    def get_all_fields(self):
        return self.entity_fields

    def update_entity_fields(self, record):
        self.entity_fields.update(record.iterkeys())

    def add_node(self, node):
        """
        Add a node to the node list
        Returns the node position in the list
        """
        # self.node_list.append(node)
        # do not actually store the Node,
        # just pretend it is at position X (current size of list)
        self.node_list_size += 1

        node.save_node_id(self.node_list_size)

        node_pk = node.get_entity_pk()
        if node_pk:
            self.reverse_lookup.append(node.entity.name, node_pk, node.get_node_id())
        else:
            print node, "has no entity PK"
            raise RuntimeError

        # reset last lookup cache
        self.last_lookup, self.last_lookup_result = None, None

        return node.get_node_id()

    def lookup_node_pos(self, entity_name, entity_pk):
        if self.last_lookup == (entity_name, entity_pk):
            return self.last_lookup_result
        else:
            result = self.reverse_lookup.lookup(entity_name, entity_pk)
            self.last_lookup = (entity_name, entity_pk)
            self.last_lookup_result = result

        return result

    def iter_nodes(self):
        while True:
            try:
                yield self.node_list.popleft()
            except:
                break


class RelationList(object):

    def __init__(self):
        self.relation_list = collections.deque()

    def add_relation(self, relation):
        self.relation_list.append(relation)

    def iter_rels(self):
        while True:
            try:
                yield self.relation_list.popleft()
            except:
                break


