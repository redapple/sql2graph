# -*- coding: utf-8 -*-
#
# Copyright 2013 Paul Tremberth, Newlynn Labs
# See LICENSE for details.

import graph
import schema
import bz2
import gzip
import csv
import traceback
import sys
import copy
import pprint
import os

# ----------------------------------------------------------------------
MERGED = '***MERGED***'

class CsvBatchWriter(object):

    CSV_BATCH_SIZE = 100000
    DEBUG = False

    def __init__(self, filename, batch_size=CSV_BATCH_SIZE):
        self.filename = filename
        self.fp = None
        self.csvwriter = None
        self.batch_size = batch_size
        self.output = []
        self.so_far = 0

    def initialize(self, header_fields):

        if self.filename.endswith(('.bz2',)):
            self.fp = bz2.BZ2File(self.filename, 'wb')
        elif self.filename.endswith(('.gz',)):
            self.fp = gzip.GzipFile(self.filename, 'wb')
        else:
            self.fp = open(self.filename, 'wb')

        self.csvwriter = csv.DictWriter(self.fp, header_fields, dialect="excel-tab")
        self.csvwriter.writeheader()

    def append(self, elem):
        self.output.append(elem)
        self.test_flush()

    def extend(self, elems):
        self.output.extend(elems)
        self.test_flush()

    def test_flush(self):
        if len(self.output) >= self.batch_size:
            self.flush()

    def flush(self, delete_elements=False):
        if self.output:

            self.csvwriter.writerows(self.output)
            self.so_far += len(self.output)
            if self.DEBUG:
                print " == %d rows written to %s (total=%d) ==" % (
                    len(self.output), self.filename, self.so_far)

            if delete_elements:
                for n in self.output:
                    del n
            self.output = []

    def close(self):
        if self.fp:
            self.fp.close()


class GraphExporter(object):

    CSV_BATCH_SIZE = 100000
    SUPPORTED_OUTPUT_FORMATS = ['neo4j']
    DEBUG = True

    def __init__(self, schema, format, dialect=csv.excel_tab, pretend=False):

        # only supported format for now is Neo4j batch-import
        # see: https://github.com/jexp/batch-import
        if not self.supported_format(format):
            raise ValueError
        self.format = format
        self.dialect = dialect

        self.node_list = graph.NodeList()
        self.relation_list = graph.RelationList()

        self.global_nodes_csv_fields = None    # used as CSV header column names
        # per-entity CSV header
        self.nodes_csv_fields = {}

        self.global_rels_csv_fields = None # used as CSV header column names
        # per-entity CSV header
        self.rels_csv_fields = {}

        self.schema = dict((entity.name, entity) for entity in schema)

        self.dumpfiles = {}
        self.dumpfile_fields = {}
        self.entity_order = []

        self.output_nodes_files = {}
        self.output_relations_files = {}
        self.output_indexes_files = {}

        self.pretend = pretend

    def supported_format(self, format):
        return format.lower() in [f.lower() for f in self.SUPPORTED_OUTPUT_FORMATS]

    def feed_dumpfile(self, entity, filename, fields=None):
        self.dumpfiles[entity] = filename
        if fields:
            self.dumpfile_fields[entity] = fields
        self.entity_order.append(entity)

    def set_output_nodes_file(self, entity, filename):
        self.output_nodes_files[entity] = filename

    def set_output_relations_file(self, entity, filename):
        self.output_relations_files[entity] = filename

    def set_output_indexes_file(self, entity, filename):
        self.output_indexes_files[entity] = filename

    def run(self, write_nodes=True, write_rels=True):
        self.read_schema()
        self.export(write_nodes, write_rels)

    def read_schema(self):
        self.read_nodes_csv_fields()
        self.read_rels_csv_fields()

    def read_nodes_csv_fields(self):
        # all Nodes SHOULD have their entity as a property
        fields_begin = ['kind']

        node_properties = []

        for entity_name, entity in self.schema.iteritems():
            print entity_name, entity
            if entity_name not in self.entity_order:
                continue
            if not entity.fields:
                print "no fields for %s" % entity_name
            else:
                self.nodes_csv_fields[entity_name] = copy.copy(fields_begin)
                for field in entity.fields:
                    # the following could be used to add a column type to CSV header fields
                    #node_properties.append(
                        #"%s%s" % (
                            #field.name,
                            #":%s" % field.db_field_type
                                #if field.db_field_type else ''))

                    node_properties.append(field.name)
                    self.nodes_csv_fields[entity_name].append(field.name)
                self.nodes_csv_fields[entity_name] = set(self.nodes_csv_fields[entity_name])
        print "nodes_csv_fields:"
        pprint.pprint(self.nodes_csv_fields)
        self.global_nodes_csv_fields = fields_begin + list(set(node_properties) - set(fields_begin))

    def read_rels_csv_fields(self):
        fields_begin = ['start', 'end', 'rel_type']
        rels_properties = []
        for entity_name, entity in self.schema.iteritems():

            if entity_name not in self.entity_order:
                continue
            if not entity.relations:
                print "no relations for %s" % entity_name
            else:
                self.rels_csv_fields[entity_name] = copy.copy(fields_begin)
                for rel in entity.relations:
                   rels_properties.extend([prop.name for prop in rel.properties])
                   self.rels_csv_fields[entity_name].extend([prop.name for prop in rel.properties])

                self.rels_csv_fields[entity_name] = fields_begin + list(
                    set(self.rels_csv_fields[entity_name]) - set(fields_begin))

        print "rels_csv_fields:"
        pprint.pprint(self.rels_csv_fields)
        self.global_rels_csv_fields = fields_begin + list(
            set(rels_properties) - set(fields_begin))

    def export(self, write_nodes=True, write_rels=True):
        """
        Read dump files and write nodes and relations at the same time
        """

        for entity_name in self.entity_order:
            self.export_entity(entity_name, write_nodes, write_rels)
            self.node_list.offload_hint()

    def export_entity(self, entity_name, write_nodes=True, write_rels=True):
        print "export_entity: %s" % entity_name
        if not self.dumpfiles.get(entity_name) or not self.schema.get(entity_name):
            if self.DEBUG:
                print "no dump file or not schema configured for entity", entity_name
            return

        onodes_filename = self.output_nodes_files.get(entity_name)
        orels_filename = self.output_relations_files.get(entity_name)
        #print onodes_filename, orels_filename
        nodes_csv_writer, rels_csv_writer = None, None

        if onodes_filename and self.nodes_csv_fields.get(entity_name):
            if not self.pretend:
                if write_nodes:
                    nodes_csv_writer = CsvBatchWriter(onodes_filename, self.CSV_BATCH_SIZE)
                    nodes_csv_writer.initialize(self.nodes_csv_fields[entity_name])
                # create a symbolic link
                else:
                    try:
                        os.symlink(self.dumpfiles.get(entity_name), onodes_filename)
                    except:
                        pass

        if orels_filename and self.rels_csv_fields.get(entity_name):
            if not self.pretend and write_rels:
                rels_csv_writer = CsvBatchWriter(orels_filename, self.CSV_BATCH_SIZE)
                rels_csv_writer.initialize(self.rels_csv_fields[entity_name])

        index_writers = {}

        if self.DEBUG:
            print "--- processing file", self.dumpfiles[entity_name]
        entity = self.schema.get(entity_name)
        with self.open_dumpfile(self.dumpfiles[entity_name]) as dumpfile:

            #self.create_index_writers_if_needed(entity, index_writers)

            self.export_tabledump(entity, dumpfile,
                nodes_csv_writer, rels_csv_writer, index_writers)

        # pending relations if any
        if not self.pretend:
            self.export_rels_csv(writer=rels_csv_writer)

        # close all CSV writers
        if nodes_csv_writer:
            nodes_csv_writer.close()

        if rels_csv_writer:
            rels_csv_writer.close()

        for w in index_writers.itervalues():
            w.close()

    def export_global(self):

        onodes_filename = self.output_nodes_files.get(MERGE)
        orels_filename = self.output_relations_files.get(MERGED)

        nodes_csv_writer, rels_csv_writer = None, None

        if onodes_filename:
            if not self.pretend:
                nodes_writer = CsvBatchWriter(onodes_filename, self.CSV_BATCH_SIZE)
                nodes_writer.initialize(self.global_nodes_csv_fields)

        if orels_filename:
            if not self.pretend:
                rels_writer = CsvBatchWriter(orels_filename, self.CSV_BATCH_SIZE)
                rels_writer.initialize(self.global_rels_csv_fields)

        index_writers = {}

        # loop on dump files in order
        if not self.entity_order:
            self.entity_order = list(self.dumpfiles.iterkeys())

        for entity_name in self.entity_order:
            if not self.dumpfiles.get(entity_name) or not self.schema.get(entity_name):
                if self.DEBUG:
                    print "no dump file or not schema configured for entity", entity_name
                continue

            if self.DEBUG:
                print "--- processing file", self.dumpfiles[entity_name]
            entity = self.schema.get(entity_name)
            with self.open_dumpfile(self.dumpfiles[entity_name]) as dumpfile:

                self.create_index_writers_if_needed(entity, index_writers)

                self.export_tabledump(entity, dumpfile,
                    nodes_writer, rels_writer, index_writers)

        # pending relations if any
        if not self.pretend:
            self.export_rels_csv(writer=rels_csv_writer)

        # close all CSV writers
        if nodes_csv_writer:
            nodes_writer.close()

        if rels_csv_writer:
            rels_writer.close()

        for w in index_writers.itervalues():
            w.close()


    def create_index_writers_if_needed(self, entity, index_writers):
        indexes = entity.get_indexed_fields()
        if indexes:
            for index_name, indexed_fields in indexes.iteritems():
                if index_name not in index_writers:

                    # check if output file has been configured for this index
                    index_filename = self.output_indexes_files.get(index_name)
                    if not index_filename:
                        print "no output file for index %s" % index_name
                        continue

                    # add a "node id" field
                    header_fields = ['node_id'] + [field.name for field in indexed_fields]

                    index_writer = CsvBatchWriter(index_filename, self.CSV_BATCH_SIZE)
                    index_writer.initialize(header_fields)

                    index_writers[index_name] = index_writer

    def export_tabledump(self, entity, fp,
            nodes_writer, rels_writer, index_writers):

        stats = {'nodes': 0, 'rels': 0, 'indexed': 0}
        if not entity:
            print "know nothing about %s" % entity.name
            return

        PRINT_FREQUENCY = 50000

        # should we write something to one or more indexes?
        if index_writers:
            indexes = entity.get_indexed_fields()
        else:
            indexes = None

        node_id = 0

        # read CSV file line by line
        #print self.dialect
        csvreader = csv.DictReader(fp, dialect=self.dialect)
        cnt=0
        for cnt, record in enumerate(csvreader, start=1):

            node = None

            # create a new node
            primary_key_field = entity.get_primary_key_field()
            if primary_key_field:

                node = graph.Node(record, entity)
                node_id = self.node_list.add_node(node)
                if not node_id:
                    # FIXME: find something better
                    raise LookupError

                if nodes_writer:
                    # add it to the write queue
                    nodes_writer.append(node.get_dict(self.nodes_csv_fields[entity.name]))

                stats['nodes'] += 1

                if indexes:
                    for index_name, indexed_fields in indexes.iteritems():
                        index_writers.get(index_name).append(
                            node.get_dict(
                                ['node_id'] + [field.name for field in indexed_fields]))
                        stats['indexed'] += 1
            #else:
                #raise RuntimeError("no primary key field for entity %s" % entity.name)

            if rels_writer:
                # add relations if needed
                new_rels = [rel.get_dict(self.rels_csv_fields[entity.name])
                    for rel in self.iter_relations(entity, record)]
                rels_writer.extend(new_rels)
                stats['rels'] += len(new_rels)

            # hint to gc; there's surely something prettier
            if node:
                del node
            del record

            if self.DEBUG:
                if not (cnt % PRINT_FREQUENCY):
                    print "\r %32s: line %8d - nodes(%8d), rels(%8d), idx(%8d) -- last node ID %d" % (
                        entity.name, cnt, stats['nodes'], stats['rels'], stats['indexed'], node_id),
                    sys.stdout.flush()

        if self.DEBUG:
            print
            print " --> Finished with %8d of entity %s" % (cnt, entity.name)
            print "nodes(%8d), rels(%8d), idx(%8d) -- last node ID %d" % (
                        stats['nodes'], stats['rels'], stats['indexed'],  node_id)

        # write everything that's pending
        writers = [nodes_writer, rels_writer] + list(index_writers.itervalues())
        for w in writers:
            if w:
                w.flush()

    def iter_relations(self, entity, record):
        relation_definitions = entity.relations
        if not relation_definitions:
            return

        for reldef in relation_definitions:
            try:
                origin_column = record.get(reldef.origin.db_column)
                target_column = record.get(reldef.target.db_column)
                if not origin_column and reldef.origin.null:
                    continue
                if not target_column and reldef.target.null:
                    continue

                origin_node_pos, target_node_pos = None, None
                origin_node_pos = self.node_list.lookup_node_pos(
                    reldef.origin.entity,
                    int(origin_column))
                target_node_pos = self.node_list.lookup_node_pos(
                    reldef.target.entity,
                    int(target_column))

                if not origin_node_pos or not target_node_pos:
                    continue
                # else:
                #   FIXME: store an unresolved relation

                properties = {}
                for prop in reldef.properties:
                    if isinstance(prop.value, schema.Column):
                        properties[prop.name] = record.get(prop.value.name)
                    else:
                        properties[prop.name] = prop.value

                yield graph.Relation(
                            origin_node_pos,
                            target_node_pos,
                            properties)
            except Exception, e:
                traceback.print_exc()
                raise e

    def resolve_relation(self, r):
        if not r.end:
            target_node_pos = self.node_list.lookup_node_pos(
                r.start_target_entity, r.start_fk)
            if target_node_pos:
                r.end = target_node_pos


    def export_rels_csv(self, fp=None, writer=None):
        BATCH_SIZE = 10000
        if not writer and fp:
            writer = csv.DictWriter(fp, self.global_rels_csv_fields, dialect="excel-tab")
            writer.writeheader()
        size = len(self.relation_list.relation_list)
        print "%d relations to write" % size
        output_relations = []
        for cnt, rel in enumerate(self.relation_list.iter_rels(), start=1):
            print "\r %8d/%8d (%.1f%%)" % (cnt, size, 100*cnt/size),
            if not rel.end:
                self.resolve_relation(rel)
            output_relations.append(rel.get_dict())
            del rel
            if not (cnt % BATCH_SIZE):
                self._flush_rows(writer, output_relations)
                output_relations = []
        if output_relations:
            self._flush_rows(writer, output_relations)
        print

    @classmethod
    def open_dumpfile(cls, filename):
        if filename.endswith(('.bz2',)):
            return bz2.BZ2File(filename, 'rb')
        elif filename.endswith(('.gz',)):
            return gzip.GzipFile(filename, 'rb')
        else:
            return open(filename, 'rb')
