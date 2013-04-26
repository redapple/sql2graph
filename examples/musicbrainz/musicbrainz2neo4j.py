#!/usr/bin/env python
# -*- coding: utf-8 -*-
import optparse
import ConfigParser
import os
import sys
import traceback
import sql2graph.export
from sql2graph.schema import Field, IntField, Relation, Reference, Column, Entity, Property

# ----------------------------------------------------------------------
MUSICBRAINZ_SIMPLE_SCHEMA = (
    Entity('artist',
        fields = [
            IntField('pk', Column('pk'), primary_key=True, index='artists'),
            Field('mbid', Column('mbid'), index='artists'),
            Field('name', Column('name'), index='artists'),
            Field('type', Column('type'), index='artists'),
            Field('country', Column('country'), index='artists'),
            Field('gender', Column('gender'), index='artists'),
            Field('comment', Column('comment')),
            Field('ended', Column('ended')),
            #Field('begin_date_year', Column('end_date_year'), index='artists'),
            #Field('end_date_year', Column('end_date_year'), index='artists'),
        ]
    ),
    Entity('artist_alias',
        fields = [
            IntField('pk', Column('pk'), primary_key=True, index='artist_aliases'),
            Field('name', Column('name'), index='artist_aliases'),
            #Field('locale',  Column('locale')),
            Field('type', Column('type'), index='artist_aliases'),
            #Field('begin_date_year', Column('end_date_year'), index='artist_aliases'),
            #Field('end_date_year', Column('end_date_year'), index='artist_aliases'),
        ],
        relations = [
            Relation(Reference('artist', 'artist_fk'),
                     Reference('artist_alias', 'pk'),
                     [Property('rel_type', 'has alias')])
        ]
    ),
    Entity('artist_credit_name',
        relations = [
            Relation(
                Reference('artist_credit', 'artist_credit_fk'),
                Reference('artist', 'artist_fk'),
                [
                    Property('rel_type', 'references'),
                    Property('as', Column('name')),
                ])
        ]
    ),
    Entity('artist_credit',
        fields = [
            IntField('pk', Column('pk'), primary_key=True, index='artist_credits'),
            Field('name', Column('name'), index='artist_credits'),
        ]
    ),
    Entity('release_group',
        fields = [
            IntField('pk', Column('pk'), primary_key=True, index='release_groups'),
            Field('mbid', Column('mbid'), index='release_groups'),
            Field('name', Column('name'), index='release_groups'),
            Field('type', Column('type'), index='release_groups'),
            Field('comment', Column('comment')),
        ],
        relations = [
            Relation(
                Reference('release_group', 'pk'),
                Reference('artist_credit', 'artist_credit_fk'),
                [Property('rel_type', 'credits'),])
        ]
    ),
    Entity('release',
        fields = [
            IntField('pk', Column('pk'), primary_key=True, index='releases'),
            Field('mbid', Column('mbid'), index='releases'),
            Field('name', Column('name'), index='releases'),
            Field('status', Column('status'), index='releases'),
            Field('country', Column('country')),
            Field('year', Column('date_year'), index='releases'),
        ],
        relations = [
            Relation(
                Reference('artist_credit', 'artist_credit_fk'),
                Reference('release', 'pk'),
                [Property('rel_type', 'released'),]),
            Relation(
                Reference('release', 'pk'),
                Reference('release_group', 'release_group_fk'),
                [Property('rel_type', 'part of'),])
        ]
    ),
    Entity('medium',
        fields = [
            IntField('pk', Column('pk'), primary_key=True, index='media'),
            Field('position', Column('position'), index='media'),
            Field('name', Column('name'), index='media'),
            Field('format', Column('format'), index='media'),
        ],
        relations = [
            Relation(
                Reference('release', 'release_fk'),
                Reference('tracklist', 'tracklist_fk'),
                [
                    Property('rel_type', 'has'),
                    Property('name', Column('name')),
                    Property('format', Column('format')),
                    Property('position', Column('position')),]
            ),
        ]
    ),
    Entity('recording',
        fields = [
            IntField('pk', Column('pk'), primary_key=True, index='recordings'),
            Field('mbid', Column('mbid'), index='recordings'),
            Field('name', Column('name'), index='recordings'),
            Field('length', Column('length'), index='recordings'),
        ],
        relations = [
            Relation(
                Reference('recording', 'pk'),
                Reference('artist_credit', 'artist_credit_fk'),
                [Property('rel_type', 'by')]
            ),
        ]
    ),
    Entity('track',
        fields = [
            IntField('pk', Column('pk'), primary_key=True, index='tracks'),
            Field('name', Column('name'), index='tracks'),
            Field('position', Column('position'), index='tracks'),
            Field('length', Column('length'), index='tracks'),
        ],
        relations = [
            Relation(
                Reference('track', 'pk'),
                Reference('tracklist', 'tracklist_fk'),
                [Property('rel_type', 'in')]
            ),
            Relation(
                Reference('track', 'pk'),
                Reference('recording', 'recording_fk'),
                [Property('rel_type', 'is')]
            ),
            Relation(
                Reference('track', 'pk'),
                Reference('artist_credit', 'artist_credit_fk'),
                [Property('rel_type', 'credits')]
            ),
        ]
    ),
    Entity('label',
        fields = [
            IntField('pk', Column('pk'), primary_key=True, index='labels'),
            Field('mbid', Column('mbid'), index='labels'),
            Field('name', Column('name'), index='labels'),
            Field('type', Column('type'), index='labels'),
            Field('country', Column('country'), index='labels'),
            #Field('label_code', Column('label_code'), index='labels'),
            #Field('comment', Column('comment')),
            Field('ended', Column('ended')),
        ]
    ),
    Entity('release_label',
        relations = [
            Relation(
                Reference('label', 'label_fk', null=True),
                Reference('release', 'release_fk'),
                [Property('rel_type', 'released'),
                Property('catalog', Column('catalog_number'))]),
        ]
    ),
    Entity('tracklist',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
        ]
    ),
    Entity('url',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('mbid', Column('mbid')),
            Field('name', Column('url')),   # lets not add a new property
        ]
    ),
    Entity('link_artist_artist',
        relations = [
            Relation(
                Reference('artist', 'artist1_fk'),
                Reference('artist', 'artist2_fk'),
                [
                    Property('rel_type', Column('name')),
                    # could add begin/end dates as properties
                ])
        ]
    ),
    Entity('link_artist_label',
        relations = [
            Relation(
                Reference('artist', 'artist_fk'),
                Reference('label', 'label_fk'),
                [
                    Property('rel_type', Column('name')),
                    # could add begin/end dates as properties
                ])
        ]
    ),
    Entity('link_label_label',
        relations = [
            Relation(
                Reference('label', 'label1_fk'),
                Reference('label', 'label2_fk'),
                [
                    Property('rel_type', Column('name')),
                    # could add begin/end dates as properties
                ])
        ]
    ),
    Entity('link_artist_url',
        relations = [
            Relation(
                Reference('artist', 'artist_fk'),
                Reference('url', 'url_fk'),
                [
                    Property('rel_type', Column('name')),
                    # could add begin/end dates as properties
                ])
        ]
    ),
    Entity('link_label_url',
        relations = [
            Relation(
                Reference('label', 'label_fk'),
                Reference('url', 'url_fk'),
                [
                    Property('rel_type', Column('name')),
                    # could add begin/end dates as properties
                    Property('short', Column('short_link_phrase')),
                ])
        ]
    ),
    Entity('link_label_recording',
        relations = [
            Relation(
                Reference('label', 'label_fk'),
                Reference('recording', 'recording_fk'),
                [Property('rel_type', Column('name')),])
        ]
    ),
)


def main():

    option_parser = optparse.OptionParser()
    option_parser.add_option("-c", "--config", dest="configfile", help="configuration file", default=None)
    option_parser.add_option("-p", "--pretend", action="store_true", dest="pretend", help="just pretend; do not write anthing", default=False)
    option_parser.add_option("-N", "--nodes", dest="nodes_file", help="Nodes file", default=None)
    option_parser.add_option("-R", "--relations", dest="rels_file", help="Relations file", default=None)
    (options, args) = option_parser.parse_args()

    config_parser = ConfigParser.RawConfigParser()
    if options.configfile:
        config_parser.read(options.configfile)
    else:
        print "you must provide a config file"
        sys.exit(0)

    if config_parser.has_section('TABLE_DUMPS'):
        dump_tables = dict((
                (entity, dump_file)
                    for entity, dump_file in config_parser.items('TABLE_DUMPS')
            ))
    else:
        print "no TABLE_DUMPS section"
        raise SystemExit


    if config_parser.has_option('IMPORT_ORDER', 'order'):
        entity_order = [entity.strip()
            for entity in config_parser.get('IMPORT_ORDER', 'order').split(',')]
    else:
        print "no IMPORT_ORDER/order"
        raise SystemExit

    if config_parser.has_section('INDEX_FILES'):
        index_files = dict((
                (index_name, index_file)
                    for index_name, index_file in config_parser.items('INDEX_FILES')
            ))
        #print "--- Index files"
        #print index_files
    else:
        print "no INDEX_FILES section"
        index_files = None



    # check if all dump files exist
    for entity in entity_order:
        if dump_tables.get(entity):
            if not os.path.isfile(dump_tables.get(entity)):
                print "file %s does not exist" % dump_tables.get(entity)
                raise RuntimeError


    exporter = sql2graph.export.GraphExporter(
        schema=MUSICBRAINZ_SIMPLE_SCHEMA, format='neo4j')

    if options.nodes_file:
        exporter.set_output_nodes_file(entity=sql2graph.export.MERGED, filename=options.nodes_file)

    if options.rels_file:
        exporter.set_output_relations_file(entity=sql2graph.export.MERGED, filename=options.rels_file)

    for index_name, index_file in index_files.iteritems():
        exporter.set_output_indexes_files(entity=index_name, filename=index_file)

    for entity in entity_order:
        if dump_tables.get(entity):
            exporter.feed_dumpfile(entity_name=entity, filename=dump_tables.get(entity))

    exporter.run()


if __name__ == '__main__':
    try:
        main()
    except Exception, e:
        traceback.print_exc()

