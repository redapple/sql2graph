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

def make_link_entity(entity_name, start_name, start_column, end_name, end_column):
    return Entity(entity_name,
        relations = [
            Relation(
                Reference(start_name, start_column),
                Reference(end_name, end_column),
                [
                    Property('rel_type', Column('name')),
                    Property('year_begin', Column('begin_date_year')),
                    Property('year_end', Column('end_date_year')),
                ])
        ]
    )

MUSICBRAINZ_SIMPLE_SCHEMA = (
    Entity('area',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('mbid:string:mbid', Column('mbid:string:mbid')),
            Field('name:string:mb', Column('name:string:mb')),
            Field('type:string:mb', Column('type:string:mb')),
        ],
    ),
    Entity('area_alias',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('name:string:mb', Column('name:string:mb')),
            #Field('locale',  Column('locale')),
            Field('type:string:mb', Column('type:string:mb')),
            #Field('begin_date_year', Column('end_date_year')),
            #Field('end_date_year', Column('end_date_year')),
        ],
        relations = [
            Relation(Reference('area', 'artist_fk'),
                     Reference('area_alias', 'pk'),
                     [Property('rel_type', 'has alias')])
        ]
    ),
    Entity('artist',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('mbid:string:mbid', Column('mbid:string:mbid')),
            Field('name:string:mb', Column('name:string:mb')),
            Field('type:string:mb', Column('type:string:mb')),
            Field('area', Column('area')),
            Field('gender', Column('gender')),
            Field('comment', Column('comment')),
            Field('ended', Column('ended')),
            Field('begin_date_year', Column('begin_date_year')),
            Field('end_date_year', Column('end_date_year')),
        ]
    ),
    Entity('artist_alias',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('name:string:mb', Column('name:string:mb')),
            #Field('locale',  Column('locale')),
            Field('type:string:mb', Column('type:string:mb')),
            #Field('begin_date_year', Column('end_date_year')),
            #Field('end_date_year', Column('end_date_year')),
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
            IntField('pk', Column('pk'), primary_key=True),
            Field('name:string:mb', Column('name:string:mb')),
        ]
    ),
    Entity('release_group',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('mbid:string:mbid', Column('mbid:string:mbid')),
            Field('name:string:mb', Column('name:string:mb')),
            Field('type:string:mb', Column('type:string:mb')),
            Field('comment', Column('comment')),
        ],
        relations = [
            Relation(
                Reference('release_group', 'pk'),
                Reference('artist_credit', 'artist_credit_fk'),
                [
                    Property('rel_type', 'credits'),
                ]
            ),
        ]
    ),
    Entity('release',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('mbid:string:mbid', Column('mbid:string:mbid')),
            Field('name:string:mb', Column('name:string:mb')),
            Field('status', Column('status')),
            Field('barcode', Column('barcode')),
        ],
        relations = [
            Relation(
                Reference('artist_credit', 'artist_credit_fk'),
                Reference('release', 'pk'),
                [
                    Property('rel_type', 'released'),
                ]
            ),
            Relation(
                Reference('release', 'pk'),
                Reference('release_group', 'release_group_fk'),
                [
                    Property('rel_type', 'part of'),
                ]
            ),
        ]
    ),
    Entity('release_country',
        relations = [
            Relation(
                Reference('release', 'release_fk'),
                Reference('area', 'area_fk'),
                [
                    Property('rel_type', 'released in'),
                    Property('year', Column('date_year')),
                    Property('month', Column('date_month')),
                ]
            ),
        ]
    ),
    Entity('medium',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('position', Column('position')),
            Field('name:string:mb', Column('name:string:mb')),
            Field('format', Column('format')),
        ],
        relations = [
            Relation(
                Reference('release', 'release_fk'),
                Reference('medium', 'pk'),
                [
                    Property('rel_type', 'released on'),
                    Property('name', Column('name')),
                    Property('format', Column('format')),
                    #Property('position', Column('position')),
                ]
            ),
        ]
    ),
    Entity('recording',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('mbid:string:mbid', Column('mbid:string:mbid')),
            Field('name:string:mb', Column('name:string:mb')),
            Field('length', Column('length')),
        ],
        relations = [
            Relation(
                Reference('recording', 'pk'),
                Reference('artist_credit', 'artist_credit_fk'),
                [
                    Property('rel_type', 'by'),
                ]
            ),
        ]
    ),
    Entity('track',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('mbid:string:mbid', Column('mbid:string:mbid')),
            Field('name:string:mb', Column('name:string:mb')),
            Field('number', Column('number')),
            Field('position', Column('position')),
            Field('length', Column('length')),
        ],
        relations = [
            Relation(
                Reference('track', 'pk'),
                Reference('recording', 'recording_fk'),
                [
                    Property('rel_type', 'is'),
                ]
            ),
            Relation(
                Reference('track', 'pk'),
                Reference('medium', 'medium_fk'),
                [
                    Property('rel_type', 'appears on'),
                ]
            ),
            Relation(
                Reference('track', 'pk'),
                Reference('artist_credit', 'artist_credit_fk'),
                [
                    Property('rel_type', 'credits'),
                ]
            ),
        ]
    ),
    Entity('work',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('mbid:string:mbid', Column('mbid:string:mbid')),
            Field('name:string:mb', Column('name:string:mb')),
            Field('type:string:mb', Column('type:string:mb')),
            Field('comment', Column('comment')),
        ],
    ),
    Entity('label',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('mbid:string:mbid', Column('mbid:string:mbid')),
            Field('name:string:mb', Column('name:string:mb')),
            Field('type:string:mb', Column('type:string:mb')),
            Field('area', Column('area')),
            #Field('label_code', Column('label_code')),
            #Field('comment', Column('comment')),
            Field('ended', Column('ended')),
        ]
    ),
    Entity('release_label',
        relations = [
            Relation(
                Reference('release', 'release_fk'),
                Reference('label', 'label_fk', null=True),
                [
                    Property('rel_type', 'released on'),
                    Property('catalog', Column('catalog_number')),
                ]
            ),
        ]
    ),
    Entity('url',
        fields = [
            IntField('pk', Column('pk'), primary_key=True),
            Field('mbid:string:mbid', Column('mbid:string:mbid')),
            Field('url', Column('url')),   # lets not add a new property
        ],
    ),

    # link_artist_*
    make_link_entity('link_artist_artist',
        'artist', 'artist1_fk',
        'artist', 'artist2_fk'),
    make_link_entity('link_artist_label',
        'artist', 'artist_fk',
        'label', 'label_fk'),
    make_link_entity('link_artist_recording',
        'artist', 'artist_fk',
        'recording', 'recording_fk'),
    make_link_entity('link_artist_release',
        'artist', 'artist_fk',
        'release', 'release_fk'),
    make_link_entity('link_artist_release_group',
        'artist', 'artist_fk',
        'release_group', 'release_group_fk'),
    make_link_entity('link_artist_url',
        'artist', 'artist_fk',
        'url', 'url_fk'),
    make_link_entity('link_artist_work',
        'artist', 'artist_fk',
        'work', 'work_fk'),

    # link_label_*
    make_link_entity('link_label_label',
        'label', 'label1_fk',
        'label', 'label1_fk'),
    make_link_entity('link_label_recording',
        'label', 'label_fk',
        'recording', 'recording_fk'),
    make_link_entity('link_label_release',
        'label', 'label_fk',
        'release', 'release_fk'),
    make_link_entity('link_label_release_group',
        'label', 'label_fk',
        'release_group', 'release_group_fk'),
    make_link_entity('link_label_url',
        'label', 'label_fk',
        'url', 'url_fk'),
    make_link_entity('link_label_work',
        'label', 'label_fk',
        'work', 'work_fk'),

    # link_area_*
    make_link_entity('link_area_area',
        'area', 'area1_fk',
        'area', 'area2_fk'),
    make_link_entity('link_area_artist',
        'area', 'area_fk',
        'artist', 'artist_fk'),
    make_link_entity('link_area_label',
        'area', 'area_fk',
        'label', 'label_fk'),
    make_link_entity('link_area_work',
        'area', 'area_fk',
        'work', 'work_fk'),
    make_link_entity('link_area_url',
        'area', 'area_fk',
        'url', 'url_fk'),
    make_link_entity('link_area_recording',
        'area', 'area_fk',
        'recording', 'recording_fk'),
    make_link_entity('link_area_release',
        'area', 'area_fk',
        'release', 'release_fk'),
    make_link_entity('link_area_release_group',
        'area', 'area_fk',
        'release_group', 'release_group_fk'),

    # link_recording_*
    make_link_entity('link_recording_recording',
        'recording', 'recording1_fk',
        'recording', 'recording2_fk'),
    make_link_entity('link_recording_release',
        'recording', 'recording_fk',
        'release', 'release_fk'),
    make_link_entity('link_recording_release_group',
        'recording', 'recording_fk',
        'release_group', 'release_group_fk'),
    make_link_entity('link_recording_url',
        'recording', 'recording_fk',
        'url', 'url_fk'),
    make_link_entity('link_recording_work',
        'recording', 'recording_fk',
        'work', 'work_fk'),

    # link_release_*
    make_link_entity('link_release_release',
        'release', 'release1_fk',
        'release', 'release2_fk'),
    make_link_entity('link_release_release_group',
        'release', 'release_fk',
        'release_group', 'release_group_fk'),
    make_link_entity('link_release_url',
        'release', 'release_fk',
        'url', 'url_fk'),
    make_link_entity('link_release_work',
        'release', 'release_fk',
        'work', 'work_fk'),

    # link_release_group_*
    make_link_entity('link_release_group_release_group',
        'release_group', 'release_group1_fk',
        'release_group', 'release_group2_fk'),
    make_link_entity('link_release_group_url',
        'release_group', 'release_group_fk',
        'url', 'url_fk'),
    make_link_entity('link_release_group_work',
        'release_group', 'release_group_fk',
        'work', 'work_fk'),

    # link_url_*
    make_link_entity('link_url_url',
        'url', 'url1_fk',
        'url', 'url2_fk'),
    make_link_entity('link_url_work',
        'url', 'url_fk',
        'work', 'work_fk'),

    # link_work_*
    make_link_entity('link_work_work',
        'work', 'work1_fk',
        'work', 'work2_fk'),
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

    if not config_parser.has_option('TABLE_DUMPS_DIR', 'dir'):
        print "you must provide TABLE_DUMPS_DIR/dir"
        sys.exit(0)
    table_dumps_dir = config_parser.get('TABLE_DUMPS_DIR', 'dir')

    if config_parser.has_section('TABLE_DUMPS'):
        dump_tables = dict((
                (entity, "%s/%s" % (table_dumps_dir, dump_file))
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


    # check if all dump files exist
    for entity in entity_order:
        if dump_tables.get(entity):
            if not os.path.isfile(dump_tables.get(entity)):
                raise RuntimeError("file %s does not exist" % dump_tables.get(entity))

    exporter = sql2graph.export.GraphExporter(
        schema=MUSICBRAINZ_SIMPLE_SCHEMA, format='neo4j')

    for entity_name in entity_order:
        if dump_tables.get(entity_name):
            exporter.set_output_nodes_file(entity=entity_name, filename="nodes_%s.csv" % entity_name)
            exporter.set_output_relations_file(entity=entity_name, filename="rels_%s.csv" % entity_name)
            exporter.feed_dumpfile(entity=entity_name, filename=dump_tables.get(entity_name))

    exporter.run(nodes=False)


if __name__ == '__main__':
    try:
        main()
    except Exception, e:
        traceback.print_exc()

