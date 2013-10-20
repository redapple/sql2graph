#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sql2graph.schema2 import SchemaHelper, Schema, Column, ForeignColumn, \
    Property, IntegerProperty, Relation, Entity, Reference

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

mbentities = [
    'area',
    'area_alias',
    'area_type',

    'place',
    'place_alias',

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
        ('area', 'place'),

        ('artist', 'artist'),
        ('artist', 'label'),
        ('artist', 'recording'),
        ('artist', 'release'),
        ('artist', 'release_group'),
        ('artist', 'url'),
        ('artist', 'work'),
        ('artist', 'place'),

        ('label', 'label'),
        ('label', 'recording'),
        ('label', 'release'),
        ('label', 'release_group'),
        ('label', 'url'),
        ('label', 'work'),
        ('label', 'place'),

        ('place', 'place'),
        ('place', 'recording'),
        ('place', 'release'),
        ('place', 'release_group'),
        ('place', 'url'),
        ('place', 'work'),

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

mbentities.extend(["l_%s_%s" % (e0, e1) for e0, e1 in linked_entities])

mbschema = Schema([
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
    Entity('area_alias',
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
    Entity('place', [
            IntegerProperty('pk', Column('id')),
            Property('mbid', Column('gid')),
            Property('name', Column('name')),
            Property('type', Column('type', ForeignColumn('place_type', 'name', null=True))),
        ],
    ),
    Entity('place_alias',
        [
            IntegerProperty('pk', Column('id')),
            Property('name', Column('name')),
            Property('type', Column('type', ForeignColumn('place_alias_type', 'name', null=True))),
            Property('locale', Column('locale')),
        ],
        [
            Relation(
                'HAS_ALIAS',
                start=Reference('place', Column('place')),
                end=Reference('place_alias', Column('id')),
                properties=[]
            ),
        ]
    ),
    Entity('artist',
        [
            IntegerProperty('pk', Column('id')),
            Property('mbid', Column('gid')),
            Property('disambiguation', Column('comment')),

            # schema change 2013-10-14
            #Property('name', Column('name', ForeignColumn('artist_name', 'name'))),
            Property('name', Column('name')),

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
                'BEGAN_IN_AREA',
                start=Reference('artist', Column('id')),
                end=Reference('area', Column('begin_area')),
                properties=[]
            ),
            Relation(
                'ENDED_IN_AREA',
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

            # schema change 2013-10-14
            #Property('name', Column('name', ForeignColumn('artist_name', 'name'))),
            Property('name', Column('name')),

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

            # schema change 2013-10-14
            #Property('name', Column('name', ForeignColumn('artist_name', 'name'))),
            Property('name', Column('name')),
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

            # schema change 2013-10-14
            #Property('name', Column('name', ForeignColumn('label_name', 'name'))),
            Property('name', Column('name')),

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
                'FROM_AREA',
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

            # schema change 2013-10-14
            #Property('name', Column('name', ForeignColumn('work_name', 'name'))),
            Property('name', Column('name')),

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

            # schema change 2013-10-14
            #Property('name', Column('name', ForeignColumn('release_name', 'name'))),
            Property('name', Column('name')),

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

            # schema change 2013-10-14
            #Property('name', Column('name', ForeignColumn('release_name', 'name'))),
            Property('name', Column('name')),

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
                    IntegerProperty('year', Column('date_year')),
                    IntegerProperty('month', Column('date_month')),
                    IntegerProperty('day', Column('date_day')),
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
                'ON_MEDIUM',
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

            # schema change 2013-10-14
            #Property('name', Column('name', ForeignColumn('track_name', 'name'))),
            Property('name', Column('name')),

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

            # schema change 2013-10-14
            #Property('name', Column('name', ForeignColumn('track_name', 'name'))),
            Property('name', Column('name')),
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

