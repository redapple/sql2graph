#!/usr/bin/env python
# -*- coding: utf-8 -*-

def gen_link_entity_entity_sql_export(entity0, entity1):
    out = """-- COPY (SELECT * FROM l_%s_%s) TO stdout CSV HEADER DELIMITER E'\\t';
""" % (entity0, entity1)

    out += """
COPY(
    SELECT
"""
    if entity0 == entity1:
        out += """
        link_ee.entity0 AS %s1_fk,
        link_ee.entity1 AS %s2_fk,
""" % (entity0, entity1)
    else:
        out += """
        link_ee.entity0 AS %s_fk,
        link_ee.entity1 AS %s_fk,
""" % (entity0, entity1)
    out += """
        link.begin_date_year,
        link.end_date_year,
        link_type.name,
        --link_type.long_link_phrase,
        --link_type.link_phrase,
        link_type.description
    FROM l_%s_%s link_ee
    JOIN link ON link_ee.link=link.id
    JOIN link_type ON link.link_type=link_type.id
)
TO stdout CSV HEADER DELIMITER E'\\t';
""" % (entity0, entity1)

    return out


entities = (
        ('area', 'area'),
        ('area', 'artist'),
        ('area', 'label'),
        ('area', 'work'),
        ('area', 'url'),
        ('area', 'recording'),
        ('area', 'release'),
        ('area', 'release_group'),

        #('artist', 'artist'),
        #('artist', 'label'),
        #('artist', 'recording'),
        #('artist', 'release'),
        #('artist', 'release_group'),
        #('artist', 'url'),
        #('artist', 'work'),

        #('label', 'label'),
        #('label', 'recording'),
        #('label', 'release'),
        #('label', 'release_group'),
        #('label', 'url'),
        #('label', 'work'),

        #('recording', 'recording'),
        #('recording', 'release'),
        #('recording', 'release_group'),
        #('recording', 'url'),
        #('recording', 'work'),

        #('release', 'release'),
        #('release', 'release_group'),
        #('release', 'url'),
        #('release', 'work'),

        #('release_group', 'release_group'),
        #('release_group', 'url'),
        #('release_group', 'work'),

        #('url', 'url'),
        #('url', 'work'),

        #('work', 'work'),
)
def main():

    for ent0, ent1 in entities:
        print "generating export script for l_%s_%s" % (ent0, ent1)
        print gen_link_entity_entity_sql_export(ent0, ent1)
    return 0

if __name__ == '__main__':
    main()



