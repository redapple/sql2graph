#!/bin/sh

# remove tables from musicbrainz schema
# do CASCADE drop if it fails
# and remove leftover TYPEs
cat sql/DropAll.sql | sed 's:admin/::' | ./mbslave-psql.py 2>&1 | grep 'cannot drop table .* because other objects depend on it' | sed -e 's:^psql.\+ cannot drop table \(\w\+\) because other objects depend on it$:DROP TABLE \1 CASCADE;:g' | ./mbslave-psql.py
grep -i "CREATE TYPE" sql/CreateTables.sql | sed -e "s:^CREATE TYPE \(\w\+\) .\+$:DROP TYPE \1;:" | ./mbslave-psql.py 

# remove tables from schemas cover_art_archive and statistics
cat sql/caa/DropTables.sql | ./mbslave-psql.py -s cover_art_archive
cat sql/statistics/DropTables.sql | ./mbslave-psql.py -s statistics

cat sql/statistics/DropReplicationTriggers.sql | ./mbslave-psql.py -s statistics
