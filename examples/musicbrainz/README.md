Importing MusicBrainz data into Neo4J
=====================================

1) Get MusicBrainz data in your database of choice
See

I use Lukas Lalisnky's mbslave https://bitbucket.org/lalinsky/mbslave
(therefor with PostgreSQL)

2) Export MusicBrainz data into simplified table dumps
(mainly resolving name links)

Look into the examples/musicbrainz/sql/ folder to export the data
as TAB delimited CSV files (with 1st row as field names)

You can choose to export only some entities.
E.g. if you're only interested in artists, labels and related URLs,
you could only use entities:
- artist,
- artist_alias,
- label,
- link_artist_artist
- link_artist_label
- link_artist_url
- link_label_label
- link_label_url

Again, I personally use ./mbslave-psql.py

    cat /path/to/examples/musicbrainz/sql/export-simple-artist.sql | ./mbslave-psql.py > dumps/simple-artist.csv
    cat /path/to/examples/musicbrainz/sql/export-simple-label.sql | ./mbslave-psql.py > dumps/simple-label.csv
    ...
    bzip2 dumps/*.csv   # optional

3) Edit mb2neo.conf to match where you have the CSV dump files
  and run sql2graph on these dumps files by adapting mb2neo.conf

    python musicbrainz2neo4j.py --config mb2neo-simple.conf

The script needs sql2graph module so you probably need to move what's in examples/musicbrainz/
where appropriate

4) Import the nodes, relations and indexes files into Neo4J using batch-import

    python run_batchimport.py --config mb2neo-simple.conf

**This takes a long time if you import all (or all main) entities.**
