sql2graph
=========

sql2graph is a python helper module to export CSV dumps from a relational database
to a format that can be imported into a graph database.

Currently, only Neo4J batch-import format is supported.
(see https://github.com/jexp/batch-import)

Instructions for Neo4J batch-import
===================================

References:
* https://github.com/jexp/batch-import
* http://maxdemarzi.com/2012/02/28/batch-importer-part-1/
    and http://maxdemarzi.com/2012/02/28/batch-importer-part-2/

Prerequisites:
- Neo4J installed (http://www.neo4j.org/download)
- batch-import installed and built (https://github.com/jexp/batch-import)

1) Export your SQL tables data as TAB-delimited CSV with fieldnames as first row.
For example, using Postgresql, running something like this should work:

    COPY (SELECT * FROM mytable) TO stdout CSV HEADER DELIMITER E'\t';

You can then redirect the output to a file from the psql command line,
or you can replace "stdout" above by a file path directly

2) Copy and rename sql2graph.conf.default to sql2graph.conf (or something else)
and edit it.
Set the dump filenames for all entities you want to import
Optionally, define some index files.

3) Copy and rename example-export.py script
and define the schema for the Nodes and their relations.
If you want to index some fields for your entities, don't forget to add
"index=True" on them
See examples folder.

Then run:

    python xyz-export.py --conf sql2graph.conf -N nodes.csv -R relations.csv

4) You should now have:
- a CSV file representing all nodes for all entities
        (nodes.csv if you ran the command above)
- a CSV file representing the relations
        (relations.csv if you ran the command above)
- depending on the field indexes you defined, a number of
        node index files for Neo4J (relation indexes not supported)

(See https://github.com/jexp/batch-import for the various files and format details.)

**Stop neo4j before running batch-import.**

You have 2 options:
 - either run Neo4J batch-import manually supplying your files on the command line
 (again, see https://github.com/jexp/batch-import on how to do that)

 - or, run run_batchimport.py which uses the config file to run the command for you
        (check your sql2graph.conf BATCHIMPORT_SETTINGS section)


5) (Re)start neo4j

