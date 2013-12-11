# Exporting the Musicbrainz database into Neo4j

What you'll need:

* Python (and psycopg2 (https://pypi.python.org/pypi/psycopg2))
* Postgresl 9.x
* Lukáš Lalinský's `mbslave` project (https://bitbucket.org/lalinsky/mbslave)
* Neo4j 2.0.x (Milestone 2.0.0-M06 Community edition is ok): http://www.neo4j.org/download
* Michael Hunger's `batch-import` (https://github.com/jexp/batch-import)

## Create a mirror of MusicBrainz Postgresl database

I recommend Lukáš Lalinský's mbslave (https://bitbucket.org/lalinsky/mbslave)

### mbslave setup

```shell
$ git clone git@bitbucket.org:lalinsky/mbslave.git
$ cd mbslave/
```

Follow mbslave's `README.md`.

(For step 2, I usually create a new cluster just for MusicBrainz:

As root,

```shell
# mkdir musicbrainz
# cd musicbrainz/
# mkdir dbdata
# chown postgres:postgres dbdata
# su postgres
```

As 'postgres' user,

```shell
postgres@host$ pg_createcluster -d $PWD/dbdata --locale C -e UTF8 9.1 musicbrainz
postgres@host$ pg_ctlcluster 9.1 musicbrainz start
```

Create a superuser, e.g. "mbadmin" and create an UTF8 database called "musicbrainz"

```
postgres@host$ createuser -p 5433 -P
postgres@host$ createdb -p 5433 -l C -E UTF-8 -T template0 -O mbadmin musicbrainz
```
and `cp mbslabe.conf.default mbslave.conf` and adapt to your settings (database name, host and port number, password))

Continue following mbslave's instructions (steps 3, 4 and 5): prepare the schema, download dumps and import data

I usually use only `mbdump.tar.bz2` (it's 1.5GB)

After step 5, it should be sufficient to only run `./mbslave-remap-schema.py <sql/CreatePrimaryKeys.sql | ./mbslave-psql.py`.


## Generate the SQL export script 

```shell
$ cd /path/to/sql2graph/
$ python musicbrainz2neo4j-export.py > musicbrainz2neo4j.sql
```

And feed this script to `psql` on the musicbrainz database ; you can of course use mbslave for that too:
```
$ cd /path/to/mbslave
$ cat /path/to/musicbrainz2neo4j.sql | ./mbslave-psql.py
```

By default, the generated TSV/CSV files are in /tmp:

```
user@host$ ls -la /tmp/musicbrainz__*
-rw-r--r-- 1 postgres postgres 355724444 oct.  28 10:23 /tmp/musicbrainz__nodes__full.csv
-rw-r--r-- 1 postgres postgres 138827549 oct.  28 10:23 /tmp/musicbrainz__rels__full.csv
user@host$ 
```

Size may vary based on what entities you choose to export:
you can limit the entities you export using something like `python musicbrainz2neo4j-export.py --limit label,artist,url`

By default, all core entities in MusicBrainz are exported

### Import into Neo4j

```shell
$ cd /path/to/sql2graph/
$ python musicbrainz2neo4j-export.py > musicbrainz2neo4j.sql

//export just 1000 rows per table
$ python musicbrainz2neo4j-export.py --limit 1000 > musicbrainz2neo4j.sql
$ cd /path/to/mbslave
$ cat /path/to/musicbrainz2neo4j.sql | ./mbslave-psql.py
```

By default, the generated TSV/CSV files are in `/tmp/musicbrainz__nodes__full.csv` and `/tmp/musicbrainz__rels__full.csv`.

Now,
* use the batch-import project to import the csv files into neo4j,
* make sure you swith to the "20" branch of batch-import (for labels support)
* using a `mb_fulltext` and `mb_exact` index in a custom `./batch.properties` file,
* putting the database under `./musicbrainz.db`.
* (this will erase your current neo4j datastore)

```shell    
$ cd /path/to/jexp/batch-import
$ git checkout -b neo4j-2.0 origin/20
$ # build batch-import...
$ # prepare a batch.properties file:
$ echo -e "batch_import.node_index.mb_exact=exact\nbatch_import.node_index.mb_fulltext=fulltext" > batch.properties
$ MAVEN_OPTS="-server -Xmx10G -Dfile.encoding=UTF-8" mvn exec:java -Dfile.encoding=UTF-8 -Dexec.mainClass="org.neo4j.batchimport.Importer" -Dexec.args="batch.properties musicbrainz.db /tmp/musicbrainz__nodes__full.csv /tmp/musicbrainz__rels__full.csv"
```


Finally, restart your Neo4j instance (you had stopped it before running the batch-import, right?)
and play around with MusicBrainz data with the sample queries in examples/musicbrainz/queries.cyp
