#!/usr/bin/env python

import os
from lxml import etree as ET
from mbslave import Config, connect_db
from mbslave.search import sqlexport_all

cfg = Config(os.path.join(os.path.dirname(__file__), 'mbslave.conf'))
db = connect_db(cfg, True)

for id, doc in sqlexport_all(cfg, db):
    print doc
