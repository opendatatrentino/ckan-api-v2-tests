#!/usr/bin/env python

"""
Recreate PostgreSQL database
"""

# We need:
# - Ckan configuration file (for db connection url)
# - PostgreSQL administrative credentials

from __future__ import print_function

from ConfigParser import RawConfigParser
import os
import sys
import subprocess
import urlparse

import psycopg2
import solr


def get_postgres_connection(host, port, user, password, database):
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database)
    conn.set_isolation_level(0)
    return conn


def recreate_db(admin_connection, user, database):
    ## Try dropping database
    ##----------------------------------------
    print("Dropping database (if exists) {0}".format(database))
    cur = admin_connection.cursor()
    cur.execute("""DROP DATABASE IF EXISTS "{0}";""".format(database))

    ## Recreate database
    ##----------------------------------------
    print("Creating database {0} (owned by {1})".format(database, user))
    cursor = admin_connection.cursor()
    cursor.execute("""
    CREATE DATABASE "{db}"
    WITH OWNER = "{user}"
    ENCODING = 'UTF8'
    TABLESPACE = pg_default
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    CONNECTION LIMIT = -1;
    """.format(db=database, user=user))


def rebuild_db_schema(conf_file):
    ## Run paster to recreate schema
    ##----------------------------------------

    print("Running paster db init")
    command = ['paster', '--plugin=ckan', 'db',
               '--conf={0}'.format(conf_file), 'init']
    subprocess.call(command)


def flush_solr(solr_url, site_id):
    """Empty this solr index"""
    s = solr.SolrConnection(solr_url)
    query = '+site_id:"{0}"'.format(site_id)
    s.delete_query(query)
    s.commit()
    assert s.query(query).numFound == 0


def reindex_solr(conf_file):
    print("Running paster search-index rebuild")
    command = ['paster', '--plugin=ckan', 'search-index',
               '--conf={0}'.format(conf_file), 'rebuild']
    subprocess.call(command)


def url_to_pg_credentials(url):
    parsed = urlparse.urlparse(url)
    return {
        'host': parsed.hostname,
        'port': parsed.port or 5432,
        'user': parsed.username,
        'password': parsed.password,
        'database': filter(None, parsed.path.split('/'))[0],
    }


if __name__ == '__main__':
    try:
        CONF_FILE = os.environ.get('CKAN_CONF')
        if not CONF_FILE:
            VIRTUAL_ENV = os.environ['VIRTUAL_ENV']
            CONF_FILE = os.path.join(
                VIRTUAL_ENV, 'etc', 'ckan', 'production.ini')
            if not os.path.exists(CONF_FILE):
                ## Ok, the file is definitely not there..
                raise KeyError

    except KeyError:
        # todo: we could guess $VIRTUAL_ENV/etc/ckan/production.ini
        print("You must pass CKAN_CONF environment variable")
        print("CKAN_CONF=/path/to/configuration/file")
        sys.exit(1)

    try:
        PG_ADMIN = os.environ['PG_ADMIN']
    except KeyError:
        print("You must pass PG_ADMIN environment variable")
        print("PG_ADMIN=username:password")
        sys.exit(1)

    conf = RawConfigParser()
    conf.read(CONF_FILE)

    postgresql_url = conf.get('app:main', 'sqlalchemy.url')
    credentials = url_to_pg_credentials(postgresql_url)
    admin_credentials = credentials.copy()
    admin_user, admin_password = PG_ADMIN.split(':', 1)
    admin_credentials.update({
        'user': admin_user,
        'password': admin_password,
        'database': 'postgres',  # administrative db
    })

    admin_connection = get_postgres_connection(**admin_credentials)

    solr_url = conf.get('app:main', 'solr_url')
    site_id = conf.get('app:main', 'ckan.site_id')

    ## First, make sure db / index are empty
    recreate_db(admin_connection, credentials['user'], credentials['database'])
    flush_solr(solr_url, site_id)

    ## Then, rebuild schemas
    rebuild_db_schema(CONF_FILE)
    reindex_solr(CONF_FILE)
