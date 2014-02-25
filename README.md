# Tests for Ckan API v2

## Usage

1. install requirements listed in ``requirements.txt``
2. have a look at ``run-tests.sh`` for the environment variables
   that need to be set
3. launch ``py.test`` to actually run the tests

## Requirements

A Ckan instance and the API key for a sysadmin user of that instance.
Beware that data will be written and potentially messed up on that
instance.


## Preparing the Ckan instance

```console
CONF_FILE="production.ini"
paster --plugin=ckan make-config ckan "$CONF_FILE"

paster --plugin=ckan db --config="$CONF_FILE" init
paster --plugin=ckan search-index --config="$CONF_FILE" rebuild

paster --plugin=ckan user --config="$CONF_FILE" add admin email=admin@e.com
paster --plugin=ckan sysadmin --config="$CONF_FILE" add admin

paster --plugin=ckan serve "$CONF_FILE"
```


## Recreating database

You can use ``scripts/recreate-db.py`` to recreate database / solr index.
Specifically, it will:

- drop and recreate postgresql database
- delete everything from the Solr index
- recreate database schema, using paster
- rebuild search index
- create and 'admin' user, make it superuser
- store the 'admin' api key in ``.apikey`` for reuse

Environment variables:

- ``CKAN_CONF`` should point to a Ckan configuration file.
  If omitted, configuration file will be searched in ``$VIRTUAL_ENV/etc/ckan/production.ini``.

- ``PG_ADMIN`` colon-separated administrative access credentials
  to postgresql. Example: ``postgres:password``


## Running tests

Use ``run-server.sh`` to launch paster server (note: client will try connecting
on port 8000, make sure you configured it correctly in ``production.ini``).

Use ``run-tests.sh`` to run tests using py.test.
The script accepts extra arguments that will be appended to py.test command,
for example I usually run it as ``./run-tests.sh -vvv --pdb``.
