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
