#!/bin/bash
exec paster --plugin=ckan serve "$VIRTUAL_ENV"/etc/ckan/production.ini "$@"
