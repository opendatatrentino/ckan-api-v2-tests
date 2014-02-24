#!/bin/bash

cd "$( dirname "$( readlink -f "$BASH_SOURCE" )" )"

export CKAN_BASE_URL='http://127.0.0.1:8000'
export CKAN_API_KEY="$( cat .apikey )"

exec py.test -v "$@"
