#!/bin/bash

export CKAN_BASE_URL='http://127.0.0.1:8000'
export CKAN_API_KEY='2e9d265f-ebb2-47d1-aa2d-4cdd12bb1664'

exec py.test -v "$@"
