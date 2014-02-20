#!/bin/bash

export CKAN_BASE_URL='http://127.0.0.1:8000'
export CKAN_API_KEY='5cfe9b53-4173-4c25-9f4d-f9cf3512485e'

exec py.test -v "$@"
