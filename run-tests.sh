#!/bin/bash

export CKAN_BASE_URL='http://127.0.0.1:8000'
export CKAN_API_KEY='1bb4091d-cf0e-439b-9208-d74abca61f2c'

exec py.test -v "$@"
