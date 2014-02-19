import os

import pytest


@pytest.fixture(scope='session')
def ckan_url():
    return os.environ['CKAN_BASE_URL']


@pytest.fixture(scope='session')
def api_key():
    return os.environ['CKAN_API_KEY']


@pytest.fixture(scope='module')
def ckan_client(ckan_url, api_key):
    from ckan_api_client import CkanClient
    return CkanClient(ckan_url, api_key)
