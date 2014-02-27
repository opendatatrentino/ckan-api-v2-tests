"""
Test some simple harvesting scenarios
"""

import os

import pytest

from ckan_api_client import CkanDataImportClient
from .utils.harvest_source import HarvestSource


HERE = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(os.path.dirname(HERE), 'data', 'random')

HARVEST_SOURCE_NAME = 'dummy-harvest-source'


@pytest.fixture(params=['day-{0:02d}'.format(x) for x in xrange(4)])
def harvest_source(request):
    return HarvestSource(DATA_DIR, request.param)


def test_real_harvesting_scenario(ckan_url, api_key, harvest_source):
    client = CkanDataImportClient(ckan_url, api_key, 'test-source')
    client.sync_data(harvest_source, double_check=True)
