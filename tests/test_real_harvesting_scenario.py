"""
Test some "real life" harvesting scenario.

We have "data dumps" of an imaginary catalog for a set of days.

The testing procedure should be run as follows:

1- Get current state of the database
2- Update data from the "harvest source"
3- Make sure the database state matches the expected one:
   - unrelated datasets should still be there
   - only datasets from this souce should have been changed,
     and should match the desired state.
4- Loop for all the days
"""

import os
import json

import pytest

from ckan_api_client import CkanDataImportClient
from .utils.harvest_source import HarvestSource


HERE = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(os.path.dirname(HERE), 'data', 'datitrentino')

HARVEST_SOURCE_NAME = 'dummy-harvest-source'


# def _collection_to_dict(coll):
#     return dict((item['id'], item) for item in coll)


# def dump_current_database(base_url):
#     client = CkanClient(base_url)
#     return {
#         'dataset': _collection_to_dict(client.iter_datasets()),
#         'group': _collection_to_dict(client.iter_groups()),
#         'organization': _collection_to_dict(client.iter_organizations()),
#     }


# def is_our_dataset(dataset):
#     harvest_source = dataset['extra'].get(HARVEST_SOURCE_KEY)
#     return harvest_source == HARVEST_SOURCE_NAME


# def filter_our_datasets(datasets):
#     return dict((k, d) for k, d in datasets.iteritems() if is_our_dataset(d))


# def diff_collections(coll1, coll2):
#     """
#     Take two collections (dict) and return changes needed
#     in order to make coll1 == coll2.

#     Returns a dict with the {inserts, updates, deletes} keys
#     """
#     inserts = {}
#     updates = {}

#     for key, dataset in coll2:
#         if key in coll1:
#             # todo: check whether we should update the dataset!
#             updates[key] = dataset
#         else:
#             inserts[key] = dataset

#     deletes = set(x for x in coll1 if x not in coll2)

#     return {
#         'inserts': inserts,
#         'updates': updates,
#         'deletes': deletes,
#     }


# @pytest.mark.skipif(True, reason="Not implemented yet")
def test_real_harvesting_scenario(ckan_url, api_key):
    client = CkanDataImportClient(ckan_url, api_key, 'test-source')

    ## Create everything
    harvest_source = HarvestSource(DATA_DIR, 'day-00')
    client.sync_data(harvest_source, double_check=True)

    ## Perform first batch of updates
    harvest_source = HarvestSource(DATA_DIR, 'day-01')
    client.sync_data(harvest_source, double_check=True)
