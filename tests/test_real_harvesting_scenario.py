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

from ckan_api_client import CkanClient


HERE = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(os.path.dirname(HERE), 'data', 'datitrentino')

HARVEST_SOURCE_NAME = 'dummy-harvest-source'
HARVEST_SOURCE_KEY = '_harvest_source'
HARVEST_ID_KEY = '_harvest_id'


def HarvestSource(object):
    def __init__(self, day):
        """
        :param day:
            The day from which to get data.
            Full name, like 'day-00', 'day-01', ..
        """
        self.day = day

    def iter_objects(self, objtype):
        folder = os.path.join(DATA_DIR, self.day, objtype)
        for filename in os.listdir(folder):
            filepath = os.path.join(folder, filename)
            with open(filepath, 'r') as f:
                data = json.load(f)
            yield data


def _collection_to_dict(coll):
    return dict((item['id'], item) for item in coll)


def dump_current_database(base_url):
    client = CkanClient(base_url)
    return {
        'dataset': _collection_to_dict(client.iter_datasets()),
        'group': _collection_to_dict(client.iter_groups()),
        'organization': _collection_to_dict(client.iter_organizations()),
    }


def is_our_dataset(dataset):
    harvest_source = dataset['extra'].get(HARVEST_SOURCE_KEY)
    return harvest_source == HARVEST_SOURCE_NAME


def filter_our_datasets(datasets):
    return dict((k, d) for k, d in datasets.iteritems() if is_our_dataset(d))


def diff_collections(coll1, coll2):
    """
    Take two collections (dict) and return changes needed
    in order to make coll1 == coll2.

    Returns a dict with the {inserts, updates, deletes} keys
    """
    inserts = {}
    updates = {}

    for key, dataset in coll2:
        if key in coll1:
            # todo: check whether we should update the dataset!
            updates[key] = dataset
        else:
            inserts[key] = dataset

    deletes = set(x for x in coll1 if x not in coll2)

    return {
        'inserts': inserts,
        'updates': updates,
        'deletes': deletes,
    }


@pytest.mark.skipif(True, reason="Not implemented yet")
def test_real_harvesting_scenario(ckan_url, api_key):
    DAYS = ['day-00', 'day-01']

    client = CkanClient(ckan_url, api_key)

    ##------------------------------------------------------------
    ## First, get the whole database
    ##------------------------------------------------------------
    current_state = dump_current_database(ckan_url)

    ##------------------------------------------------------------
    ## Then, get list of our datasets
    ##------------------------------------------------------------
    our_datasets = filter_our_datasets(current_state['dataset'])

    ##------------------------------------------------------------
    ## Make the changes needed to make the database state
    ## reflect the one of the harvested data
    ##------------------------------------------------------------
    harvest_source = HarvestSource(DAYS[0])
    harvested_datasets = _collection_to_dict(
        HarvestSource.iter_objects('dataset'))

    ok_datasets = set()  # id of datasets that are already ok
    inserts = {}
    updates = {}  # id of datasets to be updated
    deletes = set()  # id of datasets to be deleted

    for key, dataset in harvested_datasets:
        if key in our_datasets:
            # todo: check whether to put in ok_datasets or updates!
            # We only want to consider the fields that can
            # actually be changed..
            ok_datasets.add(key)

        else:
            inserts[key] = dataset

    ## Figure out deleted datasets
    for key in our_datasets:
        if key not in harvested_datasets:
            deletes.add(key)

    ##------------------------------------------------------------
    ## Check that database state is the expected one..
    ##------------------------------------------------------------
    pass
