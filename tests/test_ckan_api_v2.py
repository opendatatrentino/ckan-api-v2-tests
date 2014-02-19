#!/usr/bin/env python

"""
Blackbox testing for Ckan API v2
"""

import uuid
import datetime
import copy


def test_simple_dataset_crud(ckan_client):
    ## Let's try creating a dataset

    now = datetime.datetime.now()
    now_str = now.strftime('%F %T')

    _dataset = {
        'name': 'dataset-' + str(uuid.uuid4()),
        'title': 'Dataset {0}'.format(now_str),

        # we can use this as key for harvester?
        'url': 'http://example.com/dataset1',

        'author': 'Author 1',
        'author_email': 'author1@example.com',
        'maintainer': 'Maintainer 1',
        'maintainer_email': 'maintainer1@example.com',
        'license_id': 'cc-by',
        'notes': "Dataset 1 notes",
        'private': False,
    }

    dataset = ckan_client.post_dataset(_dataset)
    dataset_id = dataset['id']

    ## Let's check dataset data first
    for key, val in _dataset.iteritems():
        assert dataset[key] == val

    ## Check that retrieved dataset is identical
    dataset = ckan_client.get_dataset(dataset_id)
    for key, val in _dataset.iteritems():
        assert dataset[key] == val

    ## Check against data loss on update..
    retrieved_dataset = dataset
    updates = {
        'author': 'Another Author',
        'author_email': 'another.author@example.com',
    }
    new_dataset = copy.deepcopy(dataset)
    new_dataset.update(updates)

    ## Get the updated dataset
    updated_dataset = ckan_client.put_dataset(dataset_id, new_dataset)
    updated_dataset_2 = ckan_client.get_dataset(dataset_id)

    ## They should be equal!
    assert updated_dataset == updated_dataset_2

    ## And the updated dataset shouldn't have data loss
    expected_dataset = copy.deepcopy(retrieved_dataset)
    expected_dataset.update(updates)

    IGNORED_FIELDS = ['revision_id', 'metadata_modified']
    for f in IGNORED_FIELDS:
        updated_dataset.pop(f, None)
        expected_dataset.pop(f, None)

    assert updated_dataset == expected_dataset

    ## Delete the dataset
