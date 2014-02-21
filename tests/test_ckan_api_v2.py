#!/usr/bin/env python

"""
Blackbox testing for Ckan API v2
"""

import uuid
import datetime
import copy

import pytest

from ckan_api_client import HTTPError, GROUP_FIELDS
from .utils import (prepare_dataset, check_group, gen_random_id,
                    get_dummy_group)


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
    ckan_client.delete_dataset(dataset_id)


def test_delete_dataset(ckan_client):
    our_dataset = prepare_dataset(ckan_client)
    created_dataset = ckan_client.post_dataset(our_dataset)
    dataset_id = created_dataset['id']

    dataset_ids = ckan_client.list_datasets()
    assert dataset_id in dataset_ids

    ## Now delete
    ckan_client.delete_dataset(dataset_id)

    ## Anonymous users cannot see the dataset
    anon_client = ckan_client.anonymous
    dataset_ids = anon_client.list_datasets()
    assert dataset_id not in dataset_ids
    with pytest.raises(HTTPError) as excinfo:
        anon_client.get_dataset(dataset_id)
    assert excinfo.value.status_code in (403, 404)  # :(

    ## Administrators can still access deleted dataset
    deleted_dataset = ckan_client.get_dataset(dataset_id)
    assert deleted_dataset['state'] == 'deleted'

    ## But it's still gone from the list
    dataset_ids = ckan_client.list_datasets()
    assert dataset_id not in dataset_ids

    # ## Yay! Let's delete everything!
    # dataset_ids = ckan_client.list_datasets()
    # deleted = set()
    # for dataset_id in dataset_ids[:10]:
    #     ckan_client.delete_dataset(dataset_id)
    #     deleted.add(dataset_id)

    # ## Check that they're really gone!
    # dataset_ids = ckan_client.list_datasets()
    # assert deleted.intersection(dataset_ids) == set()


def test_group_crud(ckan_client):
    code = gen_random_id()
    group = {
        'name': 'group-{0}'.format(code),
        'title': 'Group {0}'.format(code),
    }
    created = ckan_client.post_group(group)
    check_group(created, group)
    group_id = created['id']

    # Retrieve & check
    retrieved = ckan_client.get_group(group_id)
    assert retrieved == created

    # Update & check
    updated = ckan_client.put_group(group_id, {'title': 'My Group'})
    assert updated['name'] == group['name']
    assert updated['title'] == 'My Group'

    # Check differences
    expected = copy.deepcopy(created)
    expected['title'] = 'My Group'
    check_group(updated, expected)

    # Retrieve & double-check
    retrieved = ckan_client.get_group(group_id)
    assert retrieved == updated

    # Delete
    #------------------------------------------------------------
    # Note: it's impossible to actually delete a group.
    #       The only hint it has been deleted is its "state"
    #       is set to "deleted".
    #------------------------------------------------------------
    ckan_client.delete_group(group_id)

    with pytest.raises(HTTPError) as excinfo:
        ckan_client.get_group(group_id)
    assert excinfo.value.status_code in (404, 403)  # workaround

    # retrieved = ckan_client.get_group(group_id)
    # assert retrieved['state'] == 'deleted'

    # anon_client = ckan_client.anonymous
    # # with pytest.raises(HTTPError) as excinfo:
    # #     anon_client.get_group(group_id)
    # # assert excinfo.value.status_code in (404, 403)  # workaround
    # retrieved = anon_client.get_group(group_id)
    # assert retrieved['state'] == 'deleted'


def test_simple_group_crud(ckan_client):
    ## Let's try creating a dataset

    _group = get_dummy_group(ckan_client)

    group = ckan_client.post_group(_group)
    group_id = group['id']

    ## Let's check group data first..
    for key, val in _group.iteritems():
        assert group[key] == val

    ## Check that retrieved group is identical
    group = ckan_client.get_group(group_id)
    for key, val in _group.iteritems():
        assert group[key] == val

    ## Check against data loss on update..
    retrieved_group = group
    updates = {
        'title': 'New group title',
        'description': 'New group description',
    }
    new_group = copy.deepcopy(group)
    new_group.update(updates)

    ## Get the updated group
    updated_group = ckan_client.put_group(group_id, new_group)
    updated_group_2 = ckan_client.get_group(group_id)

    ## They should be equal!
    assert updated_group == updated_group_2

    ## And the updated group shouldn't have data loss
    expected_group = copy.deepcopy(retrieved_group)
    expected_group.update(updates)

    check_group(updated_group, expected_group)

    # for f in GROUP_FIELDS['cruft']:
    #     updated_group.pop(f, None)
    #     expected_group.pop(f, None)

    # assert updated_group == expected_group

    ## Delete the group
    ckan_client.delete_group(group_id)
