"""
Tests to pin-point exact behavior of datasets CRUD, in particular
updates.
"""

import uuid
import datetime
import copy

import pytest

from ckan_api_client import HTTPError
from .utils import (prepare_dataset, check_dataset,
                    gen_random_id, gen_dataset_name)


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


def test_updating_extras(request, ckan_client):
    ## First, create the dataset
    our_dataset = prepare_dataset(ckan_client)
    created_dataset = ckan_client.post_dataset(our_dataset)
    dataset_id = created_dataset['id']
    request.addfinalizer(lambda: ckan_client.delete_dataset(dataset_id))

    def update_and_check(updates, expected):
        updated_dataset = ckan_client.update_dataset(dataset_id, updates)
        retrieved_dataset = ckan_client.get_dataset(dataset_id)
        assert updated_dataset == retrieved_dataset
        check_dataset(updated_dataset, expected)

    ##------------------------------------------------------------
    ## Update #1: add some extras

    extras_update = {'field-1': 'value-1', 'field-2': 'value-2'}
    expected_updated_dataset = copy.deepcopy(our_dataset)
    expected_updated_dataset['extras'].update({
        'field-1': 'value-1',
        'field-2': 'value-2',
    })
    update_and_check({'extras': extras_update}, expected_updated_dataset)

    ##------------------------------------------------------------
    ## Update #1: change a field

    extras_update = {'field-1': 'value-1-VERSION2'}
    expected_updated_dataset = copy.deepcopy(our_dataset)
    expected_updated_dataset['extras'].update({
        'field-1': 'value-1-VERSION2',
        'field-2': 'value-2',
    })
    update_and_check({'extras': extras_update}, expected_updated_dataset)

    ##------------------------------------------------------------
    ## Update #3: change a field, add another

    extras_update = {'field-1': 'value-1-VERSION3', 'field-3': 'value-3'}
    expected_updated_dataset = copy.deepcopy(our_dataset)
    expected_updated_dataset['extras'].update({
        'field-1': 'value-1-VERSION3',
        'field-2': 'value-2',
        'field-3': 'value-3',
    })
    update_and_check({'extras': extras_update}, expected_updated_dataset)

    ##------------------------------------------------------------
    ## Update #4: delete a field

    extras_update = {'field-3': None}
    expected_updated_dataset = copy.deepcopy(our_dataset)
    expected_updated_dataset['extras'].update({
        'field-1': 'value-1-VERSION3',
        'field-2': 'value-2',
    })
    update_and_check({'extras': extras_update}, expected_updated_dataset)

    ##------------------------------------------------------------
    ## Update #5: add + update + delete

    extras_update = {'field-1': 'NEW_VALUE', 'field-2': None,
                     'field-3': 'hello', 'field-4': 'world'}
    expected_updated_dataset = copy.deepcopy(our_dataset)
    expected_updated_dataset['extras'].update({
        'field-1': 'NEW_VALUE',
        'field-3': 'hello',
        'field-4': 'world',
    })
    update_and_check({'extras': extras_update}, expected_updated_dataset)


def test_extras_bad_behavior(request, ckan_client):
    dataset = {
        'name': gen_dataset_name(),
        'title': "Test dataset",
        'extras': {'a': 'aa', 'b': 'bb', 'c': 'cc'},
    }
    created = ckan_client.post_dataset(dataset)
    dataset_id = created['id']
    request.addfinalizer(lambda: ckan_client.delete_dataset(dataset_id))
    assert created['extras'] == {'a': 'aa', 'b': 'bb', 'c': 'cc'}

    ## Update #1: omitting extras will.. flush it!
    updated = ckan_client.put_dataset(dataset_id, {
        'id': dataset_id,
        'name': dataset['name'],
        'title': dataset['title'],
        # 'extras' intentionally omitted
    })
    assert updated['extras'] == {}

    ## Update #2: re-add some extras
    updated = ckan_client.put_dataset(dataset_id, {
        'id': dataset_id,
        'name': dataset['name'],
        'title': dataset['title'],
        'extras': {'a': 'aa', 'b': 'bb', 'c': 'cc'},
    })
    assert updated['extras'] == {'a': 'aa', 'b': 'bb', 'c': 'cc'}

    ## Update #3: partial extras will just update
    updated = ckan_client.put_dataset(dataset_id, {
        'id': dataset_id,
        'name': dataset['name'],
        'title': dataset['title'],
        'extras': {'a': 'UPDATED'},
    })
    assert updated['extras'] == {'a': 'UPDATED', 'b': 'bb', 'c': 'cc'}

    ## Update #4: empty extras has no effect
    updated = ckan_client.put_dataset(dataset_id, {
        'id': dataset_id,
        'name': dataset['name'],
        'title': dataset['title'],
        'extras': {},
    })
    assert updated['extras'] == {'a': 'UPDATED', 'b': 'bb', 'c': 'cc'}

    ## Update #5: this is fucked up, man..
    updated = ckan_client.put_dataset(dataset_id, {
        'id': dataset_id,
        'name': dataset['name'],
        'title': dataset['title'],
    })
    assert updated['extras'] == {}


def test_updating_groups(request, ckan_client):
    dataset = {
        'name': gen_dataset_name(),
        'title': "Test dataset",
        'groups': []
    }

    dummy_groups = []
    for x in xrange(10):
        code = gen_random_id()
        group = ckan_client.post_group({
            'name': 'group-{0}'.format(code),
            'title': 'Group {0}'.format(code),
        })
        dummy_groups.append(group)
        request.addfinalizer(lambda: ckan_client.delete_group(group['id']))

    dataset['groups'] = [x['id'] for x in dummy_groups[:5]]

    created = ckan_client.post_dataset(dataset)
    dataset_id = created['id']
    request.addfinalizer(lambda: ckan_client.delete_dataset(dataset_id))
    assert sorted(created['groups']) == sorted(dataset['groups'])

    # Let's try updating the dataset w/o groups
    updated = ckan_client.update_dataset(dataset_id, {'title': "My dataset"})
    assert sorted(updated['groups']) == sorted(dataset['groups'])

    # Let's try updating the dataset with empty groups
    updated = ckan_client.update_dataset(dataset_id, {'groups': []})
    assert sorted(updated['groups']) \
        == sorted(dataset['groups'])  # WTF? -- should be empty

    ## APPARENTLY, if we pass a subset of the datasets, the extra ones
    ## will just get deleted.

    # # Let's play around a bit..
    # new_groups = [x['id'] for x in dummy_groups[:3]]
    # updated = ckan_client.update_dataset(dataset_id, {'groups': new_groups})
    # assert sorted(updated['groups']) \
    #     == sorted(dataset['groups'] + new_groups)  # WTF?

    # # Let's play around a bit..
    # new_groups = [x['id'] for x in dummy_groups[7:9]]
    # updated = ckan_client.update_dataset(dataset_id, {'groups': new_groups})
    # assert sorted(updated['groups']) \
    #     == sorted(new_groups)  # WTF?


def test_groups_bad_behavior(request, ckan_client):
    """
    Test to pinpoint "bad behavior" when updating groups associated
    with a dataset.

    See the GROUP_TEST_CASES (initial state, update, result) below
    for more information on the behavior to expect..
    """

    OMITTED = object()
    GROUP_TEST_CASES = [
        ## If we omit the key entirely, it will be flushed
        ([], OMITTED, []),
        ([1, 2, 3], OMITTED, []),

        ## If we pass None, the API will fail with 500:
        ## Error - <type 'exceptions.TypeError'>: object of type
        ## 'NoneType' has no len()
        # ([1, 2, 3], None, []),

        ## For other cases, it seems to work when passed IDs
        ## Do **not** attempt passing objects, as behavior here
        ## is more uncertain..
        ([1, 2, 3], [], []),
        ([1], [1, 2, 3, 4], [1, 2, 3, 4]),
        ([1, 2, 3], [1, 2], [1, 2]),
        ([1, 2, 3], [1, 2, 4], [1, 2, 4]),
        ([1, 2], [1, 2, 3, 4], [1, 2, 3, 4]),
    ]

    ## Create a bunch of groups
    grp = []
    for x in xrange(5):
        code = gen_random_id()
        group = ckan_client.post_group({
            'name': 'group-{0}'.format(code),
            'title': 'Group {0}'.format(code),
        })
        grp.append(group['id'])
        request.addfinalizer(lambda: ckan_client.delete_group(group['id']))

    def _new_dataset(groups):
        code = gen_random_id()
        dataset = {
            'name': 'dataset-{0}'.format(code),
            'title': 'Dataset {0}'.format(code),
            'groups': [grp[1], grp[2]],
        }
        created = ckan_client.post_dataset(dataset)
        assert created['name'] == dataset['name']
        assert created['title'] == dataset['title']
        assert sorted(created['groups']) == sorted([grp[1], grp[2]])
        dataset_id = created['id']
        request.addfinalizer(lambda: ckan_client.delete_dataset(dataset_id))
        return created

    def _to_ids(x):
        if x is None or x is OMITTED:
            return x
        return [grp[y - 1] for y in x]

    for state, update, result in GROUP_TEST_CASES:
        state, update, result = map(_to_ids, (state, update, result))

        dataset = _new_dataset(groups=state)

        ## Intentionally using low-level put_dataset() here
        _data = {'groups': update} if update is not OMITTED else {}
        upd = ckan_client.put_dataset(dataset['id'], _data)
        upd2 = ckan_client.get_dataset(dataset['id'])

        assert sorted(upd['groups']) == sorted(result)
        assert sorted(upd2['groups']) == sorted(result)
