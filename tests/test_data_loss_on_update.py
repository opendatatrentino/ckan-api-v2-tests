"""
Here we test data loss when updating objects.

In a perfect case, we should just be able to update
things by passing in only the updates, but unfortunately
we need to attach a lot more stuff in order to prevent
them from being deleted.
"""

##----------------------------------------------------------------------
## todo: write test to check updating "groups"
##----------------------------------------------------------------------
## todo: write test to check updating "relationships"
##----------------------------------------------------------------------
## todo: write test to check updating "resources"
##----------------------------------------------------------------------
## todo: check that resources keep the same id upon update
##       - create dataset with some resources
##       - update dataset adding a resource and removing another
##       - check that resources kept the same id based on URL
##       - if not, we have to hack around this.. :(
##----------------------------------------------------------------------

import copy

from .utils import (prepare_dataset, check_dataset, check_group,
                    gen_random_id, gen_dataset_name)


def test_data_loss_on_update(request, ckan_client):
    """
    Strategy:

    1. We create the dataset
    2. We retrieve the dataset and keep it for later
    3. We send an update
    4. We check that update affected only the desired keys
    """
    our_dataset = prepare_dataset(ckan_client)

    ## Create the dataset
    created_dataset = ckan_client.post_dataset(our_dataset)
    dataset_id = created_dataset['id']
    request.addfinalizer(lambda: ckan_client.delete_dataset(dataset_id))

    ## Make sure that the thing we created makes sense
    retrieved_dataset = ckan_client.get_dataset(dataset_id)
    assert retrieved_dataset == created_dataset
    check_dataset(retrieved_dataset, our_dataset)

    ## Ok, now we can start updating and see what happens..
    updates = {'title': "My new dataset title"}

    expected_updated_dataset = copy.deepcopy(our_dataset)
    expected_updated_dataset.update(updates)

    updated_dataset = ckan_client.update_dataset(dataset_id, updates)
    retrieved_dataset = ckan_client.get_dataset(dataset_id)
    assert updated_dataset == retrieved_dataset

    check_dataset(updated_dataset, expected_updated_dataset)


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
    for x in xrange(5):
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
