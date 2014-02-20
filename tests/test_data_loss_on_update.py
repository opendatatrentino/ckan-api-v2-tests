"""
Here we test data loss when updating objects.

In a perfect case, we should just be able to update
things by passing in only the updates, but unfortunately
we need to attach a lot more stuff in order to prevent
them from being deleted.
"""

##----------------------------------------------------------------------
## todo: check that resources keep the same id upon update
##       - create dataset with some resources
##       - update dataset adding a resource and removing another
##       - check that resources kept the same id based on URL
##       - if not, we have to hack around this.. :(
##----------------------------------------------------------------------

import copy
import random
import string
import time

from ckan_api_client import DATASET_FIELDS, RESOURCE_FIELDS


OUR_GROUPS = [
    {'name': 'group-01', 'title': 'Group 01'},
    {'name': 'group-02', 'title': 'Group 02'},
    {'name': 'group-03', 'title': 'Group 03'},
]
OUR_ORG = {
    'name': 'custom-organization',
    'title': 'Custom Organization',
}
OUR_DATASET = {
    "author": "Servizio Statistica",
    "author_email": "serv.statistica@provincia.tn.it",

    "extras": {
        "Aggiornamento": "Annuale",
        "Codifica Caratteri": "UTF-8",
        "Copertura Geografica": "Provincia di Trento",
        "Copertura Temporale (Data di inizio)": "1985-01-01T00:00:00",
        "Data di aggiornamento": "2012-01-01T00:00:00",
        "Data di pubblicazione": "2013-06-16T11:45:26.324274",
        "Titolare": "Provincia Autonoma di Trento"
    },

    "groups": [
        # todo: fill with ids of the previous groups
    ],

    "license_id": "cc-by",
    "maintainer": "Servizio Statistica",
    "maintainer_email": "serv.statistica@provincia.tn.it",
    "name": "presenza-media-in-alberghi-comparati-e-alloggi",
    "notes": "**Presenza media in alberghi, comparati e alloggi**",

    # todo: fill with if of the previous organization
    # "owner_org": "4c3d9698-2f8e-49fa-ab6b-7b572862e36d",

    "private": False,

    "resources": [
        {
            "description": "Presenza media in alberghi, comparati e alloggi",
            "format": "JSON",
            "hash": "",
            "mimetype": "text/html",
            "mimetype_inner": None,
            "name": "presenza-media-in-alberghi-comparati-e-alloggi",
            "position": 0,
            "resource_type": "api",
            "size": "279202",
            "url": "http://statistica.example.com/dataset-242.json",
            "url_type": None,
        },
        {
            "description": "Presenza media in alberghi, comparati e alloggi",
            "format": "CSV",
            "hash": "706d4e38e6c1d167e5e9ef1a3a8358a581bcf157",
            "mimetype": "text/csv",
            "mimetype_inner": None,
            "name": "presenza-media-in-alberghi-comparati-e-alloggi",
            "position": 1,
            "resource_type": "file",
            "size": "78398",
            "url": "http://statistica.example.com/dataset-242.csv",
            "url_type": None,
        },
        {
            "description": ("Media giornaliera di presenze in strutture "
                            "alberghiere, complementari e alloggi"),
            "format": "JSON",
            "hash": "",
            "mimetype": "application/json",
            "mimetype_inner": None,
            "name": ("media-giornaliera-di-presenze-in-strutture-alberghiere-"
                     "complementari-e-alloggi"),
            "position": 2,
            "resource_type": "api",
            "size": None,
            "url": "http://statistica.example.com/dataset-242-d.json",
            "url_type": None,
        },
        {
            "description": ("Media giornaliera di presenze in strutture "
                            "alberghiere, complementari e alloggi"),
            "format": "CSV",
            "hash": "9d05c7959b0fae4b13b00e81dd15a0bf9e3d707a",
            "mimetype": "text/csv",
            "mimetype_inner": None,
            "name": ("media-giornaliera-di-presenze-in-strutture-alberghiere-"
                     "complementari-e-alloggi"),
            "position": 3,
            "resource_type": "file",
            "size": "8332",
            "url": "http://statistica.example.com/dataset-242-d.csv",
            "url_type": None,
        }
    ],
    "state": "active",
    "tags": [
        "servizi",
        "settori economici"
    ],
    "title": "Presenza media in alberghi, comparati e alloggi",
    "type": "dataset",
    "url": "http://www.statistica.provincia.tn.it",
}


def test_data_loss_on_update(ckan_client):
    """
    Strategy:

    1. We create the dataset
    2. We retrieve the dataset and keep it for later
    3. We send an update
    4. We check that update affected only the desired keys
    """
    our_dataset = prepare_dataset(ckan_client, OUR_DATASET)

    ## Create the dataset
    created_dataset = ckan_client.post_dataset(our_dataset)
    dataset_id = created_dataset['id']

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


def test_updating_extras(ckan_client):
    ## First, create the dataset
    our_dataset = prepare_dataset(ckan_client, OUR_DATASET)
    created_dataset = ckan_client.post_dataset(our_dataset)
    dataset_id = created_dataset['id']

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


def test_extras_bad_behavior(ckan_client):
    dataset = {
        'name': gen_dataset_name(),
        'title': "Test dataset",
        'extras': {'a': 'aa', 'b': 'bb', 'c': 'cc'},
    }
    created = ckan_client.post_dataset(dataset)
    dataset_id = created['id']
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


##----------------------------------------------------------------------
## Utility functions
##----------------------------------------------------------------------

def prepare_dataset(ckan_client, base):
    our_dataset = copy.deepcopy(base)

    ## We need to change name, as it must be unique

    our_dataset['name'] = 'dataset-{0}'.format(int(time.time()))

    ## Figure out the groups

    our_groups_ids = []

    all_groups = list(ckan_client.iter_groups())
    all_groups_by_name = dict((x['name'], x) for x in all_groups)

    for group in OUR_GROUPS:
        _group = all_groups_by_name.get(group['name'])
        if _group is None:
            ## Create group
            _group = ckan_client.post_group(group)
        our_groups_ids.append(_group['id'])

    our_dataset['groups'] = our_groups_ids

    ## Figure out the organization

    our_org_id = None

    all_orgs = list(ckan_client.iter_organizations())
    all_orgs_by_name = dict((x['name'], x) for x in all_orgs)

    if OUR_ORG['name'] in all_orgs_by_name:
        _org = all_orgs_by_name[OUR_ORG['name']]
    else:
        _org = ckan_client.post_organization(OUR_ORG)
    our_org_id = _org['id']

    our_dataset['owner_org'] = our_org_id

    return our_dataset


def check_dataset(dataset, expected):
    """
    Check that a dataset matches the expected one
    """
    for field in DATASET_FIELDS['core']:
        assert dataset[field] == expected[field]

    assert dataset['extras'] == expected['extras']
    assert sorted(dataset['groups']) == sorted(expected['groups'])

    ## Check resources
    _dataset_resources = dict((x['url'], x) for x in dataset['resources'])
    _expected_resources = dict((x['url'], x) for x in expected['resources'])

    assert len(_dataset_resources) == len(dataset['resources'])
    assert len(_expected_resources) == len(expected['resources'])
    assert len(_dataset_resources) == len(_expected_resources)

    assert sorted(_dataset_resources.iterkeys()) \
        == sorted(_expected_resources.iterkeys())

    for key in _dataset_resources:
        _resource = _dataset_resources[key]
        _expected = _expected_resources[key]
        for field in RESOURCE_FIELDS['core']:
            assert _resource[field] == _expected[field]


def gen_dataset_name():
    charset = string.ascii_lowercase + string.digits
    code = ''.join(random.choice(charset) for _ in xrange(10))
    return "dataset-{0}".format(code)
