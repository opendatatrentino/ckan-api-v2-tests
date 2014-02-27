"""
Ckan API client
"""

from collections import namedtuple
import copy
import functools
import json
import urlparse
import warnings

import requests


DATASET_FIELDS = {
    'core': [
        'author', 'author_email', 'license_id', 'maintainer',
        'maintainer_email', 'name', 'notes', 'owner_org', 'private', 'state',
        'type', 'url'
    ],
    'cruft': [
        'ckan_url', 'creator_user_id', 'isopen', 'license', 'license_title',
        'license_url', 'metadata_created', 'metadata_modified',
        'num_resources', 'num_tags', 'organization', 'ratings_average',
        'ratings_count', 'revision_id', 'version'
    ],
    'keys': ['id'],
    'special': ['extras', 'groups', 'relationships', 'resources'],
}

RESOURCE_FIELDS = {
    'core': [
        'description', 'format', 'mimetype', 'mimetype_inner', 'name',
        'position', 'resource_type', 'size', 'url', 'url_type',
    ],
    'cruft': [
        'cache_last_updated', 'cache_url', 'created', 'hash', 'last_modified',
        'package_id', 'resource_group_id', 'webstore_last_updated',
        'webstore_url',
    ],
    'keys': ['id'],
    'special': [],
}

GROUP_FIELDS = {
    'core': [
        'approval_status', 'description', 'image_display_url', 'image_url',
        'is_organization', 'name', 'state', 'title', 'type',
    ],
    'cruft': [
        'created', 'display_name', 'package_count', 'packages', 'revision_id',
    ],
    'keys': ['id'],
    'special': ['extras', 'groups', 'tags', 'users'],  # packages?
}


class SuppressExceptionIf(object):
    def __init__(self, cond):
        self.cond = cond

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value is None:
            return
        if callable(self.cond):
            # If the callable returns True, exception
            # will be suppressed
            return self.cond(exc_value)
        return self.cond


class HTTPError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message

    def __str__(self):
        return "HTTPError [{0}]: {1}".format(self.status_code, self.message)


class BadApiError(Exception):
    """Exception used to mark bad behavior from the API"""
    pass


class BadApiWarning(UserWarning):
    """Warning to mark bad behavior from the API"""
    pass


class SomethingWentWrong(Exception):
    """
    Exception to indicate that something went wrong during
    a data import.. :(
    """
    pass


##----------------------------------------------------------------------
## Typechecker validators are used here as the only way to
## try make some order in this mess of API returning unexpected things.
## They might come in handy when refactoring Ckan code too, btw..
##----------------------------------------------------------------------
## WARNING! This is not a Python good practice for normal usage, do not
##          copy code from this thing,,
##----------------------------------------------------------------------


def validate(validator, value):
    if validator is None:
        return True
    if isinstance(validator, type):
        return isinstance(value, validator)
    if callable(validator):
        return validator(value)
    raise TypeError("Invalid validator type: {0}".format(type(validator)))


def check_arg_types(*a_types, **kw_types):
    def decorator(func):
        @functools.wraps(func)
        def wrapped(*a, **kw):
            # Validate arguments
            for validator, value in zip(a_types, a):
                if not validate(validator, value):
                    raise TypeError("Invalid argument type")

            # Validate keyword arguments
            for key, validator in kw_types.iteritems():
                if key not in kw:
                    continue
                value = kw[key]
                if not validate(validator, value):
                    raise TypeError("Invalid argument type")

            # Actually call the function
            return func(*a, **kw)
        return wrapped
    return decorator


def check_retval(checker):
    def decorator(func):
        @functools.wraps(func)
        def wrapped(*a, **kw):
            retval = func(*a, **kw)
            if not validate(checker, retval):
                raise TypeError("Invalid return value")
            return retval
        return wrapped
    return decorator


def is_list_of(type_):
    def inner(obj):
        if not isinstance(obj, list):
            raise TypeError("Object is not a list")

        if not all(isinstance(x, type_) for x in obj):
            raise TypeError("A value in the list is not a {0!r}".format(type_))

        return True
    return inner


def is_dict_of(key_type, value_type):
    def inner(obj):
        if not isinstance(obj, dict):
            raise TypeError("Object is not a dict")

        for key, value in obj.iteritems():
            validate(key_type, key)
            validate(value_type, value)

        return True
    return inner


def validate_dataset(dataset):
    """Do some checking on a dataset object"""
    # todo: what about extra fields? should be warn in case we have some?
    if not isinstance(dataset, dict):
        raise ValueError("Dataset must be a dict")

    if 'extras' in dataset:
        if not isinstance(dataset['extras'], dict):
            raise ValueError("Dataset extras must be a dict")
        for key, value in dataset['extras'].iteritems():
            if not isinstance(key, basestring):
                raise ValueError("Extras keys must be strings")
            if (value is not None) and (not isinstance(value, basestring)):
                raise ValueError("Extras values must be strings (or None)")

    if 'groups' in dataset:
        if not isinstance(dataset['groups'], list):
            raise ValueError("Dataset groups must be a list")
        if not all(isinstance(x, basestring) for x in dataset['groups']):
            raise ValueError("Dataset groups must be a list of strings")

    if 'resources' in dataset:
        if not isinstance(dataset['resources'], list):
            raise ValueError("Resources must be a list")
        if not all(isinstance(x, dict) for x in dataset['resources']):
            raise ValueError("Dataset resources must be a list of dicts")
        # todo: validate each single resource object too..?

    return True  # Validation passed


##----------------------------------------------------------------------
## Actual client classes
##----------------------------------------------------------------------


class CkanClient(object):
    def __init__(self, base_url, api_key=None):
        self.base_url = base_url
        self.api_key = api_key

    @property
    def anonymous(self):
        return CkanClient(self.base_url)

    def request(self, method, path, **kwargs):
        headers = kwargs.get('headers') or {}
        kwargs['headers'] = headers

        ## Update headers for authorization
        if self.api_key is not None:
            headers['Authorization'] = self.api_key

        ## Serialize data to json, if not already
        if 'data' in kwargs:
            if not isinstance(kwargs['data'], basestring):
                kwargs['data'] = json.dumps(kwargs['data'])
                headers['content-type'] = 'application/json'

        if isinstance(path, (list, tuple)):
            path = '/'.join(path)

        url = urlparse.urljoin(self.base_url, path)
        response = requests.request(method, url, **kwargs)
        if not response.ok:
            ## todo: attach message, if any available..
            ## todo: we should find a way to figure out how to attach
            ##       original text message to the exception
            ##       as it might be: json string, part of json object,
            ##       part of html document
            raise HTTPError(response.status_code,
                            "Error while performing request")

        return response

    ##============================================================
    ## Datasets
    ##============================================================

    @check_retval(is_list_of(basestring))
    def list_datasets(self):
        path = '/api/2/rest/dataset'
        response = self.request('GET', path)
        return response.json()

    def iter_datasets(self):
        for ds_id in self.list_datasets():
            yield self.get_dataset(ds_id)

    @check_arg_types(None, basestring)
    @check_retval(dict)
    def get_dataset(self, dataset_id):
        path = '/api/2/rest/dataset/{0}'.format(dataset_id)
        response = self.request('GET', path)
        return response.json()

    @check_arg_types(None, dict)
    @check_retval(dict)
    def post_dataset(self, dataset):
        path = '/api/2/rest/dataset'
        response = self.request('POST', path, data=dataset)
        return response.json()

    @check_arg_types(None, validate_dataset)
    @check_retval(dict)
    def create_dataset(self, dataset):
        """
        High-level function to create datasets.
        Just a wrapper around post_dataset() right now, but
        might come in handy in the future to add workarounds..
        """
        return self.post_dataset(dataset)

    @check_arg_types(None, basestring, validate_dataset)
    @check_retval(dict)
    def put_dataset(self, dataset_id, dataset):
        """
        PUT a dataset (for update).

        .. warning::

            ``update_dataset()`` should be used instead, in normal cases,
            as it automatically takes care of a lot of needed workarounds
            to prevent data loss.

            Calling this method directly is almost never adviced or required.
        """
        path = '/api/2/rest/dataset/{0}'.format(dataset_id)
        response = self.request('PUT', path, data=dataset)
        return response.json()

    @check_arg_types(None, basestring, validate_dataset)
    @check_retval(dict)
    def update_dataset(self, dataset_id, updates):
        """
        Trickery to perform a safe partial update of a dataset.

        WARNING: This method contains tons of hacks to try and fix
                 major issues with the API.

        In particular, remember that:

        - Extras are updated incrementally. To delete a key, just set
          it to None.

        - Groups might accept objects too, but behavior is quite undefined
          in that case.. so don't do that.

        Fixes that are in place:

        - If the extras field is not specified on update, all extras will
          be deleted. To prevent this, we default it to {}.

        - If the groups field is not specified on update, all groups will
          be removed. To prevent this, we default it to [].
        """

        ##=====[!!]=========== IMPORTANT NOTE ===============[!!]=====
        ## - "core" fields seems to be kept
        ## - ..but "extras" need to be passed back again
        ## - ..same behavior for groups: no way to delete them,
        ##   apparently.. a part from flushing 'em all by omitting
        ##   the field...
        ## - resources?
        ## - relationships?
        ##============================================================

        original_dataset = self.get_dataset(dataset_id)

        ## Dictionary holding the actual data to be sent
        ## for performing the update
        updates_dict = {'id': dataset_id}

        ##############################################################
        ## Core fields
        ##------------------------------------------------------------

        for field in DATASET_FIELDS['core']:
            if field in updates:
                updates_dict[field] = updates[field]
            else:
                updates_dict[field] = original_dataset[field]

        ##############################################################
        ## Extras fields
        ##------------------------------------------------------------

        ##=====[!!]=========== IMPORTANT NOTE ===============[!!]=====
        ## WARNING! Behavior here is quite "funky":
        ##
        ## db: {'a': 'aa', 'b': 'bb', 'c': 'cc'}
        ## update: (no extras key)
        ## result: {}
        ##
        ## db: {'a': 'aa', 'b': 'bb', 'c': 'cc'}
        ## update: {'a': 'foo'}
        ## result: {'a': 'foo', 'b': 'bb', 'c': 'cc'}
        ##
        ## db: {'a': 'aa', 'b': 'bb', 'c': 'cc'}
        ## update: {}
        ## db: {'a': 'aa', 'b': 'bb', 'c': 'cc'}
        ##============================================================

        EXTRAS_FIELD = 'extras'  # to avoid confusion

        updates_dict[EXTRAS_FIELD] = {}

        if EXTRAS_FIELD in updates:
            # Notes: setting a field to 'None' will delete it.
            updates_dict[EXTRAS_FIELD].update(updates[EXTRAS_FIELD])

        ##############################################################
        ## These fields need to be passed again or it will just
        ## be flushed..
        ##------------------------------------------------------------

        FIELDS_THAT_NEED_TO_BE_PASSED = [
            'resources', 'relationships'
        ]
        for field in FIELDS_THAT_NEED_TO_BE_PASSED:
            if field in updates:
                updates_dict[field] = updates[field]
            else:
                updates_dict[field] = original_dataset[field]

        ##############################################################
        ## Update groups
        ##------------------------------------------------------------

        ##=====[!!]=========== IMPORTANT NOTE ===============[!!]=====
        ## - If the groups key is omitted, all groups are deleted
        ## - It seems to be possible to specify groups as objects too,
        ##   but exact behavior is uncertain, so we only accept
        ##   strings here (ids), otherwise object will not pass
        ##   validation.
        ##============================================================

        updates_dict['groups'] = (
            updates['group']
            if 'group' in updates
            else original_dataset['groups'])

        ##############################################################
        ## todo: update relationships
        ##------------------------------------------------------------

        # todo: WTF are relationships?

        ##############################################################
        ## todo: update tags
        ##------------------------------------------------------------

        ##############################################################
        ## todo: update resources
        ##------------------------------------------------------------

        ##############################################################
        ## Actually perform the update
        ##------------------------------------------------------------

        return self.put_dataset(dataset_id, updates_dict)

    @check_arg_types(None, basestring, ignore_404=bool)
    def delete_dataset(self, dataset_id, ignore_404=True):
        ign404 = SuppressExceptionIf(
            lambda e: ignore_404 and (e.status_code == 404))
        path = '/api/2/rest/dataset/{0}'.format(dataset_id)
        with ign404:
            self.request('DELETE', path, data={'id': dataset_id})

    ##============================================================
    ## Groups
    ##============================================================

    ##=====[!!]=========== IMPORTANT NOTE ===============[!!]=====
    ## BEWARE! API v2 only considers actual groups, organizations
    ## are not handled / returned by this one!
    ##============================================================

    @check_retval(is_list_of(basestring))
    def list_groups(self):
        path = '/api/2/rest/group'
        response = self.request('GET', path)
        return response.json()

    def iter_groups(self):
        all_groups = self.list_groups()
        for group_id in all_groups:
            yield self.get_group(group_id)

    @check_arg_types(None, basestring)
    @check_retval(dict)
    def get_group(self, group_id):
        path = '/api/2/rest/group/{0}'.format(group_id)
        response = self.request('GET', path)
        return response.json()

    @check_arg_types(None, dict)
    @check_retval(dict)
    def post_group(self, group):
        path = '/api/2/rest/group'
        response = self.request('POST', path, data=group)
        return response.json()

    @check_arg_types(None, basestring, dict)
    @check_retval(dict)
    def put_group(self, group_id, group):
        path = '/api/2/rest/group/{0}'.format(group_id)
        response = self.request('PUT', path, data=group)
        data = response.json()
        return data

    @check_arg_types(None, basestring, ignore_404=bool)
    def delete_group(self, group_id, ignore_404=True):
        ign404 = SuppressExceptionIf(
            lambda e: ignore_404 and (e.status_code == 404))
        path = '/api/2/rest/group/{0}'.format(group_id)
        with ign404:
            self.request('DELETE', path)
        path = '/api/3/action/group_purge'
        with ign404:
            self.request('POST', path, data={'id': group_id})

    @check_arg_types(None, basestring, dict)
    @check_retval(dict)
    def update_group(self, group_id, updates):
        """
        Trickery to perform a safe partial update of a group.
        """

        original_group = self.get_group(group_id)

        ## Dictionary holding the actual data to be sent
        ## for performing the update
        updates_dict = {'id': group_id}

        ##------------------------------------------------------------
        ## Core fields
        ##------------------------------------------------------------

        for field in GROUP_FIELDS['core']:
            if field in updates:
                updates_dict[field] = updates[field]
            else:
                updates_dict[field] = original_group[field]

        ##------------------------------------------------------------
        ## Extras fields
        ##------------------------------------------------------------

        ## We assume the same behavior here as for datasets..
        ## See update_dataset() for more details.

        EXTRAS_FIELD = 'extras'  # to avoid confusion

        updates_dict[EXTRAS_FIELD] = {}

        if EXTRAS_FIELD in updates:
            # Notes: setting a field to 'None' will delete it.
            updates_dict[EXTRAS_FIELD].update(updates[EXTRAS_FIELD])

        ## These fields need to be passed again or they will just
        ## be flushed..
        FIELDS_THAT_NEED_TO_BE_PASSED = [
            'groups',  # 'tags'?
        ]
        for field in FIELDS_THAT_NEED_TO_BE_PASSED:
            if field in updates:
                updates_dict[field] = updates[field]
            else:
                updates_dict[field] = original_group[field]

        ## Actually perform the update
        ##----------------------------------------

        return self.put_group(group_id, updates_dict)

    @check_arg_types(None, dict)
    @check_retval(dict)
    def upsert_group(self, group):
        """
        Try to "upsert" a group, by name.

        This will:
        - retrieve the group
        - if the group['state'] == 'deleted', try to restore it
        - if something changed, update it

        :return: the group object
        """

        # Try getting group..
        if 'id' in group:
            raise ValueError("You shouldn't specify a group id already!")

        ## Get the group
        ## Groups should be returned by name too (hopefully..)
        try:
            _retr_group = self.get_group(group['name'])
        except HTTPError:
            _retr_group = None

        if _retr_group is None:
            ## Just insert the group and return its id
            return self.post_group(group)

        updates = {}
        if _retr_group['state'] == 'deleted':
            ## We need to make it active again!
            updates['state'] = 'active'

        ## todo: Check if we have differences, before updating!

        updated_dict = copy.deepcopy(group)
        updated_dict.update(updates)

        return self.update_group(_retr_group['id'], updated_dict)

    ##============================================================
    ## Organizations
    ##============================================================

    ## --- [!!] NOTE ---------------------------------------------
    ## We need to fallback to api v3 here, as v2 doesn't support
    ## doing things with organizations..
    ##------------------------------------------------------------

    @check_retval(is_list_of(basestring))
    def list_organizations(self):
        path = '/api/3/action/organization_list'
        response = self.request('GET', path)
        return response.json()['result']

    def iter_organizations(self):
        for org_id in self.list_organizations():
            yield self.get_organization(org_id)

    @check_arg_types(None, basestring)
    @check_retval(dict)
    def get_organization(self, organization_id):
        path = '/api/3/action/organization_show?id={0}'.format(organization_id)
        response = self.request('GET', path)
        return response.json()['result']

    @check_retval(dict)
    def post_organization(self, organization):
        path = '/api/3/action/organization_create'
        response = self.request('POST', path, data=organization)
        return response.json()['result']

    @check_retval(dict)
    def put_organization(self, organization_id, organization):
        """Warning! with api v3 we need to use POST!"""
        organization['id'] = organization_id
        path = '/api/3/action/organization_update'
        response = self.request('POST', path, data=organization)
        return response.json()['result']

    @check_arg_types(None, basestring, dict)
    @check_retval(dict)
    def update_organization(self, organization_id, updates):
        """
        Trickery to perform a safe partial update of a organization.
        """

        original_organization = self.get_organization(organization_id)

        ## Dictionary holding the actual data to be sent
        ## for performing the update
        updates_dict = {'id': organization_id}

        ##------------------------------------------------------------
        ## Core fields
        ##------------------------------------------------------------

        for field in GROUP_FIELDS['core']:
            if field in updates:
                updates_dict[field] = updates[field]
            else:
                updates_dict[field] = original_organization[field]

        ##------------------------------------------------------------
        ## Extras fields
        ##------------------------------------------------------------

        ## We assume the same behavior here as for datasets..
        ## See update_dataset() for more details.

        # EXTRAS_FIELD = 'extras'  # to avoid confusion

        # # updates_dict[EXTRAS_FIELD] = {}
        # updates_dict[EXTRAS_FIELD] = [{}]  # BUG!!

        # if EXTRAS_FIELD in updates:
        #     # Notes: setting a field to 'None' will delete it.
        #     # updates_dict[EXTRAS_FIELD].update(updates[EXTRAS_FIELD])
        #     updates_dict[EXTRAS_FIELD] = [updates[EXTRAS_FIELD]]

        ## Fuck this

        ## These fields need to be passed again or they will just
        ## be flushed..
        FIELDS_THAT_NEED_TO_BE_PASSED = [
            'groups',  # 'tags'?
        ]
        for field in FIELDS_THAT_NEED_TO_BE_PASSED:
            if field in updates:
                updates_dict[field] = updates[field]
            else:
                updates_dict[field] = original_organization[field]

        ## Actually perform the update
        ##----------------------------------------

        return self.put_organization(organization_id, updates_dict)

    @check_arg_types(None, dict)
    @check_retval(dict)
    def upsert_organization(self, organization):
        """
        Try to "upsert" a organization, by name.

        This will:
        - retrieve the organization
        - if the organization['state'] == 'deleted', try to restore it
        - if something changed, update it

        :return: the organization object
        """

        # Try getting organization..
        if 'id' in organization:
            raise ValueError(
                "You shouldn't specify a organization id! "
                "Name is going to be used as upsert key.")

        ## Get the organization
        ## Groups should be returned by name too (hopefully..)
        try:
            _retr_organization = self.get_organization(organization['name'])
        except HTTPError:
            _retr_organization = None

        if _retr_organization is None:
            ## Just insert the organization and return its id
            return self.post_organization(organization)

        updates = {}
        if _retr_organization['state'] == 'deleted':
            ## We need to make it active again!
            updates['state'] = 'active'

        ## todo: Check if we have differences, before updating!

        updated_dict = copy.deepcopy(organization)
        updated_dict.update(updates)

        return self.update_organization(_retr_organization['id'], updated_dict)

    def delete_organization(self, organization_id, ignore_404=True):
        ign404 = SuppressExceptionIf(
            lambda e: ignore_404 and (e.status_code == 404))
        path = '/api/3/action/organization_delete'
        with ign404:
            self.request('PUT', path, data={'id': organization_id})
        path = '/api/3/action/organization_purge'
        with ign404:
            self.request('POST', path, data={'id': organization_id})

    ##============================================================
    ## Licenses
    ##============================================================

    @check_retval(is_list_of(dict))
    def list_licenses(self):
        path = '/api/2/rest/licenses'
        response = self.request('GET', path)
        return response.json()

    ##============================================================
    ## Tags
    ##============================================================

    @check_retval(is_list_of(basestring))
    def list_tags(self):
        path = '/api/2/rest/tag'
        response = self.request('GET', path)
        return response.json()

    @check_retval(is_list_of(dict))
    def list_datasets_with_tag(self, tag_id):
        path = '/api/2/rest/tag/{0}'.format(tag_id)
        response = self.request('GET', path)
        return response.json()

    def iter_datasets_with_tag(self, tag_id):
        for dataset_id in self.list_datasets_with_tag():
            yield self.get_dataset(dataset_id)


IDPair = namedtuple('IDPair', ['source_id', 'ckan_id'])


class CkanDataImportClient(object):
    """
    Client to handle importing data in ckan

    Needs:

    - Synchronize a collection of datasets with a filtered
      subset of Ckan datasets

    - Also upsert "dependency" objects, such as groups and
      organizations, in order to be able to link them with newly-created
      datasets.

    Notes:

    - dataset['groups'] will be generated by mapping names in
      dataset['group_names'] to ckan ids of the same groups

    - dataset['owner_org'] will be the ckan id coresponding to
      the name from dataset['organization_name']
    """

    source_field_name = '_harvest_source'
    source_id_field_name = '_harvest_source_id'

    def __init__(self, base_url, api_key, source_name):
        """
        :param base_url: passed to CkanClient constructor
        :param api_key: passed to CkanClient constructor
        :param source_name: identifier of the data source
        """
        self.client = CkanClient(base_url, api_key)
        self.source_name = source_name

    def sync_data(self, data, double_check=True):
        """
        Import data into Ckan

        :param data:
            Dict (or dict-like) mapping object types to
            dicts (key/object) (key is the original key)
        """

        ## Used to keep track of the executed operations,
        ## mainly used to generate reports..
        result = {
            'created': [],
            'updated': [],
            'deleted': [],
        }

        ##------------------------------------------------------------
        ## Retrieve current database state
        ##------------------------------------------------------------

        used_dataset_names = set()
        our_datasets_from_ckan = {}  # key: source id

        for dataset in self.client.iter_datasets():
            if self._is_our_dataset(dataset):
                key = dataset['extras'][self.source_id_field_name]
                our_datasets_from_ckan[key] = dataset

            used_dataset_names.add(dataset['name'])

        ##------------------------------------------------------------
        ## Utility functions
        ##------------------------------------------------------------

        def _prepare_group(group):
            # The original id is moved into name.
            # Better not messing with these fields..
            group.pop('id', None)
            group.pop('name', None)
            return group

        def _prepare_organization(obj):
            return _prepare_group(obj)

        ##------------------------------------------------------------
        ## Maps 'source_id' -> 'ckan_id' for
        ## organizations and groups.
        ##------------------------------------------------------------

        groups_map = self._ensure_groups(
            dict(
                (k, _prepare_group(g))
                for k, g in data['group'].iteritems()
            )
        )

        organizations_map = self._ensure_organizations(
            dict(
                (k, _prepare_organization(g))
                for k, g in data['organization'].iteritems()
            )
        )

        ##------------------------------------------------------------
        ## Obtain differences between datasets
        ##------------------------------------------------------------

        dataset_diffs = self._verify_datasets(data['dataset'])

        def _prepare_dataset(dataset):
            """
            Prepare a dataset from an external source for insertion in ckan
            """
            ## Let's left the original untouched
            dataset = copy.deepcopy(dataset)

            ## Pop the id, as it is not to be used as key
            ## - for creates, id will be generated
            ## - for updates, id is passed separately
            source_id = dataset.pop('id')

            ## Note: we cannot handle name change here, as we don't
            ## know whether the dataset is new or going to be updated

            ## Map group names to ids
            dataset['groups'] = [
                groups_map[x]
                for x in (dataset.get('group_names') or [])
                if x in groups_map]

            ## Map organization name to id
            dataset['owner_org'] = organizations_map.get(
                dataset.get('owner_org'))

            ## We need to mark this dataset as ours
            if 'extras' not in dataset:
                dataset['extras'] = {}
            dataset['extras'][self.source_field_name] = self.source_name
            dataset['extras'][self.source_id_field_name] = source_id

            return dataset

        ##----------------------------------------
        ## Apply creates
        ##----------------------------------------

        for idpair in dataset_diffs['missing']:
            ## Create dataset with idpair.source_id
            dataset = _prepare_dataset(data['dataset'][idpair.source_id])

            # todo: we need to make sure we use a unique name
            #       for the newly created dataset!

            # -> keep a set of used names and hope for the best..

            # todo: how to generate default name, if not specified?

            created = self.client.create_dataset(dataset)

            ## Add id in the list of created datasets
            result['created'].append(
                IDPair(source_id=idpair.source_id,
                       ckan_id=created['id']))

        ##----------------------------------------
        ## Apply updates
        ##----------------------------------------

        for idpair in dataset_diffs['updated']:
            assert idpair.source_id is not None
            assert idpair.ckan_id is not None

            ## Update dataset
            dataset = _prepare_dataset(data['dataset'][idpair.source_id])
            dataset.pop('name', None)

            # todo: we should ignore name changes, as they might cause
            #       Unique key problems.. plus, users might have
            #       customized them

            # todo: should we change groups / organizations?
            #       Best thing would be to make this configurable

            updated = self.client.update_dataset(idpair.ckan_id, dataset)
            assert updated['id'] == idpair.ckan_id

            # todo: check that the update was successful?
            # (check might be done by update_dataset() too..)

            ## Add id in the list of updated datasets
            result['updated'].append(idpair)

        ##----------------------------------------
        ## Apply removals
        ##----------------------------------------

        for idpair in dataset_diffs['deleted']:
            ## Delete dataset
            assert idpair.source_id is None
            assert idpair.ckan_id is not None
            self.client.delete_dataset(idpair.ckan_id)

            result['deleted'].append(idpair)

        ##----------------------------------------
        ## Double-check
        ##----------------------------------------

        if double_check:
            errors = 0
            differences = self._verify_datasets(data['dataset'])

            if len(differences['missing']) > 0:
                errors += 1
                warnings.warn("We still have ({0}) datasets marked as missing"
                              .format(len(differences['missing'])))

            #### TODO: RE-ENABLE THIS CHECK!!! ####

            if len(differences['updated']) > 0:
                # errors += 1
                warnings.warn("We still have ({0}) datasets marked as updated"
                              .format(len(differences['updated'])))

            if len(differences['deleted']) > 0:
                errors += 1
                warnings.warn("We still have ({0}) datasets marked as deleted"
                              .format(len(differences['deleted'])))

            # todo: check groups/orgs too!

            if errors > 0:
                raise SomethingWentWrong(
                    "Something went wrong while performing updates.")

    def _is_our_dataset(self, dataset):
        """
        Check whether a dataset is associated with this harvest source
        """
        try:
            dataset_source = dataset['extras'][self.source_field_name]
        except KeyError:
            return False
        return dataset_source == self.source_name

    def _find_our_datasets(self):
        """
        Iterate dataset, yield only the ones that match this source
        """
        for dataset in self.client.iter_datasets():
            if self._is_our_dataset(dataset):
                yield dataset

    def _check_dataset(self, dataset, expected):
        """
        Check whether dataset is up to date with expected..
        """

        # todo: should ignore names as they might change..
        # todo: we need to make sure we are getting group/org **ids**,
        #       not names

        for field in DATASET_FIELDS['core']:
            if field in expected:
                if dataset[field] != expected[field]:
                    return False

        if 'extras' in expected:
            if dataset['extras'] != expected['extras']:
                return False

        if 'groups' in expected:
            if sorted(dataset['groups']) != sorted(expected['groups']):
                return False

        ## Check resources
        if 'resources' in expected:
            _dataset_resources = dict((x['url'], x)
                                      for x in dataset['resources'])
            _expected_resources = dict((x['url'], x)
                                       for x in expected['resources'])

            if len(_dataset_resources) != len(dataset['resources']):
                return False
            if len(_expected_resources) != len(expected['resources']):
                return False
            if len(_dataset_resources) != len(_expected_resources):
                return False

            if sorted(_dataset_resources.iterkeys()) \
                    != sorted(_expected_resources.iterkeys()):
                return False

            for key in _dataset_resources:
                _resource = _dataset_resources[key]
                _expected = _expected_resources[key]
                for field in RESOURCE_FIELDS['core']:
                    if _resource[field] != _expected[field]:
                        return False

        ## Need to check relationships (wtf is that, btw?)

        return True

    def _check_group(self, group, expected):
        """
        Make sure all the data in ``expected`` is also in ``group``
        """
        return True

    def _check_organization(self, organization, expected):
        """
        Make sure all the data in ``expected`` is also in ``organization``
        """
        return True

    def _verify_datasets(self, datasets):
        """
        Compare differences between current state and desired state
        of the datasets collection.

        :param datasets:
            A dictionary (or dict-like) mapping {<source-id>: <dataset>}

        :return: a dict with following keys:
            - missing:
                List of IDPair of datasets that are in ``datasets`` but
                not in Ckan
            - up_to_date:
                List of IDPair of datasets that are both in ``datasets``
                and Ckan, and that are up to date.
            - updated:
                List of IDPair of datasets that are both in ``datasets``
                and Ckan, but are somehow different.
            - deleted:
                List of IDPair of datasets that are in Ckan but not
                in ``datasets``, and thus should be deleted.

            Each 'IDPair' is a named tuple with (source_id, ckan_id) keys.
        """

        ## Dictionary mapping {<source_id>: <dataset>} for datasets in Ckan,
        ## filtered on source name.
        our_datasets = dict(
            (x['extras'][self.source_id_field_name], x)
            for x in self._find_our_datasets())

        # ## Create map of {'source_id': 'ckan_id'}
        # dataset_ids = ((k, v['id']) for k, v in our_datasets.iteritems())

        new_datasets = []
        up_to_date_datasets = []
        updated_datasets = []

        for source_id, dataset in datasets.iteritems():

            ## Pop dataset from list, to leave only deleted ones
            existing_dataset = our_datasets.pop(source_id, None)

            if existing_dataset is None:
                ## This dataset is missing in the database,
                ## meaning we need to update it
                new_datasets.append(IDPair(source_id=source_id, ckan_id=None))

            else:
                ## Dataset is in Ckan, but is it up to date?

                _id_pair = IDPair(source_id=source_id,
                                  ckan_id=existing_dataset['id'])

                if not self._check_dataset(existing_dataset, dataset):
                    ## This dataset differs from the one in the database
                    updated_datasets.append(_id_pair)

                else:
                    ## This dataset is ok
                    up_to_date_datasets.append(_id_pair)

        ## Remaining datasets are in the db but have been deleted in
        ## the new collection.
        deleted_datasets = [IDPair(source_id=None, ckan_id=d['id'])
                            for k, d in our_datasets.iteritems()]

        return {
            'missing': new_datasets,
            'up_to_date': up_to_date_datasets,
            'updated': updated_datasets,
            'deleted': deleted_datasets,
        }

    @check_arg_types(None, is_dict_of(basestring, dict))
    @check_retval(is_dict_of(basestring, basestring))
    def _ensure_groups(self, groups):
        """
        Make sure the specified groups exist in Ckan.

        :param groups:
            a {'name': <group>} dict
        :return:
            a {'name': 'ckan-id'} dict
        """

        results = {}
        for group_name, group in groups.iteritems():
            group['name'] = group_name
            c_group = self.client.upsert_group(group)
            results[group_name] = c_group['id']
        return results

    @check_arg_types(None, is_dict_of(basestring, dict))
    @check_retval(is_dict_of(basestring, basestring))
    def _ensure_organizations(self, organizations):
        """
        Make sure the specified organizations exist in Ckan.

        :param organizations:
            a {'name': <group>} dict
        :return: a {'name':
            'ckan-id'} dict
        """

        results = {}
        for organization_name, organization in organizations.iteritems():
            organization['name'] = organization_name
            c_organization = self.client.upsert_organization(organization)
            results[organization_name] = c_organization['id']
        return results
