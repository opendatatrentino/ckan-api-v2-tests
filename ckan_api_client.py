"""
Ckan API client
"""

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


def _validate(validator, value):
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
                if not _validate(validator, value):
                    raise TypeError("Invalid argument type")

            # Validate keyword arguments
            for key, validator in kw_types.iteritems():
                if key not in kw:
                    continue
                value = kw[key]
                if not _validate(validator, value):
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
            if not _validate(checker, retval):
                raise TypeError("Invalid return value")
            return retval
        return wrapped
    return decorator


def is_list_of(type_):
    def inner(obj):
        if not isinstance(obj, list):
            return False
        return all(isinstance(x, type_) for x in obj)
    return inner


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

    @check_arg_types(None, basestring, dict)
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

    @check_arg_types(None, basestring, dict)
    @check_retval(dict)
    def update_dataset(self, dataset_id, updates):
        """
        Trickery to perform a safe partial update of a dataset.

        WARNING: This method contains tons of hacks to try and fix
                 major issues with the API.

        In particular, remember that:

        - Extras are updated incrementally. To delete a key, just set
          it to None.

        - Groups are merged. There is apparently no way to remove one.

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
        ## If the 'groups' key is omitted, all the groups relations
        ## are deleted.
        ## Otherwise, groups are just added. So the only way to
        ## "properly" remove a dataset from a group is to flush 'em
        ## all and then re-add only the needed ones...
        ##============================================================

        ## WTF --- WTF --- WTF --- WTF --- WTF --- WTF --- WTF --- WTF

        ## Note: behavior is still uncertain here!
        ## Looks like that if the passed-in groups are a subset, the
        ## extra ones are removed; but if there are extra ones, nothing
        ## is removed..

        ## WARNING
        updates_dict['groups'] = (
            updates['group']
            if 'group' in updates
            # else [])
            else original_dataset['groups'])

        ##############################################################
        ## todo: update relationships
        ##------------------------------------------------------------

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
        organization['id'] = organization_id
        path = '/api/3/action/organization_update'
        response = self.request('PUT', path, data=organization)
        return response.json()['result']

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


class CkanDataImportClient(object):
    """
    Client to handle importing data in ckan

    Needs:

    - Synchronize a collection of datasets with a filtered
      subset of Ckan datasets

    - Also upsert "dependency" objects, such as groups and
      organizations, in order to be able to link them with newly-created
      datasets.
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

        ## First, compare desired status with database status
        differences = self.verify_data(data)

        ## Map <name>:<id>
        group_name_to_id = {}
        org_name_to_id = {}

        for objid, obj in data['group']:
            obj = obj.copy()  # Let's leave the original untouched
            obj['name'] = objid
            group_name_to_id[objid] = self.upsert_group(obj)['id']

        for objid, obj in data['organization']:
            obj = obj.copy()  # Let's leave the original untouched
            obj['name'] = objid
            org_name_to_id[objid] = self.upsert_organization(obj)['id']

        ## Then, do everything necessary to bring database in
        ## the desired status (apply differences)

        ## Double-check
        ##----------------------------------------

        if double_check:
            errors = 0
            differences = self.verify_data(data)

            if len(differences['dataset']['new']) > 0:
                errors += 1
                warnings.warn("Some dataset has not been created correctly")

            if len(differences['dataset']['updated']) > 0:
                errors += 1
                warnings.warn("Some dataset has not been updated correctly")

            if len(differences['dataset']['deleted']) > 0:
                errors += 1
                warnings.warn("Some dataset has not been deleted correctly")

            # todo: check groups/orgs too!

            if errors > 0:
                raise SomethingWentWrong(
                    "Something went wrong while performing updates.")

    def _verify_datasets(self, datasets):
        our_datasets = dict(
            (x['extras'][self.source_field_name], x)
            for x in self._find_our_datasets())

        new_datasets = []
        updated_datasets = []

        for dataset_id, dataset in datasets.iteritems():
            existing_dataset = our_datasets.pop(dataset_id, None)
            if existing_dataset is None:
                ## This dataset is missing in the database
                new_datasets.append(dataset_id)
            elif not self._check_dataset(existing_dataset, dataset):
                ## This dataset differs from the one in the database
                updated_datasets.append(dataset_id)

        ## Remaining datasets are in the db but have been deleted in
        ## the new collection.
        deleted_datasets = list(our_datasets)

        return {
            'new': new_datasets,
            'updated': updated_datasets,
            'deleted': deleted_datasets,
        }

    def verify_data(self, data):
        """
        Verify that data in Ckan is consistent with the desired state.
        Return a dictionary representing differences.

        :return: a dict representing differences between the two collections.
            The first level is the collection name.
            Each seconf-level dictionary has the following keys:

            * new: ids of objects that are in data but not in ckan
            * deleted: ids of objects that are in ckan but not in data
            * updated: ids of objects that have been updated
        """

        return {
            'dataset': self._verify_datasets(data['datasets']),
            'group': {'new': [], 'updated': [], 'deleted': []},
            'organization': {'new': [], 'updated': [], 'deleted': []},
        }

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
        Make sure all the data in ``expected`` is also in ``dataset``
        """
        ## Need to check core fields for equality

        ## Need to check extras (compare dicts)

        ## Need to check resources: trickier, as we cannot rely on IDs
        ## -> use URLs as IDs

        ## Need to check groups
        ## -> Maybe, it would be better to leave them unchanged, just
        ##    make sure required groups are associated

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
