"""
Ckan API client
"""

import copy
import json
import urlparse

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


class ApiBadBehavior(Exception):
    """Exception used to mark bad behavior from the API"""
    pass


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
            raise HTTPError(response.status_code,
                            "Error while performing request")

        return response

    ##============================================================
    ## Datasets
    ##============================================================

    def list_datasets(self):
        path = '/api/2/rest/dataset'
        response = self.request('GET', path)
        return response.json()

    def iter_datasets(self):
        for ds_id in self.list_datasets():
            yield self.get_dataset(ds_id)

    def get_dataset(self, dataset_id):
        path = '/api/2/rest/dataset/{0}'.format(dataset_id)
        response = self.request('GET', path)
        return response.json()

    def post_dataset(self, dataset):
        path = '/api/2/rest/dataset'
        response = self.request('POST', path, data=dataset)
        return response.json()

    def put_dataset(self, dataset_id, dataset):
        path = '/api/2/rest/dataset/{0}'.format(dataset_id)
        response = self.request('PUT', path, data=dataset)
        return response.json()

    def update_dataset(self, dataset_id, updates):
        """
        Trickery to perform a safe partial update of a dataset.
        """

        ##-----[!!]----------- IMPORTANT NOTE ---------------[!!]-----
        ## - "core" fields seems to be kept
        ## - ..but "extras" need to be passed back again
        ## - groups?
        ## - resources?
        ## - relationships?
        ##------------------------------------------------------------

        original_dataset = self.get_dataset(dataset_id)

        ## Dictionary holding the actual data to be sent
        ## for performing the update
        updates_dict = {'id': dataset_id}

        ##--------------------------------------------------
        ## Core fields
        ##--------------------------------------------------

        for field in DATASET_FIELDS['core']:
            if field in updates:
                updates_dict[field] = updates[field]
            else:
                updates_dict[field] = original_dataset[field]

        ##--------------------------------------------------
        ## Extras fields
        ##--------------------------------------------------

        ##-----------( !! IMPORTANT NOTE !! )---------------
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
        ##--------------------------------------------------

        EXTRAS_FIELD = 'extras'  # to avoid confusion

        updates_dict[EXTRAS_FIELD] = {}

        if EXTRAS_FIELD in updates:
            # Notes: setting a field to 'None' will delete it.
            updates_dict[EXTRAS_FIELD].update(updates[EXTRAS_FIELD])

        ## These fields need to be passed again or it will just
        ## be flushed..
        FIELDS_THAT_NEED_TO_BE_PASSED = [
            'groups', 'resources', 'relationships'
        ]
        for field in FIELDS_THAT_NEED_TO_BE_PASSED:
            if field in updates:
                updates_dict[field] = updates[field]
            else:
                updates_dict[field] = original_dataset[field]

        ##--------------------------------------------------
        ## todo: update groups
        ##--------------------------------------------------

        ##--------------------------------------------------
        ## todo: update relationships
        ##--------------------------------------------------

        ##--------------------------------------------------
        ## todo: update tags
        ##--------------------------------------------------

        ##--------------------------------------------------
        ## todo: update resources
        ##--------------------------------------------------

        ##--------------------------------------------------
        ## Actually perform the update
        ##--------------------------------------------------

        return self.put_dataset(dataset_id, updates_dict)

    def delete_dataset(self, dataset_id, ignore_404=True):
        ign404 = SuppressExceptionIf(
            lambda e: ignore_404 and (e.status_code == 404))
        path = '/api/2/rest/dataset/{0}'.format(dataset_id)
        with ign404:
            self.request('DELETE', path, data={'id': dataset_id})

    ##============================================================
    ## Groups
    ##============================================================

    ##-----[!!]----------- IMPORTANT NOTE ---------------[!!]-----
    ## BEWARE! API v2 only considers actual groups, organizations
    ## are not handled / returned by this one!
    ##------------------------------------------------------------

    def list_groups(self):
        path = '/api/2/rest/group'
        response = self.request('GET', path)
        return response.json()

    def iter_groups(self):
        all_groups = self.list_groups()
        for group_id in all_groups:
            yield self.get_group(group_id)

    def get_group(self, group_id):
        path = '/api/2/rest/group/{0}'.format(group_id)
        response = self.request('GET', path)
        return response.json()

    def post_group(self, group):
        path = '/api/2/rest/group'
        response = self.request('POST', path, data=group)
        return response.json()

    def put_group(self, group_id, group):
        path = '/api/2/rest/group/{0}'.format(group_id)
        response = self.request('PUT', path, data=group)
        data = response.json()
        if not isinstance(data, dict):
            raise ApiBadBehavior("Bad value returned from the API")
        return data

    def delete_group(self, group_id, ignore_404=True):
        ign404 = SuppressExceptionIf(
            lambda e: ignore_404 and (e.status_code == 404))
        path = '/api/2/rest/group/{0}'.format(group_id)
        with ign404:
            self.request('DELETE', path)
        path = '/api/3/action/group_purge'
        with ign404:
            self.request('POST', path, data={'id': group_id})

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

        # Groups should be returned by name too (hopefully..)
        try:
            _retr_group = self.get_group(group['name'])
        except HTTPError:
            _retr_group = None

        if _retr_group is None:
            ## Just insert the group and return its id
            _ins_group = self.post_group(group)
            return _ins_group['id']

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

    def list_organizations(self):
        path = '/api/3/action/organization_list'
        response = self.request('GET', path)
        return response.json()['result']

    def iter_organizations(self):
        for org_id in self.list_organizations():
            yield self.get_organization(org_id)

    def get_organization(self, organization_id):
        path = '/api/3/action/organization_show?id={0}'.format(organization_id)
        response = self.request('GET', path)
        return response.json()['result']

    def post_organization(self, organization):
        path = '/api/3/action/organization_create'
        response = self.request('POST', path, data=organization)
        return response.json()['result']

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

    def list_licenses(self):
        path = '/api/2/rest/licenses'
        response = self.request('GET', path)
        return response.json()

    ##============================================================
    ## Tags
    ##============================================================

    def list_tags(self):
        path = '/api/2/rest/tag'
        response = self.request('GET', path)
        return response.json()

    def list_datasets_with_tag(self, tag_id):
        path = '/api/2/rest/tag/{0}'.format(tag_id)
        response = self.request('GET', path)
        return response.json()

    def iter_datasets_with_tag(self, tag_id):
        for dataset_id in self.list_datasets_with_tag():
            yield self.get_dataset(dataset_id)


class CkanDataImportClient(object):
    """Client to handle importing data in ckan"""

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

    def sync_data(self, data):
        """
        Import data into Ckan

        :param data:
            Dict (or dict-like) mapping object types to
            dicts (key/object) (key is the original key)
        """
        # We need to:

        # - Make sure all the referenced organizations are there
        # -> create a map from our ids to ckan ids

        # - Make sure all the referenced groups are there
        # -> create a map from our ids to ckan ids

        # - Import datasets (check differences -> update)
        # -> create a map from our ids to ckan ids
        pass

    def _is_our_dataset(self, dataset):
        try:
            dataset_source = dataset['extras'][self.source_field_name]
        except KeyError:
            return False
        return dataset_source == self.source_name

    def _find_our_datasets(self):
        for dataset in self.client.iter_datasets():
            if self._is_our_dataset(dataset):
                yield dataset
