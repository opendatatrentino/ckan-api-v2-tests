"""
Ckan API client
"""

import json
import urlparse

import requests


DATASET_FIELDS = {}
DATASET_FIELDS['core'] = [
    'author', 'author_email', 'license_id', 'maintainer', 'maintainer_email',
    'name', 'notes', 'owner_org', 'private', 'state', 'type', 'url'
]
DATASET_FIELDS['cruft'] = [
    'ckan_url', 'creator_user_id', 'isopen', 'license', 'license_title',
    'license_url', 'metadata_created', 'metadata_modified', 'num_resources',
    'num_tags', 'organization', 'ratings_average', 'ratings_count',
    'revision_id', 'version'
]
DATASET_FIELDS['keys'] = ['id']
DATASET_FIELDS['special'] = ['extras', 'groups', 'relationships', 'resources']


RESOURCE_FIELDS = {}
RESOURCE_FIELDS['core'] = [
    "description",
    "format",
    "mimetype",
    "mimetype_inner",
    "name",
    "position",
    "resource_type",
    "size",
    "url",
    "url_type",
]
RESOURCE_FIELDS['cruft'] = [
    "cache_last_updated",
    "cache_url",
    "created",
    "hash",
    "last_modified",
    "package_id",
    "resource_group_id",
    "webstore_last_updated",
    "webstore_url",
]
RESOURCE_FIELDS['keys'] = ['id']
RESOURCE_FIELDS['special'] = []


class HTTPError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message

    def __str__(self):
        return "HTTPError [{0}]: {1}".format(self.status_code, self.message)


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

    ## --- datasets ---

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

        ##==================================================
        ## Notes
        ## - "core" fields seems to be kept
        ## - ..but "extras" need to be passed back again
        ## - groups?
        ## - resources?
        ## - relationships?
        ##==================================================

        original_dataset = self.get_dataset(dataset_id)

        ## Dictionary holding the actual data to be sent
        ## for performing the update
        updates_dict = {'id': dataset_id}

        ## Core fields
        ##----------------------------------------

        for field in DATASET_FIELDS['core']:
            if field in updates:
                updates_dict[field] = updates[field]
            else:
                updates_dict[field] = original_dataset[field]

        ## Extras fields
        ##----------------------------------------

        ## WARNING! Behavior here is quite "funky":

        ## db: {'a': 'aa', 'b': 'bb', 'c': 'cc'}
        ## update: (no extras key)
        ## result: {}

        ## db: {'a': 'aa', 'b': 'bb', 'c': 'cc'}
        ## update: {'a': 'foo'}
        ## result: {'a': 'foo', 'b': 'bb', 'c': 'cc'}

        ## db: {'a': 'aa', 'b': 'bb', 'c': 'cc'}
        ## update: {}
        ## db: {'a': 'aa', 'b': 'bb', 'c': 'cc'}

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

        ## Actually perform the update
        ##----------------------------------------

        return self.put_dataset(dataset_id, updates_dict)

    def delete_dataset(self, dataset_id):
        path = '/api/2/rest/dataset/{0}'.format(dataset_id)
        self.request('DELETE', path, data={'id': dataset_id})
        # doesn't return anything in the response body..

    ## --- groups ---

    ## BEWARE! API v2 only considers actual groups, organizations
    ## are not handled / returned by this one!

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
        return response.json()

    def delete_group(self, group_id, group):
        path = '/api/2/rest/group/{0}'.format(group_id)
        response = self.request('DELETE', path)
        return response.json()

    ## --- organizations ---

    ## We need to fallback to api v3 here, as v2 doesn't support
    ## doing things with organizations..

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

    def delete_organization(self, organization_id):
        path = '/api/3/action/organization_delete'
        response = self.request('PUT', path, data={'id': organization_id})
        return response.json()['result']

    ## No created / update / delete as they are unreliable

    ## --- licenses ---

    def list_licenses(self):
        path = '/api/2/rest/licenses'
        response = self.request('GET', path)
        return response.json()

    ## --- tags ---

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
