"""
Ckan API client
"""

import json
import urlparse

import requests


## These are the only fields that should be needed..
CKAN_CORE_DATASET_FIELDS = [
    'id',
    'author',
    'author_email',
    'creator_user_id',
    'license_id',
    'maintainer',
    'maintainer_email',
    'name',
    'notes',
    'owner_org',
    'private',
    'title',
    'type',
    'url',
    'version',
    # organization / owner_org?
    # relationshipts, resources
]

CKAN_CORE_DATASET_RELATED_FIELDS = [
    'extras', 'relationships', 'resources'
]

## Beware that even resources have ids!
CKAN_CORE_RESOURCE_FIELDS = [
    'id',
    'position',
    'cache_last_updated',
    'cache_url',
    'created',
    'description',
    'extras',
    'format',
    'hash',
    'last_modified',
    'mimetype',
    'mimetype_inner',
    'name',
    'resource_group_id',
    'resource_type',
    'revision_id',
    'size',
    'state',
    'url',
    'url_type',
    'webstore_last_updated',
    'webstore_url',
]


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
                pass
            pass

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

    def partial_update_dataset(self, dataset_id, updates):
        """Trickery to update a dataset"""

        dataset = self.get_dataset(dataset_id)
        pass

    def delete_dataset(self, dataset_id, dataset):
        path = '/api/2/rest/dataset/{0}'.format(dataset_id)
        response = self.request('DELETE', path)
        return response.json()

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
