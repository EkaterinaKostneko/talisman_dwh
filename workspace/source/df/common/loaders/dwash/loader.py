"""
Created on Dec 17, 2020

@author: pymancer@gmail.com
"""
from df.common.exceptions import DFLoaderException  # @UnresolvedImport
from df.common.helpers.logger import Logger
from df.common.helpers.http import download  # @UnresolvedImport
from df.common.helpers.gql import request as gql_request  # @UnresolvedImport
from ..loader import AbstractLoader

log = Logger()

CLOUD_TOKEN_HEADER = 'Cloud-Token'


class DwashLoader(AbstractLoader):
    def __init__(self, host, token=None, **kwargs):
        super().__init__(host, token=token, **kwargs)
        self.gql_host = f'{self.host}/api/graphql'
        self.verify = kwargs.get('verify', kwargs.get('ssl_verify', True))
        self.container = kwargs.get('container', '')
        self.descent = int(kwargs.get('descent', 1))

    def _get_parents(self, object_, descent=1, parents=None):
        if parents is None:
            parents = list()

        if descent != 1 and object_['parent']:
            found = self.list(slugs=(object_['parent'], ))

            for i in found:
                parents.append(i)

                if descent > 1:
                    descent -= 1
    
                parents = self._get_parents(i, descent=descent, parents=parents)

        return parents

    def list(self, **kwargs):
        """ Возвращает список объектов облака. """
        query = '''mutation ($input: ListObjectCloudInput!) {
  cloud {
    list(input: $input) {
      ok
      errors {
        ... on ErrorInterface {
          message
        }
      }
      payload {
        objects {
          edges {
            node {
              url
              slug
              name
              parent
            }
          }
        }
      }
    }
  }
}'''
        params = {
          "input": {
            "token": self.token
          }
        }

        container = kwargs['parent'] if kwargs.get('parent') else self.container
        if container:
            params['input']['parents'] = [container]

        if kwargs.get('slugs'):
            params['input']['slugs'] = list(kwargs['slugs'])

        response = gql_request(self.gql_host, query, params=params, verify=self.verify)

        if not response['cloud']['list']['ok']:
            msg = response['cloud']['list']['errors'][0]['message']
            raise DFLoaderException(f'Ошибка получения данных: {msg}')

        return [i['node'] for i in response['cloud']['list']['payload']['objects']['edges']]

    def find(self, pattern, **kwargs):
        found = list()
        objects = self.list()

        for object_ in objects:
            if pattern.match(object_['name']):
                found.append(object_)

        return found

    def load(self, file, storage, **kwargs):
        parents = list()
        url = f'{self.host}/{file["url"]}'
        headers = {CLOUD_TOKEN_HEADER: self.token}

        parents = self._get_parents(file, descent=self.descent, parents=parents)

        for parent in parents[::-1]:
            storage = storage.joinpath(parent['name'])

        return download(url, file['name'], storage, headers=headers, verify=self.verify)

    def delete(self, objects, **kwargs):
        slugs = [i['slug'] for i in objects]
        query = '''mutation ($input: DeleteObjectCloudInput!) {
  cloud {
    delete(input: $input) {
      ok
      errors {
        ... on ErrorInterface {
          message
        }
      }
      payload {
        affected
      }
    }
  }
}'''
        params = {
          "input": {
            "token": self.token,
            "slugs": slugs
          }
        }
        response = gql_request(self.gql_host, query, params=params, verify=self.verify)

        if not response['cloud']['delete']['ok']:
            msg = response['cloud']['delete']['errors'][0]['message']
            raise DFLoaderException(f'Ошибка удаления: {msg}')

        return response['cloud']['delete']['payload']['affected']
