"""
Created on Dec 19, 2020

@author: pymancer@gmail.com
"""
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport


def request(url, query, params=None, headers=None, verify=True):
    actual_headers = {'Content-type': 'application/json'}
    if headers:
        actual_headers.update(headers)

    transport=RequestsHTTPTransport(
        url=url,
        use_json=True,
        headers=actual_headers,
        verify=verify,
        retries=0,
    )
    
    client = Client(
        transport=transport,
        fetch_schema_from_transport=True,
    )

    query = gql(query)

    return client.execute(query, variable_values=params)
