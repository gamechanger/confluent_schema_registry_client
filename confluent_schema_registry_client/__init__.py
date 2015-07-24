import requests
import json


HEADERS = {'content-type': 'application/vnd.schemaregistry.v1+json'}

class SchemaRegistryException(Exception):
    """
    Base exception class for all SchemaRegistryClient-raised exceptions.
    """
    def __init__(self, code, message):
        self.code = code
        self.message = message

def raise_if_failed(res):
    if res.status_code >= 400:
        data = res.json()
        raise SchemaRegistryException(data['error_code'], data['message'])


class SchemaRegistryClient(object):
    def __init__(self, host, port=8081):
        self.host = host
        self.port = port

    def _url(self, path, *args):
        return 'http://{}:{}{}'.format(
            self.host,
            self.port,
            path.format(*args))

    def get_schema(self, schema_id):
        res = requests.get(self._url('/schemas/ids/{}', schema_id))
        raise_if_failed(res)
        return json.loads(res.json()['schema'])

    def get_subjects(self):
        res = requests.get(self._url('/subjects'))
        raise_if_failed(res)
        return res.json()

    def get_subject_version_ids(self, subject):
        res = requests.get(self._url('/subjects/{}/versions', subject))
        raise_if_failed(res)
        return res.json()

    def get_subject_version(self, subject, version_id):
        res = requests.get(self._url('/subjects/{}/versions/{}', subject, version_id))
        raise_if_failed(res)
        return json.loads(res.json()['schema'])

    def get_subject_latest_version(self, subject):
        return self.get_subject_version(subject, 'latest')

    def register_subject_version(self, subject, schema):
        data = json.dumps({'schema': schema})
        res = requests.post(self._url('/subjects/{}/versions', subject), data=data, headers=HEADERS)
        raise_if_failed(res)
