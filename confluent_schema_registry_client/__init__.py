import requests
import json


HEADERS = {'content-type': 'application/vnd.schemaregistry.v1+json'}

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
        res.raise_for_status()
        return json.loads(res.json()['schema'])

    def get_subjects(self):
        res = requests.get(self._url('/subjects'))
        res.raise_for_status()
        return res.json()

    def get_subject_version_ids(self, subject):
        res = requests.get(self._url('/subjects/{}/versions', subject))
        res.raise_for_status()
        return res.json()

    def get_subject_version(self, subject, version_id):
        res = requests.get(self._url('/subjects/{}/versions/{}', subject, version_id))
        res.raise_for_status()
        return res.json()['schema']

    def get_subject_latest_version(self, subject):
        return self.get_subject_version(subject, 'latest')

    def register_subject_version(self, subject, schema):
        data = json.dumps({'schema': schema})
        res = requests.post(self._url('/subjects/{}/versions', subject), data=data, headers=HEADERS)
        res.raise_for_status()
