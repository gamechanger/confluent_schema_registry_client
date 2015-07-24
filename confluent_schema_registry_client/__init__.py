import requests
import json

HEADERS = {'content-type': 'application/vnd.schemaregistry.v1+json'}

class CompatibilityLevel(object):
    none = "NONE"
    full = "FULL"
    backward = "BACKWARD"
    forward = "FORWARD"


def raise_if_failed(res):
    if res.status_code >= 400:
        try:
            data = res.json()
            e = SchemaRegistryException(
                data.get('error_code'), data.get('message'))
        except:
            e = SchemaRegistryException()
        raise e


class SchemaRegistryException(Exception):
    """
    Base exception class for all SchemaRegistryClient-raised exceptions.
    """
    def __init__(self, code=None, message=None):
        self.code = code
        self.message = message

    def __str__(self):
        return "{}({})".format(self.__class__.__name__, self.__dict__)


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
        data = json.dumps({'schema': json.dumps(schema)})
        res = requests.post(self._url('/subjects/{}/versions', subject), data=data, headers=HEADERS)
        raise_if_failed(res)
        return res.json()

    def schema_registration_for_subject(self, subject, schema):
        data = json.dumps({'schema': json.dumps(schema)})
        res = requests.post(self._url('/subjects/{}', subject), data=data, headers=HEADERS)
        raise_if_failed(res)
        res_data = res.json()
        return res_data['id'], res_data['version']

    def schema_is_registered_for_subject(self, subject, schema):
        data = json.dumps({'schema': json.dumps(schema)})
        res = requests.post(self._url('/subjects/{}', subject), data=data, headers=HEADERS)
        if res.status_code == 404:
            return False
        raise_if_failed(res)
        return True

    def schema_is_compatible_with_subject_version(self, subject, version_id, schema):
        data = json.dumps({'schema': json.dumps(schema)})
        res = requests.post(
            self._url('/compatibility/subjects/{}/versions/{}', subject, version_id),
            data=data, headers=HEADERS)
        raise_if_failed(res)
        return res.json()['is_compatible']

    def set_global_compatibility_level(self, level):
        res = requests.put(
            self._url('/config'),
            data=json.dumps({'compatibility': level}),
            headers=HEADERS)
        raise_if_failed(res)

    def get_global_compatibility_level(self):
        res = requests.get(self._url('/config'), headers=HEADERS)
        raise_if_failed(res)
        return res.json()['compatibility']
