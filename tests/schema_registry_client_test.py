from unittest import TestCase
from requests_mock import Mocker
from confluent_schema_registry_client import SchemaRegistryClient, SchemaRegistryException


TEST_DOMAIN = 'test_domain.gc.com'
TEST_PORT = 8081
SCHEMA = {"type": "string"}
ENCODED_SCHEMA = "{\"type\": \"string\"}"


def url(path):
    return 'http://{}:{}{}'.format(
        TEST_DOMAIN, TEST_PORT, path)


class TestSchemaRegistryClient(TestCase):

    def setUp(self):
        self.client = SchemaRegistryClient('test_domain.gc.com')


    def test_get_schema(self):
        with Mocker() as m:
            m.get(url('/schemas/ids/abc123'), json={
                'schema': ENCODED_SCHEMA
            })

            schema = self.client.get_schema('abc123')
            self.assertEquals({"type": "string"}, schema)

    def test_get_non_existent_schema(self):
        with Mocker() as m:
            with self.assertRaises(SchemaRegistryException) as cm:
                m.get(
                    url('/schemas/ids/abc123'),
                    json={
                        "error_code": 40403,
                        "message": "Schema not found"
                    },
                    status_code=404)

                self.client.get_schema('abc123')

            self.assertEquals(cm.exception.code, 40403)
            self.assertEquals(cm.exception.message, 'Schema not found')

    def test_get_subjects(self):
        with Mocker() as m:
            m.get(
                url('/subjects'),
                json=["beans", "frank"])
            self.assertEquals(
                ["beans", "frank"],
                self.client.get_subjects())

    def test_get_subjects_internal_server_error(self):
        with Mocker() as m:
            with self.assertRaises(SchemaRegistryException):
                m.get(
                    url('/subjects'),
                    json={
                        "error_code": 50001,
                        "message": "Error in the backend datastore"
                    },
                    status_code=500)
                self.client.get_subjects()

    def test_get_subject_version_ids(self):
        with Mocker() as m:
            m.get(
                url('/subjects/test/versions'),
                json=[1, 2, 3])
            self.assertEquals(
                [1, 2, 3],
                self.client.get_subject_version_ids('test'))

    def test_get_subject_version_ids_non_existent_subject(self):
        with Mocker() as m:
            with self.assertRaises(SchemaRegistryException):
                m.get(
                    url('/subjects/test/versions'),
                    json={
                        "error_code": 40401,
                        "message": "Subject not found"
                    },
                    status_code=404)
                self.client.get_subject_version_ids('test')

    def test_get_subject_version(self):
        with Mocker() as m:
            m.get(
                url('/subjects/test/versions/34'),
                json={
                    'name': 'test',
                    'version': 34,
                    'schema': ENCODED_SCHEMA
                })
            self.assertEquals(
                SCHEMA,
                self.client.get_subject_version('test', 34))

    def test_get_subject_version_non_existent_version(self):
        with Mocker() as m:
            with self.assertRaises(SchemaRegistryException):
                m.get(
                    url('/subjects/test/versions/34'),
                    json={
                        "error_code": 42202,
                        "message": "Invalid version"
                    },
                    status_code=422)
                self.client.get_subject_version('test', 34)
