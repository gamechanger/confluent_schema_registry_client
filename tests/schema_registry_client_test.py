from unittest import TestCase
from requests_mock import Mocker
from confluent_schema_registry_client import SchemaRegistryClient, SchemaRegistryException


class TestSchemaRegistryClient(TestCase):

    def setUp(self):
        self.client = SchemaRegistryClient('test_domain.gc.com')


    def test_get_schema(self):
        with Mocker() as m:
            m.get('http://test_domain.gc.com:8081/schemas/ids/abc123', json={
                'schema': "{\"type\": \"string\"}"
            })

            schema = self.client.get_schema('abc123')
            self.assertEquals({"type": "string"}, schema)

    def test_get_non_existent_schema(self):
        with Mocker() as m:
            with self.assertRaises(SchemaRegistryException) as cm:
                m.get(
                    'http://test_domain.gc.com:8081/schemas/ids/abc123',
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
                'http://test_domain.gc.com:8081/subjects',
                json=["beans", "frank"])
            self.assertEquals(
                ["beans", "frank"],
                self.client.get_subjects())

    def test_get_subjects_internal_server_error(self):
        with Mocker() as m:
            with self.assertRaises(SchemaRegistryException):
                m.get(
                    'http://test_domain.gc.com:8081/subjects',
                    json={
                        "error_code": 50001,
                        "message": "Error in the backend datastore"
                    },
                    status_code=500)
                self.client.get_subjects()