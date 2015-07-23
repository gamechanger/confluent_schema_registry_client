from unittest import TestCase
import requests_mock
import json
from confluent_schema_registry_client import SchemaRegistryClient


class TestSchemaRegistryClient(TestCase):

    def setUp(self):
        self.client = SchemaRegistryClient('test_domain.gc.com')


    def test_get_schema(self):
        with requests_mock.Mocker() as m:
            m.get('http://test_domain.gc.com:8081/schemas/ids/abc123', text=json.dumps({
                'schema': "{\"type\": \"string\"}"
                }))

            schema = self.client.get_schema('abc123')
            self.assertEquals({"type": "string"}, schema)
