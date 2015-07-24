# confluent_schema_registry_client
A simple Python client for Confluent's Schema Registry


## Example
```python
from confluent_schema_registry_client import SchemaRegistryClient
from confluent_schema_registry_client import CompatibilityLevel

c = SchemaRegistryClient('schema-registry.mydomain.com')

# Get the list of known subjects
c.get_subjects()
# => ['subject1', subject2]


# Register a new schema under a subject
schema = {
    "type": "record",
    "name": "test",
    "fields":
        [
            {
                "type": "string",
                "name": "field1"
            },
            {
                "type": "integer",
                "name": "field2"
            }
        ]
    }

schema_id = c.register_subject_version('subject1', schema)
# => 13


# Retrieve schema by id
c.get_schema(schema_id)
# => {"type": "record", "name": "test", "fields": [{"type": "string", name": "field1"}, {"type": "integer", "name": "field2"}]}


# Get the list of schema versions registered under a subject
c.get_subject_version_ids('subject1')
# => [1, 2, 3, 4]


# Retrieve a specific schema by subject and version id
c.get_subject_version('subject1', 4)
# => {"type": "record", "name": "test", "fields": [{"type": "string", name": "field1"}, {"type": "integer", "name": "field2"}]}


# Retrieve the latest registered version of a schema
c.get_subject_latest_version('subject1')
# => {"type": "record", "name": "test", "fields": [{"type": "string", name": "field1"}, {"type": "integer", "name": "field2"}]}


# Check if a schema is registered under a given subject
c.schema_is_registered_for_subject('subject1', schema)
# => True


# Check compatibility of schema with registered schema version
# Retrieve a specific schema by subject and version id
c.schema_is_compatible_with_subject_version('subject1', 4, schema)
# => True


# Get and set compatilibility levels
c.set_global_compatibility_level(CompatibilityLevel.full)
c.get_global_compatibility_level()
# => "FULL"
c.set_subject_compatibility_level('subject1', CompatibilityLevel.backward)
c.get_subject_compatibility_level('subject1')
# => "BACKWARD"
```
