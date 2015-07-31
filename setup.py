import setuptools

setuptools.setup(
    name="confluent_schema_registry_client",
    version="1.0.1",
    author="Tom Leach",
    author_email="tom@gc.com",
    description="A simple client Confluent's Avro Schema Registry",
    license="MIT",
    keywords="confluent schema registry client avro http rest",
    url="http://github.com/gamechanger/confluent_schema_registry_client",
    packages=["confluent_schema_registry_client"],
    install_requires=['requests>=2.3.0'],
    tests_require=['nose', 'requests_mock']
    )
