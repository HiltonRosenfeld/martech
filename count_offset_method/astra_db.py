from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel


import os
from dotenv import load_dotenv
load_dotenv()

ASTRA_DB_APPLICATION_TOKEN = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_SECURE_BUNDLE_PATH = os.getenv("ASTRA_DB_SECURE_BUNDLE_PATH")
KEYSPACE = "martech"

class AstraSession:
    def __init__(self, keyspace=KEYSPACE, secure_bundle_path=ASTRA_DB_SECURE_BUNDLE_PATH, application_token=ASTRA_DB_APPLICATION_TOKEN):
        cloud_config = {'secure_connect_bundle': secure_bundle_path}
        auth_provider = PlainTextAuthProvider('token', application_token)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = cluster.connect()
        self.session.set_keyspace(keyspace)


    def shutdown(self):
        self.session.shutdown()
