from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel


import os
from dotenv import load_dotenv
load_dotenv()

ASTRA_DB_APPLICATION_TOKEN = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_SECURE_BUNDLE_PATH = os.getenv("ASTRA_DB_SECURE_BUNDLE_PATH")
KEYSPACE = "martech_offset"

class AstraSession:
    def __init__(self, keyspace=KEYSPACE, secure_bundle_path=ASTRA_DB_SECURE_BUNDLE_PATH, application_token=ASTRA_DB_APPLICATION_TOKEN):
        cloud_config = {'secure_connect_bundle': secure_bundle_path}
        auth_provider = PlainTextAuthProvider('token', application_token)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = cluster.connect()
        self.session.set_keyspace(keyspace)


    def shutdown(self):
        self.session.shutdown()


"""
    def prepare_statements(self):
        #
        # Communications Table
        # 
        # communication_id: UUID.
        # comm_date: Timestamp.
        # comm_date_bucket: Timestamp (rounded to the nearest hour).
        self.insert_communication_stmt = self.session.prepare("INSERT INTO communications (communication_id, comm_date, customer_id, channel, category_group, category, activity_name) VALUES (?, ?, ?, ?, ?, ?, ?)")

        # PROFILECAP
        self.update_profile_cap_stmt = self.session.prepare("INSERT INTO profile_cap (customer_id, channel, comm_date_bucket, communication_id) VALUES (?, ?, ?, ?)")
        self.get_profile_cap_stmt = self.session.prepare("SELECT * from profile_cap WHERE customer_id=? and channel=? AND comm_date_bucket=?")

        # ACTYCAP
        self.update_acty_cap_stmt = self.session.prepare("INSERT INTO acty_cap (activity_name, comm_date_bucket, communication_id) VALUES (?, ?, ?)")
        self.get_acty_cap_stmt = self.session.prepare("SELECT * from acty_cap WHERE activity_name=? AND comm_date_bucket=?")

        # PRTYCAP
        self.update_prty_cap_stmt = self.session.prepare("INSERT INTO prty_cap (category_group, category, comm_date_bucket, communication_id) VALUES (?, ?, ?, ?)")
        self.get_prty_cap_stmt = self.session.prepare("SELECT * from prty_cap WHERE category_group = ? AND category = ? AND comm_date_bucket=?")

        # CHANNELCAP
        self.update_channel_cap_stmt = self.session.prepare("INSERT INTO channel_cap (channel, comm_date_bucket, communication_id) VALUES (?, ?, ?)")
        self.get_channel_cap_stmt = self.session.prepare("SELECT * from channel_cap WHERE channel=? AND comm_date_bucket=?")
"""
