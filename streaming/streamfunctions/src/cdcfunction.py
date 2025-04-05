# For Astra Streaming
import base64
import json
import io
import avro.schema
from avro.io import BinaryDecoder, DatumReader
from pulsar import Function

# For Astra DB
import re
import os.path as ospath
import requests
import shutil
import cassandra.auth as cassauth
import cassandra.cluster as casscluster
import cassandra.query as cassquery
import uuid

ASTRA_DOMAIN = 'astra.datastax.com'


## helpers

keySchemaDict = {
    "type": "record",
    "name": "communications",
    "fields": [
      {
        "name": "communication_id",
        "type": {
          "type": "string",
          "logicalType": "uuid"
        }
      },
      {
        "name": "comm_datetime",
        "type": [
          "null",
          {
            "type": "long",
            "logicalType": "timestamp-millis"
          }
        ],
        "default": None
      }
    ]
  }

valueSchemaDict = {
    "type": "record",
    "name": "communications",
    "fields": [
      {
        "name": "customer_id",
        "type": [
          "null",
          {
            "type": "string",
            "logicalType": "uuid"
          }
        ],
        "default": None
      },
      {
        "name": "activity_name",
        "type": [
          "null",
          "string"
        ],
        "default": None
      },
      {
        "name": "category_group",
        "type": [
          "null",
          "string"
        ],
        "default": None
      },
      {
        "name": "category",
        "type": [
          "null",
          "string"
        ],
        "default": None
      },
      {
        "name": "channel",
        "type": [
          "null",
          "string"
        ],
        "default": None
      }
    ]
  }


def createAvroReader(schemaDict):
    return DatumReader(avro.schema.make_avsc_object(schemaDict))

def bytesToReadDict(by, avroReader):
    binDecoded = BinaryDecoder(io.BytesIO(by))
    return avroReader.read(binDecoded)

def b64ToReadDict(b64string, avroReader):
    b64Decoded = base64.b64decode(b64string)
    return bytesToReadDict(b64Decoded, avroReader)

def cdcMessageToDictPF(pk, body, keyReader, valueReader):
    # Body can be a 'str' or 'bytes' already depending on the length
    # of the input fields in the table insert.
    #       ¯\_(ツ)_/¯
    # Take care of this:
    encodedBody = body if isinstance(body, bytes) else body.encode()
    #
    return {
        **bytesToReadDict(base64.b64decode(pk), keyReader),
        **bytesToReadDict(encodedBody, valueReader),
    }


class ProcessComms(Function):

    def __init__(self):
        # Astra Streaming
        self.keyReader = createAvroReader(keySchemaDict)
        self.valueReader = createAvroReader(valueSchemaDict)
        
        # Astra DB
        self.logger = None

        self.astra_streaming_output_topic_url = ''

        self.astra_db_id = ''
        self.astra_db_region = ''
        self.astra_db_session_type = ''
        self.astra_db_session = None
        self.astra_db_keyspace = ''
        self.astra_db_table = ''
        self.astra_db_venues_query_columns = []
        self.astra_db_venues_query_type = ''
        self.astra_db_token_file = ''
        self.astra_db_token = ''
        self.astra_db_scb_file = ''

        self.astra_api_get_scb_download_link_url = ''
        self.astra_api_headers = {}

        self.astra_db_query_base_url = ''
        self.astra_db_query_headers = {}
        self.astra_db_query_params = {}
        self.astra_db_query_page_size = 0

        self.min_venue_result = 0
        self.base_dir = ''


    def setup(self, context):
        self.logger = context.get_logger()

        self.astra_streaming_output_topic_url = 'persistent://{}'.format(context.get_output_topic())
        self.base_dir = '{}/streamfunctions'.format(context.user_code_dir)

        self.astra_db_token = context.get_user_config_value('astra_db_token')
        self.astra_db_id = context.get_user_config_value('astra_db_id')

        self.astra_db_keyspace = 'martech'

        self.astra_db_session = AstraDatabaseSession(
            db_token=self.astra_db_token,
            db_domain=ASTRA_DOMAIN,
            db_id=self.astra_db_id,
            db_keyspace=self.astra_db_keyspace,
            base_dir=self.base_dir,
            logger=self.logger
        )


    def process(self, msgBody, context):
        self.setup(context)

        msgPK = context.get_partition_key()
        msgDict = cdcMessageToDictPF(msgPK, msgBody, self.keyReader, self.valueReader)

        #
        # parse the message
        comm_id = uuid.UUID(msgDict['communication_id'])
        comm_datetime = msgDict['comm_datetime']
        comm_activity = msgDict['activity_name']
        comm_category = msgDict['category']
        comm_category_group = msgDict['category_group']
        comm_channel = msgDict['channel']
        comm_customer_id = uuid.UUID(msgDict['customer_id'])

        self.logger.info(f"Parsed message: {comm_id}, {comm_datetime}, {comm_activity}, {comm_category}, {comm_category_group}, {comm_channel}, {comm_customer_id}")

        # format date and time
        comm_date = int(comm_datetime.strftime('%Y%m%d')) # format date as YYYYMMDD
        comm_time = int(comm_datetime.strftime('%H%M%S%f')[:9]) # format time as HHMMSSsss
        comm_month = int(comm_datetime.strftime('%Y%m')) # format month as YYYYMM
        comm_day = int(comm_datetime.strftime('%d')) # format day as DD

        #
        # CHANNELCAP
        # Prepare Statement
        channel_cap_stmt = self.astra_db_session.db_session.prepare("INSERT INTO channelcap_day (channel, comm_date, comm_time) VALUES (?, ?, ?)")
        # Execute Statement
        self.astra_db_session.db_session.execute(channel_cap_stmt, (comm_channel, comm_date, comm_time))

        #
        # ACTIVITYCAP
        # Prepare Statement
        activity_cap_stmt = self.astra_db_session.db_session.prepare("INSERT INTO actycap_day (activity, comm_date, comm_time) VALUES (?, ?, ?)")
        # Execute Statement
        self.astra_db_session.db_session.execute(activity_cap_stmt, (comm_activity, comm_date, comm_time))

        #
        # PRTYCAP
        # Prepare Statement
        party_cap_stmt = self.astra_db_session.db_session.prepare("INSERT INTO prtycap_day (category_group, category, comm_date, comm_time) VALUES (?, ?, ?, ?)")
        # Execute Statement
        self.astra_db_session.db_session.execute(party_cap_stmt, (comm_category_group, comm_category, comm_date, comm_time))

        #
        # PROFILECAP
        # Prepare Statement
        profile_cap_stmt = self.astra_db_session.db_session.prepare("INSERT INTO profilecap_month (customer_id, channel, comm_month, comm_day, communication_id) VALUES (?, ?, ?, ?, ?)")
        # Execute Statement
        self.astra_db_session.db_session.execute(profile_cap_stmt, (comm_customer_id, comm_channel, comm_month, comm_day, comm_id))
        

        #
        # insert the message into the Astra DB
        """
        insert_statement = CQL_STATEMENT_TEMPLATES['insert'].format(
            self.astra_db_keyspace,
            self.astra_db_table,
            'id, amount, account_id, type, date, description',
            '%s, %s, %s, %s, %s, %s'
        )

        self.astra_db_session.db_session.execute(
            insert_statement, 
            (msgId, msgAmount, msgAccountId, msgType, msgDate, msgDescription)
            #(msgId, msgAmount, msgAccountId, msgType, msgDate, msgDescription, customer_name, account_name)
            )
        """

        return comm_id
    




###
### DATABASE SECTION
###

CQL_STATEMENT_TEMPLATES = {
    'select': 'SELECT {} FROM {}.{}',
    'insert': 'INSERT INTO {}.{} ({}) VALUES ({})'
}

class AstraDatabaseSession:
    cassname_regex_pattern = re.compile(r'\(.*\)')

    def __init__(self, **kwargs):
        self.db_token = ''
        self.db_domain = ''
        self.db_id = ''
        self.db_keyspace = ''
        self.base_dir = ''
        self.logger = None

        self.__init_attributes(**kwargs)

        self.db_session = None
        self.db_cluster = None

        self.statement_library = {}

        self.db_scb_local_path = '{}/auth/{}_scb.zip'.format(self.base_dir, self.db_id)
        self.db_capabilities_local_path = '{}/{}_capabilities.txt'.format(self.base_dir, self.db_id)

        if not ospath.exists(self.db_scb_local_path):
            self.__download_secure_connect_bundle()

        cloud_config = {
            'secure_connect_bundle': self.db_scb_local_path
        }
        auth_provider = cassauth.PlainTextAuthProvider('token', self.db_token)
        self.db_cluster = casscluster.Cluster(
            cloud=cloud_config,
            auth_provider=auth_provider,
            protocol_version=4
        )
        self.db_session = self.db_cluster.connect()
        self.db_session.set_keyspace('martech')
        self.db_session.row_factory = cassquery.dict_factory


    def __init_attributes(self, **kwargs):
        for setting in [
            'db_token',
            'db_domain',
            'db_id',
            'base_dir',
            'logger'
        ]:
            if setting not in kwargs:
                raise ValueError('Missing config setting: {}'.format(setting))
            setattr(self, setting, kwargs[setting])


    def __download_secure_connect_bundle(self):
        astra_api_get_scb_download_link_url = 'https://api.{}/v2/databases/{}/secureBundleURL'.format(
            self.db_domain,
            self.db_id
        )

        astra_api_headers = {
            'Authorization': 'Bearer {}'.format(self.db_token),
            'Content-Type': 'application/json'
        }

        request_response = requests.post(astra_api_get_scb_download_link_url, headers=astra_api_headers)
        response_data = request_response.json()

        self.logger.info('Downloading secure connect bundle; this should only happen once')
        if not request_response.ok:
            raise Exception('Failed to Astra API request. Request returned {} {} - {}'.format(
                request_response.status_code, request_response.reason, response_data['errors'][0]['message']))

        if 'downloadURL' not in response_data:
            raise Exception('Failed to get secure connect bundle download URL. Request returned {} {}'.format(
                request_response.status_code, request_response.reason))

        request_response = requests.get(response_data['downloadURL'], stream=True)

        if not request_response.ok:
            raise Exception('Failed download secure connect bundle. Request returned {} {}'.format(
                request_response.status_code, request_response.reason))

        with open(self.db_scb_local_path, 'wb') as scb_file_handle:
            shutil.copyfileobj(request_response.raw, scb_file_handle)

        if not ospath.exists(self.db_scb_local_path):
            raise Exception('Failed to download secure connect bundle')

        self.logger.info('Secure connect bundle downloaded')