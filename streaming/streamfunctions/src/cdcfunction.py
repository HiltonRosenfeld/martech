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

from datetime import datetime, timezone

ASTRA_DOMAIN = 'astra.datastax.com'
ASTRA_KEYSPACE = 'martech'


## helpers

keySchemaDict = {
    "type": "record",
    "name": "communication_data",
    "fields": [
      {
        "name": "comm_id",
        "type": "string"
      }
    ]
  }

valueSchemaDict = {
    "type": "record",
    "name": "communication_data",
    "fields": [
      {
        "name": "activity",
        "type": [
          "null",
          "string"
        ],
        "default": None
      },
      {
        "name": "customer_id",
        "type": [
          "null",
          "string"
        ],
        "default": None
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
      },
      {
        "name": "request_date",
        "type": [
          "null",
          {
            "type": "int",
            "logicalType": "date"
          }
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

# 
# CLASS to count daily totals
#
class DailyTotals(Function):
    
    def __init__(self):
        
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

        #self.astra_streaming_output_topic_url = 'persistent://{}'.format(context.get_output_topic())
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

    def update_acty_cap(self, current_date):
        # Activity count
        # Iterate through each activity
        # Todo: change this to read the set of activities from the events_by_day table
        for activity in ['credit_card', 'mortgage', 'business', 'retail', 'investment']:

            query_stmt = "SELECT comm_time, comm_offset_time, comm_offset_count FROM actycap_day WHERE activity = %s AND comm_date = %s"
            rows = self.astra_db_session.db_session.execute(query_stmt, (activity, current_date))
        
            if not rows:
                row_count = 0
                self.logger.info(f"Daily Totals - Activity: {activity} Row Count: no rows returned")
                continue
            else:
                row_count = sum(1 for _ in rows)  # Count rows
                self.logger.info(f"Daily Totals - Activity: {activity} Row Count: {row_count}")

            # Insert summary data for the day
            insert_stmt = "INSERT INTO actycap_week (activity, comm_date, comm_count) VALUES (%s, %s, %s)"
            self.astra_db_session.db_session.execute(insert_stmt, (activity, current_date, row_count))

    def update_channel_cap(self, current_date):
        # Channel count
        # Iterate through each channel
        # Todo: change this to read the set of channels from the events_by_day table
        query_stmt = "SELECT cap_action FROM events_by_day WHERE cap_type = %s AND cap_date = %s"
        rows = self.astra_db_session.db_session.execute(query_stmt, ('channel', current_date))
        if not rows:
            channel_list = ['email', 'sms', 'push', 'in-app', 'direct']
        else:
            channel_list = []
            for row in rows:
                c = rows[0]["cap_action"] if "cap_action" in rows[0] else 0
                channel_list.append(c)

        for channel in channel_list:
            query_stmt = "SELECT comm_time, comm_offset_time, comm_offset_count FROM channelcap_day WHERE channel = %s AND comm_date = %s"
            rows = self.astra_db_session.db_session.execute(query_stmt, (channel, current_date))
        
            if not rows:
                row_count = 0
                self.logger.info(f"Daily Totals - Channel: {channel} Row Count: no rows returned")
                continue
            else:
                row_count = sum(1 for _ in rows)  # Count rows
                self.logger.info(f"Daily Totals - Channel: {channel} Row Count: {row_count}")

            # Insert summary data for the day
            insert_stmt = "INSERT INTO channelcap_week (channel, comm_date, comm_count) VALUES (%s, %s, %s)"
            self.astra_db_session.db_session.execute(insert_stmt, (channel, current_date, row_count))

    def update_priority_cap(self, current_date):
        # Priority count
        # Iterate through each category
        # Todo: change this to read the set of categorys from the events_by_day table
        for category in ['ctgy_1', 'ctgy_2', 'ctgy_3', 'ctgy_4', 'ctgy_5']:

            query_stmt = "SELECT comm_time, comm_offset_time, comm_offset_count FROM prtycap_day WHERE category_group = 'group' AND category = %s AND comm_date = %s"
            rows = self.astra_db_session.db_session.execute(query_stmt, (category, current_date))
        
            if not rows:
                row_count = 0
                self.logger.info(f"Daily Totals - Category: {category} Row Count: no rows returned")
                continue
            else:
                row_count = sum(1 for _ in rows)  # Count rows
                self.logger.info(f"Daily Totals - Category: {category} Row Count: {row_count}")

            # Insert summary data for the day
            insert_stmt = "INSERT INTO prtycap_week (category_group, category, comm_date, comm_count) VALUES ('group', %s, %s, %s)"
            self.astra_db_session.db_session.execute(insert_stmt, (category, current_date, row_count))


    def process(self, msgBody, context):
        self.setup(context)

        # Read the message body to get the date to process
        if isinstance(msgBody, bytes):
            message = input.decode('utf-8')
        else:
            message = str(msgBody)

        # Set the current day and time bucket based on today's date
        date = datetime.now(timezone.utc)
        current_date = int(date.strftime('%Y%m%d'))
        
        # if the event message is in the format "YYYYMMDD", then set current_date to message content
        if len(message) == 8 and message.isdigit():
            current_date = int(message)
        
        self.logger.info(f"Daily Totals - trigger received for {current_date}")

        # ACTY
        self.update_acty_cap(current_date)
        # CHANNEL
        self.update_channel_cap(current_date)
        # PRTY
        self.update_priority_cap(current_date)





# 
# CLASS to process each communication message
#
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


    def should_update_offsets(self, run_count):
        last_run_file = '{}/last_run_file.txt'.format(self.base_dir)

        last_run = 0

        # read the last value from the file
        if ospath.exists(last_run_file):
            with open(last_run_file, 'r') as fh:
                last_run = fh.read()
                if last_run:
                    last_run = int(last_run)
        
        # increment that last_run and write the new value to the file
        last_run += 1
        with open(last_run_file, 'w') as fh:
            fh.write(str(last_run))

        # check if we should update offsets
        # check if last_run is divisible by 10
        # Todo: adapt the logic to be dynamic based on how many communications are recorded per minute
        if (last_run % run_count == 0):
            return True
        else:
            return False


    def get_offset_count(self, cap_type, activity, comm_date):
        # Get the offset count
        rows = None
        state_stmt = "SELECT comm_time, comm_offset_count, comm_offset_time FROM actycap_day WHERE activity = %s AND comm_date = %s"
        rows = self.astra_db_session.db_session.execute(state_stmt, (activity, comm_date))
            # Todo: use getone() function to select only the first row.
        comm_offset_time = 0
        comm_offset_count = 0
        if rows and rows[0]["comm_offset_count"]:
            comm_offset_time = rows[0]["comm_offset_time"] if "comm_offset_time" in rows[0] else 0
            comm_offset_count = rows[0]["comm_offset_count"] if "comm_offset_count" in rows[0] else 0
            self.logger.info(f"STATE_COUNT {cap_type}|{activity} - get offset: {comm_offset_time}, {comm_offset_count}")
        else:
            self.logger.info(f"STATE_COUNT {cap_type}|{activity} - no offset found")

        return comm_offset_time, comm_offset_count
    

    def get_row_count(self, cap_type, activity, comm_offset_time):
        # Set the current day and time buckets
        date = datetime.now(timezone.utc)
        current_date = int(date.strftime('%Y%m%d')) # bucket by day

        rows = None
        cap_stmt = "SELECT comm_time, comm_offset_time, comm_offset_count FROM actycap_day WHERE activity = %s AND comm_date = %s AND comm_time > %s"
        self.logger.info(f"SELECT comm_time, comm_offset_time, comm_offset_count FROM actycap_day WHERE activity = {activity} AND comm_date = {current_date} AND comm_time > {comm_offset_time}")
        rows = self.astra_db_session.db_session.execute(cap_stmt, [activity, current_date, comm_offset_time ])
        if not rows:
            row_count = 0
            comm_time = 0
            self.logger.info(f"ROW_COUNT {cap_type}|{activity} - get rows: No rows returned")
        else:
            #comm_offset_time = rows[0]["comm_offset_time"] if "comm_offset_time" in rows[0] else 0
            #comm_offset_count = rows[0]["comm_offset_count"] if "comm_offset_count" in rows[0] else 0
            comm_time = rows[0]["comm_time"] if "comm_time" in rows[0] else 0
            row_count = sum(1 for _ in rows)  # Count rows
            self.logger.info(f"ROW_COUNT {cap_type}|{activity} - get rows: {row_count}")
        
        return row_count, comm_time



    def process(self, msgBody, context):
        self.setup(context)

        msgPK = context.get_partition_key()
        msgDict = cdcMessageToDictPF(msgPK, msgBody, self.keyReader, self.valueReader)

        #
        # parse the message
        comm_id = msgDict['comm_id']
        comm_datetime = msgDict['comm_datetime']
        comm_request_date = msgDict['request_date']
        comm_activity = msgDict['activity']
        comm_category = msgDict['category']
        comm_channel = msgDict['channel']
        comm_customer_id = msgDict['customer_id']

        self.logger.info(f"Parsed message: {comm_id}, {comm_datetime}, {comm_request_date}, {comm_activity}, {comm_category}, {comm_channel}, {comm_customer_id}")

        # format date and time
        comm_date = int(comm_datetime.strftime('%Y%m%d')) # format date as YYYYMMDD
        comm_time = int(comm_datetime.strftime('%H%M%S%f')[:9]) # format time as HHMMSSsss
        comm_month = int(comm_datetime.strftime('%Y%m')) # format month as YYYYMM
        comm_day = int(comm_datetime.strftime('%d')) # format day as DD

        #
        # Define EVENTS BY DAY statement
        #
        events_by_day_stmt = "INSERT INTO events_by_day (cap_date, cap_type, cap_action) VALUES (%s, %s, %s)"

        #
        # ACTIVITYCAP
        activity_cap_stmt = "INSERT INTO actycap_day (activity, comm_date, comm_time) VALUES (%s, %s, %s)"
        self.astra_db_session.db_session.execute(activity_cap_stmt, (comm_activity, comm_date, comm_time))
        # update the events_by_day table
        self.astra_db_session.db_session.execute(events_by_day_stmt, (comm_date, 'activity', comm_activity))

        #
        # CHANNELCAP
        channel_cap_stmt = "INSERT INTO channelcap_day (channel, comm_date, comm_time) VALUES (%s, %s, %s)"
        self.astra_db_session.db_session.execute(channel_cap_stmt, (comm_channel, comm_date, comm_time))
        # update the events_by_day table
        self.astra_db_session.db_session.execute(events_by_day_stmt, (comm_date, 'channel', comm_channel))

        #
        # PRTYCAP
        prty_cap_stmt = "INSERT INTO prtycap_day (category_group, category, comm_date, comm_time) VALUES (%s, %s, %s, %s)"
        self.astra_db_session.db_session.execute(prty_cap_stmt, ("group", comm_category, comm_date, comm_time))
        # update the events_by_day table
        self.astra_db_session.db_session.execute(events_by_day_stmt, (comm_date, 'priority', comm_category))

        #
        # PROFILECAP
        profile_cap_stmt = "INSERT INTO profilecap_month (customer_id, channel, comm_month, comm_day, communication_id) VALUES (%s, %s, %s, %s, %s)"
        self.astra_db_session.db_session.execute(profile_cap_stmt, (comm_customer_id, comm_channel, comm_month, comm_day, comm_id))
        

        #
        # Update Offests
        if self.should_update_offsets(10): # update every 10 messages
            
            # ACTIVITY
            cap_type = 'activity'
            # Todo: change this to iterate through all the Cap Types
            
            # Iterate through each activity
            for activity in ['credit_card', 'mortgage', 'business', 'retail', 'investment']:
            # Todo: change this to read the set of activities from the events_by_day table

                # Get the offset count
                (comm_offset_time, comm_offset_count) = self.get_offset_count(cap_type, activity, comm_date)
            
                # Get the count of rows since last offset
                (row_count, comm_time) = self.get_row_count(cap_type, activity, comm_offset_time)

                # Total count = offset_count + rows since last offset
                total_comm_count = comm_offset_count + row_count
                self.logger.info(f"COUNT {cap_type}|{activity} - Total Count: {total_comm_count},  Row Count: {row_count}, Offset Count: {comm_offset_count}")

                if (row_count > 0):
                  self.logger.info(f"UPDATE OFFSET {cap_type}|{activity}")

                  # Update the offset_count in the cap table
                  update_acty_cap_stmt = "UPDATE actycap_day SET comm_offset_time = %s, comm_offset_count = %s WHERE activity = %s AND comm_date = %s"
                  self.astra_db_session.db_session.execute(update_acty_cap_stmt, (comm_time, total_comm_count, activity, comm_date))
                
        
        return comm_id
    


    



###
### DATABASE CLASS
###

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
        self.db_session.set_keyspace(ASTRA_KEYSPACE)
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