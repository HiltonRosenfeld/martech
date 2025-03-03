-- # Table: comm_by_activity_day
--
-- This table will store the number of messages sent in a day for a given activity.
--
-- The number of messages sent per day is currently 4 million. This is expected to grow to 20 million a day. To store
-- up to 20 million messages in a day we need to be very efficient with storage. There is no need to store the message
-- id as we just want to count the number of messages in a given time period. To save on space, we store the time the
-- message was written in a custom format that is smaller than a timestamp.
--
CREATE TABLE IF NOT EXISTS comm_by_activity_day (
    activity_name text,
    comm_date int,
    comm_time int,
    comm_offset_time int static,
    comm_offset_count int static,
    PRIMARY KEY ((activity_name, comm_date), comm_time)
) WITH CLUSTERING ORDER BY (comm_time DESC) AND default_time_to_live = 604800;

--
-- ## Columns
--  * activity_name: assumed to be around 10 characters e.g. "mortgage" or "creditcard"
--  * comm_date: the day (bucket) date in the format YYYYMMDD; there is a new partition for each day.
--  * comm_time: the time a message is written in the format HHMMSSssss; there is a new row for each write message time.
--  * comm_offset_time: the time offset in the same format as comm_time; use this in conjunction with comm_offset_count
--      to reduce the number of rows read.
--  * comm_offset_count: the number of messages from the start of the day to comm_offset_time.
--
-- ## Usage
-- Use comm_offset_time to reduce the number of rows we need to read to get the message count for a given time period.
-- At the start of each day this value will be null. The application can periodically store the last comm_offset_time
-- and comm_offset_count after performing a read operation to reduce the number of rows on subsequent reads.
--
-- Use the following statement to get the information needed to count the number of messages in a day:
--
      SELECT comm_time, comm_offset_time, comm_offset_count
      FROM comm_by_activity_day
      WHERE activity_name = '<activity>'
          AND comm_date = <today>
          AND comm_time > <offset_time>;
--
-- In the above query, the value for `offset_time` in the `AND comm_time` term can be 'null'. In this case Astra /
-- Cassandra / DSE will interpret this as 0 and return all rows with a comm_time greater than 0.

-- Using the information from the above query, the application can count the number of rows returned and add the
-- comm_offset_count to get the total number of messages in a day. In addition, the application may choose to write
-- back to the table to update the comm_offset_time and comm_offset_count. In this case, the comm_offset_time will be
-- the last comm_time value read, and the comm_offset_count will be the number of comm_time values counted up to that
-- time.
--
-- Use the following statement to update the comm_offset_time and comm_offset_count:
--
      UPDATE comm_by_activity_day
      SET comm_offset_time = <latest_comm_time>, comm_offset_count = <comm_time_rows_counted>
      WHERE activity_name = '<activity>'
          AND comm_date = <today>;
--
-- ## Storage
-- A Time-To-Live (TTL) of 7 days is set on the table. This is because the data is only useful for a day, however
-- we store it for 7 days to provide a grace period for the application to read the data. After 7 days the data will be
-- marked for deletion from the time it was last written to. A such, the table will contain at most 7 days of live data,
-- and 10 days of data that is marked for deletion; i.e, 17 days in total. The 10 days of data marked for deletion is a
-- grace period set by Astra / Cassandra / DSE to allow the deletes to be replicated to all copies of the data.
--
-- row size = comm_time
--          = 4 bytes
--
-- partition size = row size * 20 million message + (activity_name + comm_date + comm_offset_time + comm_offset_count)
--                ~ 80 MB

-- table size = partition size * 17 days
--            ~ 1.36 GB
--
-- The above sizing is based on expected growth of 20 million messages in a day. The actual size will vary depending
-- on the number of messages written in a day. Currently, there are around 4 million messages written in a day.
-- Even with 4 million messages, the partition size is around 16 MB which is over the recommended size of 10 MB.
-- To maintain read performance, and the time to calculate the number of messages in a day, the comm_offset_time and
-- comm_offset_count should be updated regularly throughout the day. This will reduce the number of rows read and
-- the amount of work the application needs to do to get the total number of messages sent in a day. That is, only a
-- portion of the partition will need to be read to get the message count for a given time period.



-- # Table: comm_by_activity_week
--
-- This table will store the number of messages sent on every day in a given year for a given activity.
--
-- The number of messages sent per day around 4 million and expected to grow to 20 million. Rather than store each
-- message sent in a day for an activity, we store the total number of messages sent in a day. This significantly
-- reduces the number of rows stored and the amount of work the application needs to do to get the total number of
-- messages sent for a certain time period. The number of messages sent in a day can be obtained from the
-- comm_by_activity_day table and used to populate this table.
--
CREATE TABLE IF NOT EXISTS comm_by_activity_week (
    activity_name text,
    comm_date int,
    comm_count int,
    PRIMARY KEY ((activity_name), comm_date)
) WITH CLUSTERING ORDER BY (comm_date DESC) AND default_time_to_live = 7776000;
--
-- ## Columns
--  * activity_name: assumed to be around 10 characters e.g. "mortgage" or "creditcard"
--  * comm_date: the day date in the format YYYYMMDD; there is a new row for each day in the week.
--  * comm_count: the number of messages sent for the given comm_date day.
--
-- ## Usage
-- This table will contain the number of messages sent on each day in an activity. It can be used to obtain the number
-- of messages sent in a seven-day period, or over a 30-day period. This can be done by providing a date range for the
-- comm_date column.
--
-- Use the following statement to get the number of messages sent in a seven-day period:
--
      SELECT comm_count
      FROM comm_by_activity_week
      WHERE activity_name = '<activity>'
          AND comm_date >= <start_date>
          AND comm_date <= <end_date>;
--
-- In the above query, the `start_date` and `end_date` can be used to get the number of messages sent in a seven-day
-- period. The application can then sum the comm_count values to get the total number of messages sent in the given
-- time period. The same steps can be done for a 30-day period.
-- When a new day is started in the comm_by_activity_day table, the total message count from the previous day will need
-- to be stored in this table.
--
-- ## Storage
-- A TTL of 90 days is set on the table, as per the retention requirements. This means that an activity partition will
-- contain at most 90 days of live data, and 10 days of data that is marked for deletion; i.e, 100 days in total.
--
-- row size = comm_date + comm_count
--          = 8 bytes
--
-- partition size = row size * 100 days + activity_name
--                ~ 800 bytes
--
-- table size = partition size * number activities