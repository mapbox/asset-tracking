create database assets;
-- Create the table in assets database
use assets;
-- Create a columnstore table 
-- KEY: sort key, designated for columnstore table
-- This allows for much faster analytic querying and utilizes disk.
-- SHARD KEY: distribution key. Co-locate all data with the same key on the same shard
create table assetlog
(
    ts INT,
    elevation FLOAT,
    geofencestatus VARCHAR(10),
    geofencename VARCHAR(255),
    longitude FLOAT,
    latitude FLOAT,
    id INT,
    KEY (`ts`) USING CLUSTERED COLUMNSTORE,
    SHARD KEY (`id`)
);

-- This is MemSQL syntax for creating a pipeline
-- Point at a bucket and pass credentials
-- Credentials can also be passed via EC2 Instance role
-- Final three lines are important
-- LINES STARTING BY: Ignore the initial double-quote
-- LINES TERMINATED BY: Know when to move to the next row
-- FIELDS TERMINATED BY: Know when to move the next column
CREATE PIPELINE assetingest
AS LOAD DATA S3 'BUCKET'
CONFIG '{"region": "REGION"}'
CREDENTIALS '{"aws_access_key_id": "AWS ACCESS KEY", "aws_secret_access_key": "AWS SECRET KEY"}'
INTO TABLE `assetlog`
LINES STARTING BY '"'
LINES TERMINATED BY '\n'
FIELDS TERMINATED BY ',';

-- Start the Pipeline and validate it is running
start pipeline assetingest;
show pipelines;

-- Data will be available within 1 minute, verify by running this
select count(*) from assetlog;

-- To see timeseries by ID
select * from assetlog
order by id, ts;