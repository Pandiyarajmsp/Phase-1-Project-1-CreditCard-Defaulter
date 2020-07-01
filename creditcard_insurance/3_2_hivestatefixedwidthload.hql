drop table if exists state_master;

CREATE EXTERNAL TABLE state_master (statecd STRING, statedesc STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe' 
WITH SERDEPROPERTIES ("input.regex" = "(.{2})(.{20})" ) 
LOCATION '/user/hduser/states';

load data local inpath '/home/hduser/creditcard_insurance/states_fixedwidth' overwrite into table state_master;
