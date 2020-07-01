create database if not exists insure;
use insure;
drop table if exists insurance;
CREATE TABLE insurance (IssuerId1 int,IssuerId2 int,BusinessYear int,StateCode string,SourceName
string,NetworkName string,NetworkURL string,RowNumber int,MarketCoverage string,DentalOnlyPlan
string)
row format delimited fields terminated by ','
TBLPROPERTIES ("skip.header.line.count"="1");
load data local inpath '/home/hduser/creditcard_insurance/insuranceinfo.csv' into table insurance;


use insure;
drop table if exists state_master;
CREATE EXTERNAL TABLE state_master (statecd STRING, statedesc STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES ("input.regex" = "(.{2})(.{20})" )
LOCATION '/user/hduser/states';
load data local inpath '/home/hduser/creditcard_insurance/states_fixedwidth' overwrite into table
state_master;

use insure;
drop table if exists defaulters;
CREATE TABLE defaulters (id int,newlmt int,sex int,edu int,marital int,age int,pay int,newbillamt int,defaulter int,IssuerId1 int,IssuerId2 int,tz string,lmt int,billamt int)
row format delimited fields terminated by ','
LOCATION '/user/hduser/CreditCard_Defaulter_project/defaultersout';







