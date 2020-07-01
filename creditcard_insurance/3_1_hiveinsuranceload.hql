create database if not exists insure;

use insure;

drop table if exists insurance;

CREATE TABLE insurance (IssuerId1 int,IssuerId2 int,BusinessYear int,StateCode string,SourceName string,NetworkName string,NetworkURL string,RowNumber int,MarketCoverage string,DentalOnlyPlan string) 
row format delimited fields terminated by ','
TBLPROPERTIES ("skip.header.line.count"="1");

load data local inpath '/home/hduser/creditcard_insurance/insuranceinfo.csv' into table insurance;


