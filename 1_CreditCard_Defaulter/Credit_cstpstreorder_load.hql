use custdb;

Create temporary table pst_temp (id int,lmt int,sex int,edu int,marital int,age int,pay int,billamt int,defaulter int,issuerid1 int,issuerid2 int,tz string) row format delimited fields terminated by ',';
load data  inpath '/user/hduser/CreditCard_Defaulter_project/credits_pst/' into table pst_temp;

Create temporary table cst_temp (id int,lmt int,sex int,edu int,marital int,age int,pay int,billamt int,defaulter int,issuerid1 int,issuerid2 int,tz string) row format delimited fields terminated by ',';
load data  inpath '/user/hduser/CreditCard_Defaulter_project/credits_cst/' into table cst_temp;


drop table if exists cstpstreorder;
Create  table cstpstreorder (id int,newlmt int,sex int,edu int,marital int,age int,pay int,newbillamt int,defaulter int,issuerid1 int,issuerid2 int,tz string,lmt int,billamt int) row format delimited fields terminated by ',';

insert into cstpstreorder  select id,case defaulter when 1 then lmt-(lmt*0.04) else lmt end as newlmt,sex,edu,marital,age,pay,case defaulter when 1 then billamt+(billamt*0.02) else billamt end as newbillamt ,defaulter ,issuerid1,issuerid2,tz,lmt,billamt from pst_temp where billamt>0 union all
select id,case defaulter when 1 then lmt-(lmt*0.04) else lmt end as newlmt,sex,edu,marital,age,pay,case defaulter when 1 then billamt+(billamt*0.02) else billamt end as newbillamt ,defaulter ,issuerid1,issuerid2,tz,lmt,billamt from cst_temp where billamt>0;


INSERT OVERWRITE DIRECTORY '/user/hduser/CreditCard_Defaulter_project/defaultersout' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * FROM cstpstreorder where defaulter=1; 
INSERT OVERWRITE DIRECTORY '/user/hduser/CreditCard_Defaulter_project/nondefaultersout' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * FROM cstpstreorder where defaulter=0; 
