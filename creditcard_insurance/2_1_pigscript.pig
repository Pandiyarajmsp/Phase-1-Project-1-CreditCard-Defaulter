pst = load '/user/hduser/credits_pst/' using PigStorage (',') as (id:int,lmt:int,sex:int,edu:int,marital:int,age:int,pay:int,billamt:int,defaulter:int,issuerid1:int,issuerid2:int,tz:chararray);
cst = load '/user/hduser/credits_cst/' using PigStorage (',') as(id:int,lmt:int,sex:int,edu:int,marital:int,age:int,pay:int,billamt:int,defaulter:int,issuerid1:int,issuerid2:int,tz:chararray);
cstpst = UNION cst,pst;
cstpstreorder = foreach cstpst generate id,issuerid1,issuerid2,lmt,sex,edu,marital,pay,billamt,defaulter;
cstpstpenality = foreach cstpstreorder generate id,issuerid1,issuerid2,lmt,case defaulter when 1 then lmt-(lmt*.01) else lmt end as newlmt ,sex,edu,marital,pay,billamt,case defaulter when 1 then billamt-(billamt*.02) else billamt end as newbillamt,defaulter;
SPLIT cstpstpenality into def if defaulter == 1, nondef if defaulter == 0;
store def into '/user/hduser/defaultersout/' using PigStorage (',');
store def into '/user/hduser/nondefaultersout/' using PigStorage (',');
