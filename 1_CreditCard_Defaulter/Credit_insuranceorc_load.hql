

use insure;


drop view if exists middlegradeview;
create view middlegradeview as
select
issuerid,businessyear,statedesc,sourcename,translate(translate(translate(translate(translate(networkurl
,'a','x'),'b','y'),'c','z'),'s','r'),'.com','.aaa') as maskednetworkurl,sex,grade,marital,newbillamt,defaulter
from insuranceorc
where grade='middle grade'
and issuerid is not null;



insert overwrite directory '/user/hduser/CreditCard_Defaulter_project/middlegrademaskedout' select concat(issuerid,',',businessyear,',',statedesc,',',sourcename,',',maskednetworkurl,',',sex,',',grade,',',marital,',',newbillamt,',',defaulter) from middlegradeview;

