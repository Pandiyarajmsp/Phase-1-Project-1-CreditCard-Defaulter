
#!/bin/bash
echo "Table creation"
mysql -u root -proot -D custdb -e "drop table if exists middlegrade;
create table middlegrade (issuerid integer,businessyear integer,statedesc varchar(200),sourcename varchar(100),maskednetworkurl varchar(200),sex varchar(10),grade varchar(20),marital varchar(10),newbillamt integer,defaulter varchar(20));"
echo "Table creation completed"

sqoop export --connect jdbc:mysql://localhost/custdb --username root --password root --table middlegrade --export-dir /user/hduser/CreditCard_Defaulter_project/middlegrademaskedout --fields-terminated-by ',';



