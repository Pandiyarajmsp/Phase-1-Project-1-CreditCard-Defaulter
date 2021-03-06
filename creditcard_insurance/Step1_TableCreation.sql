drop table if exists credits_pst;
drop table if exists credits_cst;
drop table if exists custmaster;

create table if not exists credits_pst (id integer,lmt integer,sex integer,edu integer,marital integer,age integer,pay integer,billamt integer,defaulter integer,issuerid1 integer,issuerid2 integer,tz varchar(3));

create table if not exists credits_cst (id integer,lmt integer,sex integer,edu integer,marital integer,age integer,pay integer,billamt integer,defaulter integer,issuerid1 integer,issuerid2 integer,tz varchar(3));

create table if not exists custmaster (id integer,fname varchar(100),lname varchar(100),ageval integer,profession varchar(100));
