drop user if exists u1,u2,u3,u4,u5,u6,u7,u8,u9,u10;
create user u1 identified by '111', u2 identified by '111' default role public;
create user u3 identified by '111', u4 identified by '111';
create user u5 identified by '111', u6 identified by '111' default role moadmin;
create user u7 identified by '111', u8 identified by '111' default role rx;

drop role if exists r1;
create role r1;
create user u9 identified by '111', u10 identified by '111' default role r1;


drop user if exists u1,u2,u3,u4,u5,u6,u7,u8,u9,u10;
drop role r1;