drop database if exists my_spark;

create database my_spark;


drop table if exists my_spark.client_info;

create table my_spark.client_info
(
    id           int comment '主键id',
    client_no    string comment '客户号',
    client_name  string comment '客户姓名',
    sex          string comment '性别',
    id_number    string comment '证件号码',
    birthday     string comment '出生日期',
    mobile_phone string comment '手机号码',
    nation       string comment '国籍'
) partitioned by (generate_date string comment '数据生成日期')
    row format delimited
        fields terminated by ','
        lines terminated by '\n'
    stored as textfile;



insert into table my_spark.client_info partition (generate_date = '20230814')
values (1, 'C2023080001', '王二小', 'M', '23111231200', '2003-01-07', '1234567891', 'AD'),
       (2, 'C2023080002', '张静', 'F', '23111231211', '2009-11-01', '1234567892', 'BB'),
       (3, 'C2023080003', '孙秀丽', 'F', '23111231222', '1998-07-23', '1234567893', 'IS'),
       (4, 'C2023080004', '李思儿', 'M', '23111231233', '2013-02-14', '1234567894', 'TW'),
       (5, 'C2023080005', '韩珊珊', 'F', '23111231244', '2005-08-28', '1234567895', 'NL'),
       (6, 'C2023080006', '苟熊', 'M', '23111231255', '1994-10-11', '1234567896', 'AQ');



drop table if exists my_spark.nation_base;

create table my_spark.nation_base
(
    id          int comment '主键id',
    nation_code string comment '国家代码',
    nation_name string comment '国家名称'
) row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile;


insert into my_spark.nation_base
values (1, 'AD', '安道尔'),
       (2, 'AQ', '南极洲'),
       (3, 'BB', '巴巴多斯'),
       (4, 'US', '美国'),
       (5, 'JP', '日本'),
       (6, 'TW', '台湾'),
       (7, 'NL', '荷兰'),
       (8, 'IS', '冰岛'),
       (9, 'GU', '关岛'),
       (10, 'GL', '格林兰');
