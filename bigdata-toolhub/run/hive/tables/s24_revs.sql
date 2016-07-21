use zhgl;

create table if not exists S24_REVS(
xaccount         string,
action_day       string,
bank             string,
category         string,
create_day       string,
create_tm        string,
rpt_name         string,
xsource          string,
business         string,
curr_num         string,
cycle_nbr        string,
month_nbr        string,
reasn_code       string,
ref_nbr          string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_revs';