use zhgl;

create table if not exists S24_BACCT(
bank           string,
bankacct       string,
acct_des       string,
drcr_role      string,
glflag         string,
bp_flag        string,
res_flag       string,
curr_num       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_bacct';