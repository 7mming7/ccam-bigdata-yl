use zhgl;

create table if not exists S24_ISPCS(
bank             string,
scrn_name        string,
acc_code         string,
add_level        string,
apply_ind        string,
branch_acc       string,
chg_day          string,
chg_empl         string,
chg_level        string,
del_level        string,
input_scr        string,
inq_level        string,
no_switch        string,
scrn_desc        string,
upd_ind          string,
ovrid_flag       string
)
row format delimited
fields terminated by '|'
stored as textfile 
location '/user/hive/warehouse/HDFSDIRNAME/s24_ispcs';