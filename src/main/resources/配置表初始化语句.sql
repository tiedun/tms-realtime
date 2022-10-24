CREATE TABLE `tms_config_dim` (
  `source_table` varchar(200) NOT NULL COMMENT '数据源表',
  `sink_table` varchar(200) DEFAULT NULL COMMENT '目标表',
  `sink_columns` varchar(200) DEFAULT NULL COMMENT '目标表字段',
  `primary_key` varchar(200) DEFAULT NULL COMMENT '目标表主键',
  `sink_extend` varchar(200) DEFAULT NULL COMMENT '建表扩展语句',
  PRIMARY KEY (`source_table`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='物流实时配置表';

INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('base_complex', 'dim_base_complex', 'id,complex_name,province_id,city_id,district_id,district_name,create_time,update_time,is_deleted', 'id', 'salt_buckets=4');
INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('base_organ', 'dim_base_organ', 'id,org_name,org_level,region_id,org_parent_id,create_time,update_time,is_deleted', 'id', '');
INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('base_region_info', 'dim_base_region_info', 'id,parent_id,name,dict_code,short_name,create_time,update_time,is_deleted', 'id', '');
INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('employee_info', 'dim_employee_info', 'id,username,password,real_name,id_card,phone,birthday,gender,address,employment_date,graduation_date,education,position_type,create_time,update_time,is_deleted', 'id', 'salt_buckets=4');
INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('express_courier', 'dim_express_courier', 'id,emp_id,org_id,working_phone,express_type,create_time,update_time,is_deleted', 'id', '');
INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('line_base_info', 'dim_line_base_info', 'id,name,line_no,line_level,org_id,transport_line_type_id,start_org_id,start_org_name,end_org_id,end_org_name,pair_line_id,distance,cost,estimated_time,status,create_time,update_time,is_deleted', 'id', '');
INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('line_base_shift', 'dim_line_base_shift', 'id,line_id,start_time,est_end_time,driver1_emp_id,driver2_emp_id,truck_id,pair_shift_id,is_enabled,create_time,update_time,is_deleted', 'id', 'salt_buckets=4');
INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('truck_driver', 'dim_truck_driver', 'id,emp_id,org_id,team_id,license_type,init_license_date,expire_date,license_no,is_enabled,create_time,update_time,is_deleted', 'id', '');
INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('truck_info', 'dim_truck_info', 'id,team_id,truck_no,truck_model_id,device_gps_id,engine_no,license_registration_date,license_last_check_date,license_expire_date,is_enabled,create_time,update_time,is_deleted', 'id', '');
INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('truck_model', 'dim_truck_model', 'id,model_name,model_type,model_no,brand,truck_weight,load_weight,total_weight,eev,boxcar_len,boxcar_wd,boxcar_hg,max_speed,oil_vol,create_time,update_time,is_deleted', 'id', '');
INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('truck_team', 'dim_truck_team', 'id,name,team_no,org_id,manager_emp_id,create_time,update_time,is_deleted', 'id', '');
INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('user_address', 'dim_user_address', 'id,user_id,phone,province_id,city_id,district_id,complex_id,address,is_default,create_time,update_time,is_deleted', 'id', 'salt_buckets=4');
INSERT INTO `tms_config_dim`(`source_table`, `sink_table`, `sink_columns`, `primary_key`, `sink_extend`) values('user_info', 'dim_user_info', 'id,login_name,nick_name,passwd,real_name,phone_num,email,user_level,birthday,gender,create_time,update_time,is_deleted', 'id', 'salt_buckets=4');
