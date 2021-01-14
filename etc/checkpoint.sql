create table rlink_ck
(
	id int auto_increment comment 'pk'
		primary key,
	application_name varchar(128) default '' not null comment 'application name',
	application_id varchar(128) default '' not null comment 'application id',
	job_id int default 0 not null comment 'job id',
    task_number int default 0 not null comment 'task number',
    num_tasks int default 0 not null comment 'num tasks',
	operator_id int default 0 not null comment 'job id',
	checkpoint_id bigint default 0 not null comment 'checkpoint id',
	handle varchar(2000) default '' not null comment 'checkpoint handle can access checkpoint state. eg: mq''s offset, file''s path',
	create_time datetime default '1900-01-01 00:00:00' not null comment 'create datetime'
)