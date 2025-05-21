create database if not exists logs;

create table if not exists runlog (
    process varchar(50),
    run_id int, 
    start_time datetime,
    complete_time datetime,
    error tinyint,
    user varchar(50),
    log_path varchar(255),
    dtl_log_path varchar(255),
    primary key (process, run_id)
);