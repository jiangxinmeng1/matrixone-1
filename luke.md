# SQL语句分析与优化
## 一、SQL语句整理
### （一）原始SQL语句
```sql
{
    "UPDATE `task` SET `db_status`=2,`end_time`=NULL,`update_time`=1739531700 WHERE id = 1889857503637045248 AND db_status in (1) ;",
    "UPDATE `task` SET `db_status`=1,`end_time`=1739531700,`update_time`=1739531700 WHERE id = 1889857503637045248 AND db_status in (2) ;",
    "SELECT count(*) FROM `task` WHERE source_connector_id in (190004,240003)",
    "SELECT * FROM `task` WHERE source_connector_id in (190004,240003)",
    "SELECT * FROM `task` WHERE uid = '6' ORDER BY create_time desc LIMIT 10",
    "SELECT count(*) FROM `task` WHERE uid = '6'",
    "SELECT * FROM `connector_job` WHERE task_log_id =1889857505604173828;",
    "SELECT status, count(*) as count FROM `connector_job` WHERE task_log_id = 1889857505604173828 GROUP BY `status`",
    "SELECT count(*) FROM `connector` WHERE user_id ='019462df-e3cb-7127-8eb5-d35d2e226187'",
    "SELECT * FROM `connector` WHERE user_id = '019462df-e3cb-7127-8eb5-d35d2e226187' ORDER BY created_at ASC LIMIT 10",
    "SELECT * FROM `connector` WHERE id = 310007",
    "SELECT * FROM `file` WHERE file.id = 1890762739305349137",
    "SELECT count(*) FROM `file` WHERE file.task_id = 1890754962524667904",
    "SELECT * FROM `file` WHERE file.task_id = 1890754962524667904",
    "SELECT file.uid, count(*) as count, sum(size) as size_by_bytes, sum(connector_job.upload_end_time - connector_job.created_at) as latency FROM `file` Left Join connector_job on file.job_id = connector_job.id WHERE file.type not in (2) GROUP BY `file`.`uid`"
sql2 := "INSERT INTO `task_log` (`task_id`,`creator`,`status`,`create_time`,`update_time`,`start_time`) VALUES (1889857503637045248,'admin',1,1739531700,1739531700,1739531700);"
}
```
## 二、耗时较长的SQL语句及解决方案
### （一）`SELECT * FROM connector_job WHERE task_log_id =1889857505604173828;`

### （二）`SELECT * FROM `file` WHERE file.task_id = 1890754962524667904;`
对于`SELECT * FROM `connector_job` WHERE task_log_id =1889857505604173828;`和`SELECT * FROM `file` WHERE file.task_id = 1890754962524667904;`这两条单表查询语句，通过在`connector_job`表的`task_log_id`字段和`file`表的`task_id`字段上添加索引，可以显著提高查询速度。索引能够减少表扫描的行数，快速定位到符合条件的数据。
### （三）`SELECT file.uid, count(*) as count, sum(size) as size_by_bytes, sum(connector_job.upload_end_time - connector_job.created_at) as latency FROM `file` Left Join connector_job on file.job_id = connector_job.id WHERE file.type not in (2) GROUP BY `file`.`uid`;`
```
 Project                                                                                                                                                                                                                                                           |
|   Analyze: timeConsumed=19ms waitTime=1026ms inputRows=966184 outputRows=966184 InputSize=77mb OutputSize=77mb MemorySize=2mb                                                                                                                                     |
|   ->  Aggregate                                                                                                                                                                                                                                                   |
|         Analyze: timeConsumed=590ms group_time=[total=383ms,min=0ms,max=383ms,dop=8] waitTime=0ms inputRows=0 outputRows=966184 InputSize=0bytes OutputSize=77mb MemorySize=0bytes                                                                                |
|         Group Key: file.uid shuffle: range(file.uid)                                                                                                                                                                                                              |
|         Aggregate Functions: starcount(1), sum(file.size), sum((connector_job.upload_end_time - connector_job.created_at))                                                                                                                                        |
|         ->  Join                                                                                                                                                                                                                                                  |
|               Analyze: timeConsumed=4503ms waitTime=966ms inputRows=2472736 outputRows=1030576 InputSize=56mb OutputSize=82mb MemorySize=32mb                                                                                                                     |
|               Join Type: RIGHT                                                                                                                                                                                                                                    |
|               Join Cond: (connector_job.id = file.job_id) shuffle: range(connector_job.id)                                                                                                                                                                        |
|               ->  Table Scan on test.connector_job                                                                                                                                                                                                                |
|                     Analyze: timeConsumed=213ms scan_time=[total=59ms,min=5ms,max=10ms,dop=8] filter_time=[total=153ms,min=0ms,max=151ms,dop=8] waitTime=0ms inputBlocks=304 inputRows=2472736 outputRows=2472736 InputSize=56mb OutputSize=56mb MemorySize=109mb |
|               ->  Table Scan on test.file                                                                                                                                                                                                                         |
|                     Analyze: timeConsumed=84ms scan_time=[total=43ms,min=3ms,max=6ms,dop=8] waitTime=0ms inputBlocks=127 inputRows=1030768 outputRows=1030576 InputSize=82mb OutputSize=74mb MemorySize=10mb                                                      |
|                     Filter Cond: (file.type != 1) 
```
对于`SELECT file.uid, count(*) as count, sum(size) as size_by_bytes, sum(connector_job.upload_end_time - connector_job.created_at) as latency FROM `file` Left Join connector_job on file.job_id = connector_job.id WHERE file.type not in (2) GROUP BY `file`.`uid`;`这条复杂的多表连接和聚合查询语句，目前没有有效的优化方法。 