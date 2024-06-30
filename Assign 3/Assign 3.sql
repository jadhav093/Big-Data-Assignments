1. Execute following queries on MySQL emp database using Recursive CTEs (not supported in Hive 3.x).
1. Find years in range 1975 to 1985, where no emps were hired.

with recursive years(n) as(
    (select 1975)
    union
    (select n+1 from years where n<1985)

)
select * from years where n not in (select year(hire) from emp);


+------+
| n    |
+------+
| 1975 |
| 1976 |
| 1977 |
| 1978 |
| 1979 |
| 1984 |
| 1985 |
+------+


2. Display emps with their level in emp hierarchy. Level employee is Level of his manager + 1.


with recursive cte1 as(
    (select empno, ename, mgr,1 AS level from emp where mgr IS NULL)
    union
    (select e.empno,e.ename,e.mgr,level+1 from emp e inner join cte1 on cte1.empno=e.mgr)
)
select * from cte1;


3. Create a "newemp" table with foreign constraints enabled for "mgr" column. Also enable DELETE ON CASCADE for the same. Insert data into the table from emp table. Hint: You need to insert data levelwise to avoid FK constraint error.



alter table emp add primary key (empno);

CREATE TABLE newemp(
    empno int primary key,
    ename char(20),
    job CHAR(20),
    mgr int,
    hire date,
    sal decimal(8,2),
    comm decimal(8,2),
    deptno int,
    foreign key (mgr) references newemp(empno)
    ON DELETE CASCADE
);


INSERT INTO newemp (empno, ename, job, mgr, hire, sal, comm, deptno)
SELECT empno, ename, job, mgr, hire, sal, comm, deptno
FROM (
    WITH RECURSIVE EmpHierarchy AS (
        SELECT empno, ename, job, mgr, hire, sal, comm, deptno
        FROM emp
        WHERE mgr IS NULL
        UNION ALL
        SELECT e.empno, e.ename, e.job, e.mgr, e.hire, e.sal, e.comm, e.deptno
        FROM emp e
        INNER JOIN EmpHierarchy eh ON e.mgr = eh.empno
    )
    SELECT * FROM EmpHierarchy
) AS eh;





4. From "newemp" table, delete employee KING. What is result?

DELETE FROM newemp WHERE empno = 7839;

DELETE FROM newemp WHERE ename = "KING";


2. Implement movie recommendation in python/java + hive.


3. Create ORC table emp_job_part to partition emp data jobwise. Upload emp data dynamically into these partitions.


CREATE TABLE emp_job_part(
    empno int,
    ename STRING,
    mgr INT,
    hire DATE,
    sal DOUBLE,
    comm DOUBLE,
    deptno INT
)
PARTITIONED BY (job STRING)
STORED AS ORC
TBLPROPERTIES('transactional'='true');

INSERT INTO emp_job_part PARTITION(job)
SELECT empno, ename, mgr, hire, sal, comm, deptno, job FROM emp_staging;


4. Create ORC table emp_job_dept_part to partition emp data jobwise and deptwise. Also divide them into two buckets by empno. Upload emp data dynamically into these partitions.


CREATE TABLE emp_job_dept_part(
    empno int,
    ename STRING,
    mgr INT,
    hire DATE,
    sal DOUBLE,
    comm DOUBLE
   
)
PARTITIONED BY (job STRING,deptno INT)
CLUSTERED BY (empno) INTO 2 BUCKETS
STORED AS ORC;

INSERT INTO emp_job_dept_part PARTITION(job,deptno)
SELECT empno, ename, mgr, hire, sal, comm, job,deptno FROM emp_staging;

5. Load Fire data into Hive in a staging table " re_staging".

create table fire_staging(
call_number BIGINT, unit_id STRING, incident_number BIGINT, call_type STRING, call_date string, watch_date string, received_dttm string, entry_dttm string, dispatch_dttm string, response_dttm string, onscene_dttm string, transport_dttm string, hospital_dttm string, final_disp string, available_dttm string, address string, city string, zipcode_inci BIGINT, battalion string, station_area int, box int, orig_prio string, prio string, final_prio int, als_unit string, call_type_grp string, number_of_alarms int, unit_type string, seq_call_disp int, fire_prev_dist int, supervisor_dist int, neighbor string, row_id string, case_loc string, data_as_of string, data_loaded_at string, analysis_neighbor int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES('skip.header.line.count'='1');


LOAD DATA LOCAL INPATH '/home/sarang/sunbeam_material/Fire_Department_Calls_for_Service.csv' INTO TABLE re_staging;

6. Create a transactional ORC table " re_data" with appropriate data types partitioned by city and buckted by call number into 4 buckets. Load data from staging table into this table.


CREATE  TABLE fire_data(
    call_number BIGINT,
    unit_id string,
    incident_number BIGINT,
    call_type string,
    call_date DATE,
    watch_date DATE,
    received_dtm TIMESTAMP,
    entry_dtm TIMESTAMP,
    dispatch_dtm TIMESTAMP,
    response_dtm TIMESTAMP,
    on_scene_dtm TIMESTAMP,
    transport_dtm TIMESTAMP,
    hospital_dtm TIMESTAMP,
    call_final_disposition STRING,
    available_dtm TIMESTAMP,
    address STRING,
    
    zipcode_of_incident BIGINT,
    battalion STRING,
    station_area INT,
    box int,
    original_priority string,
    priority string,
    final_priority int,
    als_unit string,
    call_type_group string,
    number_of_alarms int,
    unit_type string,
    unit_seq int,
    fire_prev int,
    supervisor_district int,
    neighborhoods string,
    row_id string,
    case_location string,
    data_as TIMESTAMP,
    data_loaded TIMESTAMP,
    analysis_nei int
)
PARTITIONED BY (city string)
CLUSTERED by (call_number) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES('transacational'='true');

INSERT INTO fire_data PARTITION(city)
select 
call_number ,
    unit_id ,
    incident_number ,
    call_type ,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(call_date,'mm/dd/yy' ))),
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(watch_date,'mm/dd/yy' ))),
    FROM_UNIXTIME(UNIX_TIMESTAMP(received_dtm,'mm/dd/yy HH:mm:ss a' )),
    FROM_UNIXTIME(UNIX_TIMESTAMP(entry_dtm,'mm/dd/yy HH:mm:ss a' )),
    FROM_UNIXTIME(UNIX_TIMESTAMP(dispatch_dtm,'mm/dd/yy HH:mm:ss a' )),
    FROM_UNIXTIME(UNIX_TIMESTAMP(response_dtm,'mm/dd/yy HH:mm:ss a' )),
    FROM_UNIXTIME(UNIX_TIMESTAMP(on_scene_dtm,'mm/dd/yy HH:mm:ss a' )),
    FROM_UNIXTIME(UNIX_TIMESTAMP(transport_dtm,'mm/dd/yy HH:mm:ss a' )),
    FROM_UNIXTIME(UNIX_TIMESTAMP(hospital_dtm,'mm/dd/yy HH:mm:ss a' )),
    call_final_disposition STRING,
    FROM_UNIXTIME(UNIX_TIMESTAMP(available_dtm,'mm/dd/yy HH:mm:ss a' )),
    address ,
    zipcode_of_incident ,
    battalion ,
    station_area ,
    box ,
    original_priority ,
    priority ,
    final_priority ,
    als_unit ,
    call_type_group ,
    number_of_alarms ,
    unit_type ,
    unit_seq ,
    fire_prev ,
    supervisor_district ,
    neighborhoods ,
    row_id ,
    case_location ,
     FROM_UNIXTIME(UNIX_TIMESTAMP(data_as,'mm/dd/yy HH:mm:ss a' )),
     FROM_UNIXTIME(UNIX_TIMESTAMP(data_loaded,'mm/dd/yy HH:mm:ss a' )),
    analysis_nei,
    city
from re_staging;


7. Execute following queries on re dataset.
1. How many distinct types of calls were made to the fire department?

select count(distinct call_number) from fire_data;

+----------+
|   _c0    |
+----------+
| 2969492  |
+----------+


2. What are distinct types of calls made to the re department?

SELECT DISTINCT call_type from fire_data;

+--------------------------------------+
|              call_type               |
+--------------------------------------+
| "Extrication / Entrapped (Machinery  |
| Administrative                       |
| Alarms                               |
| Electrical Hazard                    |
| Elevator / Escalator Rescue          |
| Fuel Spill                           |
| Gas Leak (Natural and LP Gases)      |
| High Angle Rescue                    |
| Marine Fire                          |
| Mutual Aid / Assist Outside Agency   |
| Odor (Strange / Unknown)             |
| Oil Spill                            |
| Other                                |
| Structure Fire / Smoke in Building   |
| Vehicle Fire                         |
| Water Rescue                         |
| Aircraft Emergency                   |
| Assist Police                        |
| Citizen Assist / Service Call        |
| Confined Space / Structure Collapse  |
| Explosion                            |
| HazMat                               |
| Industrial Accidents                 |
| Lightning Strike (Investigation)     |
| Medical Incident                     |
| Outside Fire                         |
| Smoke Investigation (Outside)        |
| Structure Fire                       |
| Suspicious Package                   |
| Traffic Collision                    |
| Train / Rail Fire                    |
| Train / Rail Incident                |
| Watercraft in Distress               |
+--------------------------------------+


3. Find out all responses for delayed times greater than 5 mins?


with cte as (select unix_timestamp(received_dttm)-unix_timestamp(response_dttm) as delayed_resp from fire_data) select count(delayed_resp) from cte where delayed_resp > 300;

+---------+
|   _c0   |
+---------+
| 960296  |
+---------+

4. What were the most common call types?

select call_type, count(call_type) from fire_data group by call_type order by count(call_type) desc;

+--------------------------------------+----------+
|              call_type               |   _c1    |
+--------------------------------------+----------+
| Medical Incident                     | 4247943  |
| Alarms                               | 720968   |
| Structure Fire                       | 714873   |
| Traffic Collision                    | 259541   |
| Other                                | 110855   |
| Citizen Assist / Service Call        | 96222    |
| Outside Fire                         | 85967    |
| Water Rescue                         | 34061    |
| Gas Leak (Natural and LP Gases)      | 30484    |
| Vehicle Fire                         | 28378    |
| Electrical Hazard                    | 21907    |
| Structure Fire / Smoke in Building   | 18894    |
| Elevator / Escalator Rescue          | 17952    |
| Smoke Investigation (Outside)        | 14613    |
| Odor (Strange / Unknown)             | 13673    |
| Fuel Spill                           | 7038     |
| HazMat                               | 4399     |
| Industrial Accidents                 | 3333     |
| Explosion                            | 3067     |
| Train / Rail Incident                | 1715     |
| Aircraft Emergency                   | 1512     |
| Assist Police                        | 1508     |
| High Angle Rescue                    | 1456     |
| Watercraft in Distress               | 1237     |
| "Extrication / Entrapped (Machinery  | 935      |
| Confined Space / Structure Collapse  | 791      |
| Mutual Aid / Assist Outside Agency   | 626      |
| Oil Spill                            | 518      |
| Marine Fire                          | 508      |
| Suspicious Package                   | 368      |
| Administrative                       | 345      |
| Train / Rail Fire                    | 120      |
| Lightning Strike (Investigation)     | 21       |
+--------------------------------------+----------+

5. What zip codes accounted for the most common calls?

select zipcode_inci, call_type, count(call_type) from fire_data group by zipcode_inci, call_type;

+---------------+--------------------------------------+------+
| zipcode_inci  |              call_type               | _c2  |
+---------------+--------------------------------------+------+
| NULL          | "Extrication / Entrapped (Machinery  | 935  |
| 94102         | Administrative                       | 28   |
| 94108         | Administrative                       | 4    |
| 94110         | Administrative                       | 119  |
| 94112         | Administrative                       | 7    |
| 94114         | Administrative                       | 24   |
| 94116         | Administrative                       | 6    |
| 94118         | Administrative                       | 3    |
| 94122         | Administrative                       | 7    |
| 94124         | Administrative                       | 35   |
+---------------+--------------------------------------+------+


6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

select count(analysis_neighbor) from fire_data where city="San Francisco" and zipcode_inci in (94102, 94103);

+---------+
|   _c0   |
+---------+
| 795804  |
+---------+

7. What was the sum of all calls, average, min, and max of the call response times?

select sum(UNIX_TIMESTAMP(response_dttm)),avg(UNIX_TIMESTAMP(response_dttm)),min(UNIX_TIMESTAMP(response_dttm)),max(UNIX_TIMESTAMP(response_dttm)) from fire_data;

+-------------------+-----------------------+------------+-------------+
|        _c0        |          _c1          |    _c2     |     _c3     |
+-------------------+-----------------------+------------+-------------+
| 8208810037395004  | 1.3675291908009758E9  | 955530711  | 1704027500  |
+-------------------+-----------------------+------------+-------------+


8. How many distinct years of data are in the CSV file?

select distinct year(call_date) from fire_data;

+-------+
|  _c0  |
+-------+
| NULL  |
| 2000  |
| 2002  |
| 2004  |
| 2006  |
| 2008  |
| 2010  |
| 2012  |
| 2014  |
| 2016  |
| 2018  |
| 2020  |
| 2022  |
| 2001  |
| 2003  |
| 2005  |
| 2007  |
| 2009  |
| 2011  |
| 2013  |
| 2015  |
| 2017  |
| 2019  |
| 2021  |
| 2023  |
+-------+
25 rows selected (343.17 seconds)


9. What week of the year in 2018 had the most fire calls?

select weekofyear(call_date), count(call_date) cnt from fire_data where year(call_date)=2018 and call_type like "%Fire%" group by weekofyear(call_date) order by cnt;

+------+------+
| _c0  | cnt  |
+------+------+
| 39   | 404  |
| 50   | 413  |
| 34   | 449  |
| 19   | 470  |
| 12   | 489  |
| 30   | 505  |
| 36   | 519  |
| 24   | 522  |
| 37   | 524  |
| 51   | 537  |
| 33   | 540  |
| 52   | 542  |
| 14   | 547  |
| 10   | 551  |
| 20   | 552  |
| 21   | 554  |
| 31   | 567  |
| 45   | 572  |
| 17   | 580  |
| 42   | 582  |
| 41   | 584  |
| 9    | 594  |
| 48   | 596  |
| 11   | 597  |
| 32   | 604  |
| 23   | 609  |
| 40   | 615  |
| 26   | 617  |
| 38   | 620  |
| 6    | 635  |
| 18   | 637  |
| 15   | 638  |
| 35   | 643  |
| 47   | 652  |
| 13   | 656  |
| 5    | 658  |
| 3    | 660  |
| 2    | 680  |
| 4    | 684  |
| 16   | 699  |
| 46   | 700  |
| 7    | 730  |
| 29   | 736  |
| 43   | 751  |
| 28   | 757  |
| 22   | 767  |
| 25   | 769  |
| 44   | 771  |
| 8    | 775  |
| 49   | 837  |
| 27   | 867  |
| 1    | 879  |
+------+------+


10. What neighborhoods in San Francisco had the worst response time in 2018?


select neighbor,UNIX_TIMESTAMP(response_dttm)-UNIX_TIMESTAMP(received_dttm) delay from fire_data where city='San Francisco' and year(call_date)=2018 order by delay desc limit 20;



+---------------------------------+--------+
|            neighbor             | delay  |
+---------------------------------+--------+
| Inner Sunset                    | 96859  |
| Inner Sunset                    | 96845  |
| Inner Sunset                    | 96730  |
| Inner Sunset                    | 96577  |
| Mission                         | 95979  |
| Excelsior                       | 91659  |
| Excelsior                       | 91543  |
| Mission                         | 91539  |
| Mission                         | 91537  |
| Financial District/South Beach  | 90926  |
| Tenderloin                      | 90723  |
| Portola                         | 89692  |
| Tenderloin                      | 89657  |
| Russian Hill                    | 89558  |
| Tenderloin                      | 89336  |
| Russian Hill                    | 89281  |
| Marina                          | 89202  |
| Tenderloin                      | 89202  |
| Twin Peaks                      | 89089  |
| Marina                          | 88869  |
+---------------------------------+--------+
20 rows selected (18.719 seconds)
