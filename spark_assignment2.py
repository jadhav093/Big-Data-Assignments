from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .appName('assignment-2')\
        .config("spark.jars", "mysql-connector-java-8.0.13.jar")\
        .config('spark.driver.memory','6g')\
        .config('spark.sql.legacy.timeParserPolicy', 'LEGACY')\
        .getOrCreate()
spark.sparkContext.setCheckpointDir('/tmp/checkpoints')


def question1():
        filepath = '/home/sarang/sunbeam_material/ncdc'
        ncdc_staging = spark.read.text(filepath)

        # ncdc_staging.show(truncate=False)
        regex = r'^.{15}([0-9]{4}).{68}([-\+][0-9]{4})([0-9]).*$'

        ncdc = ncdc_staging.select(regexp_extract('value', regex, 1).cast('int').alias('yr'),
                                   regexp_extract('value', regex, 2).cast('int').alias('temp'),
                                   regexp_extract('value', regex, 3).cast('int').alias('quality')) \
                .where('temp!= 9999 and quality in (0,1,2,4,5,9)')

        dbUrl = 'jdbc:mysql://localhost:3306/classwork_db'
        dbDriver = 'com.mysql.cj.jdbc.Driver'
        dbUser = 'root'
        dbPass = 'manager'
        dbTable = 'ncdc'
        ncdc.write \
                .option('url', dbUrl) \
                .option('driver', dbDriver) \
                .option('user', dbUser) \
                .option('password', dbPass) \
                .option('dbtable', dbTable) \
                .mode('overwrite') \
                .format('jdbc') \
                .save()


# question1()


def question2():
        from_mysql = spark.read \
                .format("jdbc") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("url", "jdbc:mysql://localhost:3306/classwork_db") \
                .option("dbtable", "ncdc") \
                .option("user", "root") \
                .option("password", "manager") \
                .load()

        # from_mysql.show()

        avg_temp = from_mysql.groupBy('yr').avg('temp') \
                .withColumnRenamed('avg(temp)', 'avgtemp') \
                .orderBy(desc('avgtemp'))

        avg_temp.show()


# question2()


def question3():
        filepath = '/home/sarang/sunbeam_material/movies/ratings.csv'

        schema = 'userId INT, movieId INT, rating DOUBLE, timestamp LONG'
        ratings = spark.read.schema(schema).option('header', 'true').csv(filepath)

        ratings.createOrReplaceTempView('v_ratings')

        result = spark.sql(
                'select month(from_unixtime(timestamp)) as month, count(userId) from v_ratings group by month')

        result.show()


# question3()


def question4():
        filepath = '/home/sarang/sunbeam_material/movies/ratings.csv'
        ratings = spark.read.option('inferSchema', 'true').option('header', 'true').csv(filepath)

        filepath2 = '/home/sarang/sunbeam_material/movies/movies.csv'
        movies = spark.read.option('inferSchema', 'true').option('header', 'true').csv(filepath2)

        ratings.createOrReplaceTempView('v_ratings')
        movies.createOrReplaceTempView('v_movies')

        recommend = spark.sql(
                'select t1.userId, t1.movieId as m1, t1.rating as r1, t2.movieId as m2, t2.rating as r2 from v_ratings t1 inner join v_ratings t2 ON t1.userId = t2.userId AND t1.movieId < t2.movieId WHERE t1.movieId IN (SELECT movieId FROM v_ratings) AND t2.movieId IN (SELECT movieId FROM v_ratings)').cache()
        recommend.createOrReplaceTempView('v_corrtable')

        corr_table = spark.sql('select m1, m2, corr(r1, r2) cor from v_corrtable group by m1,m2 having cor>0.8').cache()
        corr_table.createOrReplaceTempView('v_recommend')

        movieid1 = int(input("Enter movieId: "))
        recommend_final = spark.sql(
                f'select m1.title as movie1, m2.title as movie2, t1.cor from v_recommend t1 inner join v_movies m1 on m1.movieId={movieid1} inner join v_movies m2 on m2.movieId=t1.m2')
        print("Recommendations for this movie are: ")
        recommend_final.show()


# question4()


filepath = '/home/sarang/sunbeam_material/BigData/data/Fire_Service_Calls_Sample.csv'
schema = """
call_number BIGINT, unit_id STRING, incident_number BIGINT, call_type STRING, call_date string, watch_date string, received_dttm string, entry_dttm string, dispatch_dttm string, response_dttm string, onscene_dttm string, transport_dttm string, hospital_dttm string, final_disp string, available_dttm string, address string, city string, zipcode_inci BIGINT, battalion string, station_area int, box int, orig_prio string, prio string, final_prio int, als_unit string, call_type_grp string, number_of_alarms int, unit_type string, seq_call_disp int, fire_prev_dist int, supervisor_dist int, neighbor string, row_id string, case_loc string, data_as_of string, data_loaded_at string, analysis_neighbor int
"""
fire_calls_staging = spark.read.option('header', 'true').schema(schema).csv(filepath).cache()
fire_calls_staging.createOrReplaceTempView('v_fire_staging')

fire_calls = spark.sql("""
               SELECT 
        call_number, 
        unit_id, 
        incident_number, 
        call_type, 
        to_date(from_unixtime(unix_timestamp(call_date, 'MM/dd/yy'))) AS call_date,
        to_date(from_unixtime(unix_timestamp(watch_date, 'MM/dd/yy'))) AS watch_date,
        from_unixtime(unix_timestamp(received_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS received_dttm,
        from_unixtime(unix_timestamp(entry_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS entry_dttm,
        from_unixtime(unix_timestamp(dispatch_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS dispatch_dttm,
        from_unixtime(unix_timestamp(response_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS response_dttm,
        from_unixtime(unix_timestamp(onscene_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS onscene_dttm,
        from_unixtime(unix_timestamp(transport_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS transport_dttm,
        from_unixtime(unix_timestamp(hospital_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS hospital_dttm,
        final_disp, 
        from_unixtime(unix_timestamp(available_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS available_dttm,
        address, 
        city, 
        cast(zipcode_inci as INT) AS zipcode_inci, 
        battalion, 
        cast(station_area as INT) AS station_area,
        cast(box as INT) AS box, 
        orig_prio, 
        prio, 
        final_prio, 
        als_unit, 
        call_type_grp, 
        cast(number_of_alarms as INT) AS number_of_alarms,
        unit_type, 
        cast(seq_call_disp as INT) AS seq_call_disp, 
        cast(fire_prev_dist as INT) AS fire_prev_dist, 
        cast(supervisor_dist as INT) AS supervisor_dist,
        neighbor, 
        row_id, 
        case_loc, 
        data_as_of, 
        from_unixtime(unix_timestamp(data_loaded_at, 'MM/dd/yyyy hh:mm:ss a')) AS data_loaded_at,
        cast(analysis_neighbor as INT) AS analysis_neighbor 
    FROM v_fire_staging
                        """).cache()

q1 = fire_calls.select(count_distinct('call_type'))
# q1.show()

q2 = fire_calls.select('call_type').distinct()
# q2.show()

q3_1 = fire_calls.selectExpr('unix_timestamp(response_dttm)-unix_timestamp(received_dttm)')\
        .withColumnRenamed('(unix_timestamp(response_dttm, yyyy-MM-dd HH:mm:ss) - unix_timestamp(received_dttm, yyyy-MM-dd HH:mm:ss))', 'delay')
q3 = q3_1.select('delay').where('delay > 300').agg(count('delay'))
# q3.show()

q4 = fire_calls.selectExpr('call_type').groupBy('call_type').agg(count('call_type').alias('cnt')).orderBy(desc('cnt'))
# q4.show()



q5_1 = fire_calls.select('zipcode_inci', 'call_type').groupBy('zipcode_inci', 'call_type').agg(count('call_type').alias('cnt')).orderBy(desc('cnt')).cache()
q5_2 = q5_1.groupBy('call_type').agg(max('cnt').alias('max_cnt')).orderBy(desc('max_cnt')).cache()
q5_1_alias = q5_1.alias("q5_1")
q5_2_alias = q5_2.alias("q5_2")
q5 = q5_1_alias.join(
    q5_2_alias,
    (q5_1_alias.call_type == q5_2_alias.call_type) & (q5_1_alias.cnt == q5_2_alias.max_cnt),
    'inner'
).select(
    q5_1_alias.zipcode_inci,
    q5_1_alias.call_type,
    q5_1_alias.cnt
)
# q5.show()

q6 = fire_calls.select('neighbor').where('city = "San Francisco" and zipcode_inci in (94102, 94103)').distinct()
# q6.show()

q7 = fire_calls.agg(sum(unix_timestamp('response_dttm')-unix_timestamp('received_dttm')).alias('sum_time'),
                    avg(unix_timestamp('response_dttm')-unix_timestamp('received_dttm')).alias('avg_time'),
                    min(unix_timestamp('response_dttm')-unix_timestamp('received_dttm')).alias('min_time'),
                    max(unix_timestamp('response_dttm')-unix_timestamp('received_dttm')).alias('max_time')
                    )
# q7.show()

q8 = fire_calls.select(count_distinct(year('call_date')))
# q8.show()

q9 = fire_calls.select(weekofyear('call_date').alias('weeks')).where('year(call_date)=2018 and call_type like "%Fire%"').groupBy('weeks').agg(count('weeks').alias('cnt')).orderBy(desc('cnt')).limit(1)
# q9.show()

q10 = fire_calls.select('neighbor', (unix_timestamp('response_dttm')-unix_timestamp('received_dttm')).alias('delay')).where('city = "San Francisco" and year(call_date)=2018').groupBy('neighbor').agg(avg('delay').alias('delay')).orderBy(desc('delay'))
# q10.show()

spark.stop()