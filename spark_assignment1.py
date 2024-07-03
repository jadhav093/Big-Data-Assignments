from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, DateType, TimestampType

spark = SparkSession.builder\
    .getOrCreate()

def question1():
    filepath = '/home/sarang/uwufetch/LICENSE'

    words = spark.read.text(filepath)

    op1 = words.select(explode(split(lower('value'), '[^a-z0-9]')).alias('words')) \
        .where(
        'words not in ("", "the", "of", "to", "a", "an", "or", "you", \
        "that", "and", "this", "for", "in", "not", "under", "any", "with", "by", \
        "is", "it", "as", "may", "on", "be", "from", "are", "other", "if", "your", \
        "source", "all", "use")') \
        .groupBy('words').count() \
        .orderBy(desc('count'))

    op1.show()
    spark.stop()
# question1()


def question2():
    filepath = '/home/sarang/sunbeam_material/BigData/data/emp.csv'
    emp_schema = StructType()\
            .add('empno', IntegerType(), True)\
            .add('ename', StringType(), True)\
            .add('job', StringType(), True)\
            .add('mgr', IntegerType(), True)\
            .add('hire', DateType(), True)\
            .add('sal', DoubleType(), True)\
            .add('comm', DoubleType(), True)\
            .add('deptno', IntegerType(), True)

    emp = spark.read.schema(emp_schema).csv(filepath)

    emp.show(truncate=False)

    result = emp.select('deptno', 'job', 'sal')\
            .groupBy('deptno', 'job').max('sal')\
            .orderBy('deptno')
    result.show()
    spark.stop()

# question2()

def question3():
    filepath = '/home/sarang/sunbeam_material/BigData/data/emp.csv'
    filepath2 = '/home/sarang/sunbeam_material/BigData/data/dept.csv'
    emp_schema = StructType() \
        .add('empno', IntegerType(), True) \
        .add('ename', StringType(), True) \
        .add('job', StringType(), True) \
        .add('mgr', IntegerType(), True) \
        .add('hire', DateType(), True) \
        .add('sal', DoubleType(), True) \
        .add('comm', DoubleType(), True) \
        .add('deptno', IntegerType(), True)

    dept_schema = StructType() \
        .add('deptno', IntegerType(), True) \
        .add('dname', StringType(), True) \
        .add('loc', StringType(), True)

    emp = spark.read.schema(emp_schema).csv(filepath)
    dept = spark.read.schema(dept_schema).csv(filepath2)

    result1 = emp.join(dept, [emp.deptno == dept.deptno], 'inner') \
        .select('dname', 'sal') \
        .groupBy('dname').sum('sal')

    result1.show()
    spark.stop()

# question3()


def question4():
    filepath = '/home/sarang/sunbeam_material/movies/ratings.csv'
    # rating_schema = StructType() \
    #         .add('userId', IntegerType(), True) \
    #         .add('movieID', StringType(), True) \
    #         .add('rating', StringType(), True)\
    #         .add('timestamp', TimestampType(), True)
    ratings = spark.read.option('header', 'true').option('inferSchema', 'true').csv(filepath)

    ratings.show()

    result = ratings.select(year(from_unixtime('timestamp')).alias('year'), 'rating').groupBy('year').count().orderBy(
        desc('count'))

    result.show()
    spark.stop()

# question4()


def question5():
    movie_path = '/home/sarang/sunbeam_material/movies/movies.csv'
    rating_path = '/home/sarang/sunbeam_material/movies/ratings.csv'

    movies = spark.read.option('header', 'true').option('inferSchema', 'true').csv(movie_path)
    ratings = spark.read.option('header', 'true').option('inferSchema', 'true').csv(rating_path)

    result1 = ratings.alias("a").join(ratings.alias("b"),
                                      (col('a.userId') == col('b.userId')) & (col('a.movieId') < col('b.movieId')),
                                      'inner')
    result2 = result1.select(col('a.movieId').alias('m1'), col('a.rating').alias('r1'), col('b.movieId').alias('m2'),
                             col('b.rating').alias('r2'))
    result3 = result2.groupBy('m1', 'm2').agg(corr('r1', 'r2').alias('correl')).filter(col('correl') > 0.7)

    result3.show()

    recommend = result3.join(movies.alias("t1"), result3.m1 == col('t1.movieId'), 'inner') \
        .select('m1', col('t1.title').alias('movie1'), 'm2', 'correl')

    # Join again with movies DataFrame to get the name for movieId2 (m2)
    recommend = recommend.join(movies.alias("t2"), recommend.m2 == col('t2.movieId'), 'inner') \
        .select('movie1', col('t2.title').alias('movie2'), 'correl')

    recommend.show()
    spark.stop()

# question5()