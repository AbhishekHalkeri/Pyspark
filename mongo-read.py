from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def parseInput(line):
    fields=line.split('|')
    return Row(user_id=int(fields[0]),age=int(fields[1]),gender=fields[2],occupation=fields[3],zip=fields[4])
if __name__=='__main__':
    spark=SparkSession.builder\
    .appName('MongoDBIntegration')\
    .getOrCreate()

    readUsers=spark.read\
    .format('com.mongodb.spark.sql.Defaultsource')\
    .option('url','mongodb://192.168.1.28:27017/moviedata.user')\
    .load()

    readUsers.createOrReplaceTempView('users')
    readUsers.printSchema()

    sqlDF=spark.sql("""
    SELECT occupation,count(user_id) as cnt_usr
    FROM users
    GROUP BY occupation
    ORDER BY cnt_usr DESC
    """)
    sqlDF.show()
