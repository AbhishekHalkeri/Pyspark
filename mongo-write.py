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

    lines=spark.sparkContext.textFile('hdfs:///user/maria_dev/mongodb/movies.user')
    users=lines.map(parseInput)
    userDataset=spark.createDataFrame(users)
    userDataset.write\
    .format('com.mongodb.spark.sql.DefaultSource')\
    .option('url','mongodb://192.168.1.28:27017/moviedata.users')\
    .mode('append')\
    .save()

