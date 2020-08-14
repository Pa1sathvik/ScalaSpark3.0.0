package org.spark3.performance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{ col, from_json }
import org.apache.spark.sql.streaming.Trigger

object K2K {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder()
      .appName(args(0)).master(args(1)).getOrCreate()

    import sparkSession.implicits._
    val sqlContext = sparkSession.sqlContext
    val sparkContext = sparkSession.sparkContext
    //sparkContext.setLogLevel("OFF")
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "<Set Default File system URL of hadoop cluster>")

    val readStreamDFInd = sqlContext.readStream.format("kafka").option("kafka.bootstrap.servers", args(2))
      .option("subscribe", args(3)).option("failOnDataLoss", "false")
      .option("fetchOffset.retryIntervalMs", "180000")
      .option("auto.offset.reset", "Latest")
      .option("readFromTimestamp", "0")
      .option("refresh.leader.backoff.ms", "5000")
      .option("startingOffsets", "Latest")
      .load()

    val schema = new StructType()
      .add("l_orderkey", IntegerType, false)
      .add("l_partkey", IntegerType, false)
      .add("l_suppkey", IntegerType, false)
      .add("l_linenumber", IntegerType, false)
      .add("l_quantity", IntegerType, false)
      .add("l_extendedprice", DoubleType, false)
      .add("l_discount", DoubleType, false)
      .add("l_tax", DoubleType, false)
      .add("l_returnflag", StringType, false)
      .add("l_linestatus", StringType, false)
      .add("l_shipdate", StringType, false)
      .add("l_commitdate", StringType, false)
      .add("l_receiptdate", StringType, false)
      .add("l_shipinstruct", StringType, false)
      .add("l_shipmode", StringType, false)
      .add("l_comment", StringType, false)

    val stocksIndia = readStreamDFInd.selectExpr("CAST(value as STRING) as json").select(from_json($"json", schema).as("data")).select("data.*")

    stocksIndia.selectExpr("to_json(struct(*)) AS value").writeStream.format("Kafka")
      .option("kafka.bootstrap.servers", args(2))
      .option("topic", args(4)).trigger(Trigger.ProcessingTime(60000L))
      .option("checkpointLocation", "/tmp/Sathvik").queryName(args(4))
      .start()

    sqlContext.streams.awaitAnyTermination()
    
    /*
     * Spark Submit command to run the Kafka -> Kafka with specified schema. Make a JAR package with this class K2K.jar.
     * Place the JAR in the Hadoop or any cluster which run spark 3.0.0 or other version.
     * Below is the command used to run spark job. You can check the status of spark in <host>:4040 url.
     * 
     * spark.sql.shuffle.partitions count should be equal to Source , target Topic commands for better performance.
     * 
     * Command is ---> "spark-submit --conf spark.sql.shuffle.partitions=18 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview2 --files /etc/hive/conf/hive-site.xml --class org.spark3.performance.K2K --master yarn --deploy-mode cluster --num-executors 6 --executor-memory 16g --executor-cores 3  K2K.jar POC yarn localhost:9092 MS MT1" 
     *   
     */
    
    
  }
}