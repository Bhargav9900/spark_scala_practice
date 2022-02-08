package date_7_2_2022

import org.apache.spark.sql.SparkSession

object SparkSessionDemo {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName(name = "RDD DEMO")
      .master(master = "local")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:////home/sterlite/Spark/spark-events")
      .config("spark.history.fs.logDirectory", "file:////home/sterlite/Spark/spark-events")
      .getOrCreate()

    val doc = sparkSession.read.textFile("/home/sterlite/Desktop/new 1.txt")
    println(doc.show())


  }
}
