package date_8_2_2022

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Parallelize {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local")
      .appName("RDD_Parallelize")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val my_rdd: RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5,6))
    val rddCollect: Array[Int] = my_rdd.collect()
    println("Number of Partitions: " + my_rdd.getNumPartitions)
    println("First element: " + my_rdd.first())
    println("RDD converted to Array[Int] : ")
    rddCollect.foreach(println)
  }
}