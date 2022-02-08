package date_8_2_2022

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.math.Ordered.orderingToOrdered

object RDDTransformation {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local")
      .appName("RDD_Transformation")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rdd1 = spark.sparkContext.textFile("/home/sterlite/Downloads/sample-text-file.txt")

    //FlatMap method use to flat data present in text file
    val rdd2 = rdd1.flatMap(f=>f.split(" "))
    rdd2.foreach(println)

    //map all words with 1
    //val rdd3:RDD[(String,Int)]= rdd2.map(m=>(m,1))
    val rdd3 = rdd2.map(m=>(m,1))
    rdd3.foreach(println)


  }
}
