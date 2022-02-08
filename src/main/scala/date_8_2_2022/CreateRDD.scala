package date_8_2_2022

import org.apache.spark.sql.SparkSession

object CreateRDD {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local")
      .appName("RDD_Creation")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //create rdd using sparkContext
    val rdd1=spark.sparkContext.parallelize(Seq(("Neer", 300),("Bhargav", 169), ("Vasu", 234)))
    rdd1.foreach(println)

    println()
    //rdd using txt file
    val rdd2 = spark.sparkContext.textFile("/home/sterlite/Downloads/sample-text-file.txt")
    rdd2.foreach(println)

    //rdd with read all text
    val rdd3 = spark.sparkContext.wholeTextFiles("/home/sterlite/Downloads/sample-text-file.txt")
    rdd3.foreach(println)

    println()
    //Creating from another RDD

    val rdd4 = rdd1.map(row=>{(row._1 + " patel",row._2+100)})
    rdd4.foreach(println)

    //From existing DataFrames and DataSet
    println()

    val rdd5 = spark.range(20).toDF().rdd
    rdd5.foreach(println)

  }

}
