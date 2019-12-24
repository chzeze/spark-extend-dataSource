package com.zeze.spark.sql.datasource

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Created by: chenzz on 2019/12/24, 10:41.
 */
object app extends App {
  println("Application started...")

  val conf = new SparkConf().setAppName("spark-custom-datasource")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

  val df = spark.sqlContext.read.format("com.zeze.spark.sql.datasource").load("data/")

  //  print the schema
  df.printSchema()

  //print the data
  df.show()

  //save the data
  df.write.options(Map("format" -> "customFormat")).mode(SaveMode.Overwrite).format("com.zeze.spark.sql.datasource").save("out_custom/")
  df.write.options(Map("format" -> "json")).mode(SaveMode.Overwrite).format("com.zeze.spark.sql.datasource").save("out_json/")
  df.write.mode(SaveMode.Overwrite).format("com.zeze.spark.sql.datasource").save("out_none/")

  df.createOrReplaceTempView("test")

  //select some specific columns
  //    spark.sql("select id, name, gender, salary from test where salary = 50000").show()
  spark.sql("select * from test").show()

  //filter data
  //  spark.sql("select id, name, salary from test where salary = 50000").show()
  //  spark.sql("select * from test where salary = 50000").show()

  println("Application Ended...")
}
