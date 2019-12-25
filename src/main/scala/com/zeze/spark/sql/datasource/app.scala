package com.zeze.spark.sql.datasource

import com.zeze.spark.sql.datasource.core.SparkSessionManger
import com.zeze.spark.sql.datasource.custom.CustomSQL
import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{Encoders, Row}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by: chenzz on 2019/12/24, 10:41.
 */
object app extends App {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  logger.info("Application started...")

  val conf = new SparkConf().setAppName("spark-custom-datasource")
  SparkSessionManger.buid(conf)

  //select some specific columns
  val sqlDF = CustomSQL.sql("select * from people where salary >= 50000")
  logger.info(s"{}", sqlDF.show())

  val resultDf = sqlDF.map(new MapFunction[Row, String] {
    override def call(row: Row): String = row.mkString("\t")
  }, Encoders.STRING).toDF("value")
  //save the data to path
  CustomSQL.export(resultDf, "out_put/")
  logger.info("Application Ended...")
}
