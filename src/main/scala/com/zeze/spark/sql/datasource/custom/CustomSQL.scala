package com.zeze.spark.sql.datasource.custom

import com.zeze.spark.sql.datasource.core.SparkSessionManger
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by: chenzz on 2019/12/25, 9:25.
 */
object CustomSQL {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def sql(sqlText: String): Dataset[Row] = {
    val tableName = paraserCollectorType(sqlText)
    SparkSessionManger.checkAndLocalTable(tableName)
    val sparkSession = SparkSessionManger.getOrCreate()
    sparkSession.sqlContext.tableNames().foreach((str: String) => println("sqlContext TableView: " + str))
    sparkSession.sql(sqlText)
  }

  def export(dataset: Dataset[Row], path: String): Unit = {
    dataset.write.format("text")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  def paraserCollectorType(sql: String): String = {
    if (!sql.contains(" from ")) {
      null
    } else {
      val str = sql.substring(sql.indexOf(" from ") + 5).trim
      str.split(" ")(0)
    }
  }

}
