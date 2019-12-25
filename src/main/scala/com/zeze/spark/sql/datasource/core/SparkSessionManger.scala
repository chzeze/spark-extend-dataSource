package com.zeze.spark.sql.datasource.core

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by: chenzz on 2019/12/24, 18:03.
 */
object SparkSessionManger {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val lock: AnyRef = new Object()
  private var _current: SparkSession = null
  private var sparkConf: SparkConf = null

  def buid(sparkConf: SparkConf): Unit = {
    this.sparkConf = sparkConf
    initSparkSession
    val tableLoader: Thread = new Thread(new Runnable {
      override def run(): Unit = {
        loadTables("people")
      }
    })
    tableLoader.start()
  }

  def getOrCreate(): SparkSession = {
    if (_current == null || _current.sparkContext == null || _current.sparkContext.isStopped) {
      lock.synchronized {
        if (_current.sparkContext.isStopped) {
          if (_current != null) {
            _current.close()
          }
          _current = null
          initSparkSession
        }
      }
    }
    _current
  }

  private def initSparkSession = {
    _current = SparkSession.builder().config(sparkConf).master("local").getOrCreate()
    _current.udf.register("longToString", (time: Long, patten: String) => datetoString(new Date(time), patten))
  }

  def checkAndLocalTable(tableName: String) = {
    val sparkSession: SparkSession = getOrCreate()
    val sqlContext: SQLContext = sparkSession.sqlContext
    if (!sqlContext.tableNames().contains(tableName)) {
      loadTables(tableName)
    }
  }

  def loadTables(tableName: String): Unit = {
    val sparkSession: SparkSession = getOrCreate()
    val sqlContext: SQLContext = sparkSession.sqlContext
    val df = sqlContext.read
      .format("com.zeze.spark.sql.datasource.custom")
      .option("path", "data/")
      .load()
    df.createOrReplaceTempView(tableName)
    println("load table[" + tableName + "] to sqlContext")
  }

  def datetoString(time: Date, patten: String): String = {
    new SimpleDateFormat(patten, Locale.CHINA).format(time);
  }
}
