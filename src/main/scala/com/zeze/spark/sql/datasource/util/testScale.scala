package com.zeze.spark.sql.datasource.util


import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by: chenzz on 2019/12/25, 16:33.
 */
object testScale {

  def main(args: Array[String]) {
    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("SQL-2").setMaster("local")
    //SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)
    //创建SQLContext
    val sqlContext = new SQLContext(sc)
    //从指定的地址创建RDD
    val personRDD = sc.textFile("data/input2.txt").map(_.split(","))
    //    val rdd = sqlContext.sparkContext.wholeTextFiles("data/input2.txt").map(f => f._2)

    //通过StructType直接指定每个字段的schema
    val schema = StructType(
      StructField("id", LongType, false) ::
        StructField("name", StringType, true) ::
        StructField("gender", StringType, true) ::
        StructField("salary", LongType, true) ::
        StructField("expense", LongType, true) :: Nil
    )

    //允许字段为空
    val rdd = personRDD.map(row =>
      row.toSeq.map(r => {
        if (r.trim.length > 0) {
          val castValue = Util.castTo(r.trim, schema.fields(row.toSeq.indexOf(r)).dataType)
          castValue
        }
        else null
      })).map(Row.fromSeq)

    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p => Row(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim.toLong))

    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rdd, schema)
    //注册表
    personDataFrame.registerTempTable("t_person")
    //执行SQL
    val df = sqlContext.sql("select * from t_person")
    //将结果以JSON的方式存储到指定位置
    df.write.mode(SaveMode.Overwrite).json("out_put/")
    //停止Spark Context
    sc.stop()
  }
}