package com.zeze.spark.sql.datasource

import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}

/**
 * Created by: chenzz on 2019/12/24, 16:06.
 */
object Util {
  def castTo(value: String, dataType: DataType) = {
    dataType match {
      case _: IntegerType => value.toInt
      case _: LongType => value.toLong
      case _: StringType => value
    }
  }
}
