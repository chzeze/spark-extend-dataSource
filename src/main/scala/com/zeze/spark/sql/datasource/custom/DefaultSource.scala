package com.zeze.spark.sql.datasource.custom

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

/**
 * Created by: chenzz on 2019/12/24, 10:01.
 */
class DefaultSource extends RelationProvider with DataSourceRegister {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path", "./date/")
    new CustomDatasourceRelation(sqlContext, path)
  }

  override def shortName(): String = "custom"
}
