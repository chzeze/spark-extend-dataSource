package com.zeze.spark.sql.datasource.custom

import com.zeze.spark.sql.datasource.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Created by: chenzz on 2019/12/24, 16:05.
 */
class CustomDatasourceRelation(
                                override val sqlContext: SQLContext,
                                path: String)
  extends BaseRelation with PrunedFilteredScan with Serializable {

  override def schema: StructType = {
    val s = StructType(
      StructField("id", LongType, false) ::
        StructField("name", StringType, true) ::
        StructField("gender", StringType, true) ::
        StructField("salary", LongType, true) ::
        StructField("expenses", LongType, true) :: Nil
    )
    println(s.treeString)
    s
  }

  private def createBaseRdd(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    var customFilters: Map[String, List[CustomFilter]] = Map[String, List[CustomFilter]]()
    filters.foreach {
      case EqualTo(attr, value) =>
        println("EqualTo filter is used!!" + "Attribute: " + attr + " Value: " + value)

        /**
         * as we are implementing only one filter for now, you can think that this below line doesn't make much sense
         * because any attribute can be equal to one value at a time. so what's the purpose of storing the same filter
         * again if there are.
         * but it will be useful when we have more than one filter on the same attribute. Take the below condition
         * for example:
         * attr > 5 && attr < 10
         * so for such cases, it's better to keep a list.
         * you can add some more filters in this code and try them. Here, we are implementing only equalTo filter
         * for understanding of this concept.
         */
        customFilters = customFilters ++ Map(attr -> {
          customFilters.getOrElse(attr, List[CustomFilter]()) :+ new CustomFilter(attr, value, "equalTo")
        })
      case f => println("filter: " + f.toString + " is not implemented by us!!")
    }

    val schemaFields = schema.fields
    // Reading the file's content
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f => f._2)

    val rows = rdd.map(file => {
      val lines = file.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)

      val filteredData = data.map(s => if (customFilters.nonEmpty) {
        var includeInResultSet = true
        s.zipWithIndex.foreach {
          case (value, index) =>
            val attr = schemaFields(index).name
            val filtersList = customFilters.getOrElse(attr, List())
            if (filtersList.nonEmpty) {
              if (CustomFilter.applyFilters(filtersList, value, schema)) {
              } else {
                includeInResultSet = false
              }
            }
        }
        if (includeInResultSet) s else Seq()
      } else s)

      val tmp = filteredData.filter(_.nonEmpty).map(s => s.zipWithIndex.map {
        case (value, index) =>
          val colName = schemaFields(index).name
          val castedValue = Util.castTo(if (colName.equalsIgnoreCase("gender")) {
            if (value.toInt == 1) "Male" else "Female"
          } else value,
            schemaFields(index).dataType)
          if (requiredColumns.contains(colName)) Some(castedValue) else None
      })

      tmp.map(s => Row.fromSeq(s.filter(_.isDefined).map(value => value.get)))
    })

    rows.flatMap(e => e)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    println("PrunedFilterScan: buildScan called...")
    println("requiredColumns length:" + requiredColumns.length)
    requiredColumns.foreach((r: String) => println("requiredColumns: " + r))
    println("Filters: ")
    filters.foreach(f => println(f.toString))
    createBaseRdd(requiredColumns, filters)
  }
}
