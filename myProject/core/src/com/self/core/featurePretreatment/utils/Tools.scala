package com.self.core.featurePretreatment.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.mllib.linalg.VectorUDT

object Tools {
  def checkColumnExists(colName: String, data: DataFrame): Boolean = {
    val schema = data.schema
    if(schema.fieldNames contains colName)
      true
    else
      throw new IllegalArgumentException(s"您输入的列名${colName}不存在")
  }

  def checkColumnTypes(colName: String, data: DataFrame, types: Array[DataType]): Boolean = {
    val dataType = data.schema(colName).dataType
    types contains dataType
  }

}
