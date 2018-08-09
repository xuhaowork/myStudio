package com.zzjz.deepinsight.core.TestJarWithSpark


import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Utils2 extends Serializable {
  val split: UserDefinedFunction = udf((_: String) => {
    "this is a test udf"
  })

}
