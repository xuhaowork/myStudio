package com.self.core.VAR.VAR2.tests

import java.text.SimpleDateFormat

import com.zzjz.deepinsight.basic.BaseMain
import com.zzjz.deepinsight.core.TimeSeries.VAR.models.{VAR, VARModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

object TestVAR extends BaseMain {
  override def run(): Unit = {
    val s = TestTimeWrapping.result


    /** 最大滞后阶数 */
    val maxLag: Int = util.Try("2".toInt) getOrElse 1
    val predictStep: Int = util.Try("3".toInt) getOrElse 1
    val varTrain = new VAR(maxLag)
    s.mapValues(bm => {
      val varModel: VARModel = varTrain.run(bm)
      varModel.predict(predictStep)
    }).foreach(println)




  }


}

