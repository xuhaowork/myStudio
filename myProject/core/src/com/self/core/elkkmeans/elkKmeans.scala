package com.self.core.elkkmeans

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary


/**
  * Created by DataShoe on 2018/1/26.
  */
object elkKmeans extends myAPP{
  override def run(): Unit = {
//    import org.apache.spark.clustering.mllib.ElkKmeans
//    ElkKmeans.printk()
    import org.apache.spark.mllib.linalg.{Vector, Vectors}
    val vs = Vectors.sparse(10, Array(0, 1, 2, 9), Array(9, 5, 2, 7))
    println(vs(2))

    import org.apache.spark.mllib.stat.Statistics













  }
}
