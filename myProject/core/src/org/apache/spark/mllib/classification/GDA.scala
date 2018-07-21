package org.apache.spark.mllib.classification

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by datasure on 2018-2-28.
  */
class GDA extends Serializable with {
  def fit(input: RDD[LabeledPoint]) = {
    if (input.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }




  }



}
