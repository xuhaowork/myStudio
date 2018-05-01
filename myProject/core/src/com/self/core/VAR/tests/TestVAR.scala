package com.self.core.VAR.tests

import com.self.core.VAR.models.{VAR, VARModel}
import com.self.core.baseApp.myAPP

object TestVAR extends myAPP {
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

