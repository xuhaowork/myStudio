package com.self.core.fastICA

import com.self.core.baseApp.myAPP

/**
  * Created by dell on 2018/2/22.
  */
object ICA extends myAPP{
  override def run(): Unit = {
    // 构造4种常见信号波形, 20000条数据
    var data = Array.range(0, 20000)

    // 第一种，正弦数据，每200条一个周期
    val data1 = data.map(d => {
      (d, 50*scala.math.sin(scala.math.Pi * 2* d / 200))})

    // 第二种，锯齿数据，每150一个周期
    val data2 = data.map(d => {
      (100 - d % 200)*0.5
    })

    // 第三种，城墙形状，每120一个周期
    val data3 = data.map(d => {
      val phase = d % 120
      if(scala.math.abs(phase - 60) <= 5){
        10 * (60 - phase)
      }else if(scala.math.abs(phase - 60) <= 55){
        50
      }else if((phase - 60) < -55 ){
        10 *phase
      }else{
        10*(phase - 120)
      }
    })

    // 第四种，每10个点组成一个随机游走.





















  }
}
