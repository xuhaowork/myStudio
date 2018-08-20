package com.self.core.learningStreaming

import com.self.core.baseApp.myAPP

object LearningStreaming extends myAPP{
  override def run(): Unit = {
    println("good")

    /**  通过文件创建DStream，要求文件的时间戳要晚于每次执行时的时间戳 */
    val lines = smc.textFileStream("F://myStudio/data/streaming/")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x=> (x, 1)).reduceByKey(_ + _)

    wordCounts.print()

    smc.start()

    smc.awaitTermination()


    smc.socketTextStream("192.168.21.11", 9999)


  }
}
