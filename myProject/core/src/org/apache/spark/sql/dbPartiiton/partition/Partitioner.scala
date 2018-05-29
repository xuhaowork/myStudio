//package org.apache.spark.sql.dbPartiiton.partition
//
//import java.sql.ResultSet
//import java.text.SimpleDateFormat
//
//import de.lmu.ifi.dbs.elki.math.random.XorShift64NonThreadsafeRandom
//
//import scala.util.Random
//
//class Partitioner {
//  //此方法没有用到，可不必理会。
//  def  longPartition(lowerBound:Long,upperBound:Long,partiNum:Int):Array[Long]={
//
//    val bounds=new Array[Long](partiNum+1)
//    bounds(0)=lowerBound
//    bounds(partiNum)=upperBound
//    if(partiNum>1) {
//      for (i <- 1 to partiNum - 1) {
//        bounds(i) =lowerBound+(upperBound-lowerBound+1)*i/partiNum
//      }
//    }
//    bounds
//  }
//
//  // 此方法没有用到，可不必理会
//  def  longPartiInvesion(lowerBound:Long,upperBound:Long,partiNum:Int):Array[Long]={
//     val   bounds=new  Array[Long](partiNum+1)
//      bounds(0)=upperBound
//      bounds(partiNum)=lowerBound
//      if (partiNum>1){
//        for (i <- 1 to partiNum - 1) {
//          bounds(i) =upperBound-(upperBound-lowerBound+1)*i/partiNum
//        }
//      }
//    bounds
//  }
//
//  // 此方法没有用到，可不必理会
// def  timePartiton(lowerBound:String,upperBound:String,partiNum:Int):Array[String]={
//
//   val  sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//
//   val  beginTime=sdf.parse(lowerBound)
//   val  endTime=sdf.parse(upperBound)
//   val  diff=(endTime.getTime-beginTime.getTime)
//
//   var  bounds=new Array[String](partiNum+1)
//   bounds(0)="'"+new java.sql.Timestamp(beginTime.getTime)+"'"
//   bounds(partiNum)="'"+new java.sql.Timestamp(endTime.getTime)+"'"
//
//   if(partiNum>1) {
//     for (i <- 1 until partiNum) {
//       bounds(i) = "'"+new java.sql.Timestamp(beginTime.getTime + i * diff / partiNum)+"'"
//     }
//   }
//   bounds
//
//}
//
//
//  def reservoirSampleLong(
//                               input: ResultSet,
//                               k: Int,
//                               seed: Long = Random.nextLong())
//  : Array[Long]= {
//    val reservoir = new Array[Long](k)
//    var i = 0
//    while (i < k && input.next()) {
//      val item =input.getLong(1)
//      reservoir(i) = item
//      i += 1
//    }
//    if (i < k) {
//      val trimReservoir = new Array[Long](i)
//      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
//      trimReservoir.sorted
//    } else {
//      val rand = new XorShift64NonThreadsafeRandom(seed)
//
//      while (input.next()) {
//        val item =input.getLong(1)
//        val replacementIndex = rand.nextInt(i)
//        if (replacementIndex < k) {
//          reservoir(replacementIndex)=item
//        }
//        i += 1
//      }
//      reservoir.sorted
//
//    }
//  }
//
//
//  def reservoirSampleDouble(
//                           input: ResultSet,
//                           k: Int,
//                           seed: Long = Random.nextLong())
//  : Array[Double]= {
//    val reservoir = new Array[Double](k)
//    var i = 0
//    while (i < k && input.next()) {
//      val item =input.getDouble(1)
//      reservoir(i) = item
//      i += 1
//    }
//    if (i < k) {
//      val trimReservoir = new Array[Double](i)
//      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
//      trimReservoir.sorted
//    } else {
//      val rand = new XorShift64NonThreadsafeRandom(seed)
//
//      while (input.next()) {
//        val item =input.getDouble(1)
//        val replacementIndex = rand.nextInt(i)
//        if (replacementIndex < k) {
//          reservoir(replacementIndex)=item
//        }
//        i += 1
//      }
//      reservoir.sorted
//    }
//  }
//
//
//
//
//  def reservoirSampleString(
//                               input: ResultSet,
//                               k: Int,
//                               seed: Long = Random.nextLong())
//  : Array[String]= {
//    val reservoir = new Array[String](k)
//    var i = 0
//    while (i < k && input.next()) {
//      val item =input.getString(1)
//      reservoir(i) = item
//      i += 1
//    }
//    if (i < k) {
//      val trimReservoir = new Array[String](i)
//      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
//      trimReservoir.sorted
//    } else {
//      val rand = new XorShift64NonThreadsafeRandom(seed)
//
//      while (input.next()) {
//        val item =input.getString(1)
//        val replacementIndex = rand.nextInt(i)
//        if (replacementIndex < k) {
//          reservoir(replacementIndex)=item
//        }
//        i += 1
//      }
//      reservoir.sorted
//    }
//  }
//  // 此方法没有用到，可不必理会
//  def sampleLongPartition(lowerBound:Long,upperBound:Long,sample:Array[Long],partiNum:Int) :Array[Long]={
//    val bounds = new Array[Long](partiNum + 1)
//    bounds(0) = lowerBound
//    bounds(partiNum) =upperBound
//    if (partiNum > 1) {
//      if (sample.length > 0) {
//        for (i <- 1 to partiNum - 1) {
//          val index = (sample.length) * i / partiNum
//          bounds(i) =sample(index)
//        }
//      }
//    }
//    bounds
//  }
//
//  // 此方法没有用到，可不必理会
//def sampleStringPartition(lowerBound:String,upperBound:String,sample:Array[String],partiNum:Int) :Array[String]={
//  val bounds = new Array[String](partiNum + 1)
//  bounds(0) = "'" + lowerBound + "'"
//  bounds(partiNum) = "'" + upperBound + "'"
//  if (partiNum > 1) {
//    if (sample.length > 0) {
//      for (i <- 1 to partiNum - 1) {
//        val index = (sample.length) * i / partiNum
//        bounds(i) ="'"+sample(index)+"'"
//      }
//    }
//  }
//  bounds
//}
//
// //  don't  sample
//  def  timeAvePart{}   // need  lower and   upper  reserve
//  def  longAvePart{}   // need  lower and   upper  reserve
//  def  doubleAvePart{} // need  lower and   upper  reserve
//
//
//
//  //  partition with  query  double  condition
//  def  stringPart(lowerBound:String,upperBound:String,sample:Array[String],partiNum:Int,indexColumn:String):Array[String]={
//    val bounds = new Array[String](partiNum)
//    val tem=new Array[String](partiNum-1)
//
//    val tem1 = lowerBound  //preliminary assignment, maybe  be  null
//    val tem2 = upperBound    // preliminary assignment, maybe  be  null
//
//    if (partiNum<=1){
//      if(tem1.length==0 && tem2.length>0){
//        bounds(0)=tem2
//      }else if(tem2.length==0 && tem1.length>0){
//        bounds(0)=tem1
//      }else if (tem2.length>0 && tem1.length>0){
//        bounds(0)=lowerBound+" and "+upperBound
//      }  else{
//        bounds(0)=""
//      }
//    }else{
//      if (sample.length > 0) {
//        for (i <- 1 to partiNum - 1) {
//          val index = (sample.length)*i/partiNum
//          tem(i-1) ="'"+sample(index)+"'"
//        }
//        if (tem1.length!=0) {
//          bounds(0) =tem1+" and "+indexColumn+"<"+tem(0)
//        }else{
//          bounds(0)=indexColumn+"<"+tem(0)
//        }
//        if (tem2.length!=0){
//          bounds(partiNum-1) =indexColumn+">="+tem(partiNum-2)+" and "+tem2
//        }else{
//          bounds(partiNum-1) =indexColumn+">="+tem(partiNum-2)
//        }
//        if(partiNum>2) {
//          for (i <- 1 until partiNum - 1) {
//            bounds(i) = indexColumn + ">=" + tem(i-1) + " and " + indexColumn + "<" + tem(i)
//          }
//        }
//      }
//    }
//    bounds
//  }
//
// //  partition with  query  long  condition
//  def  longPart(lowerBound:String,upperBound:String,sample:Array[Long],partiNum:Int,indexColumn:String):Array[String]={
//
//    val bounds = new Array[String](partiNum)
//    val tem=new Array[Long](partiNum-1)
//
//
//    val tem1 = lowerBound  //preliminary assignment, maybe  be  0
//    val tem2 = upperBound    // preliminary assignment, maybe  be  0
//
//    if (partiNum<=1){
//
//
//      if(tem1.length==0 && tem2.length>0){
//        bounds(0)=tem2
//      }else if(tem2.length==0 && tem1.length>0){
//        bounds(0)=tem1
//      }else if (tem2.length>0 && tem1.length>0){
//        bounds(0)=lowerBound+" and "+upperBound
//      }  else{
//        bounds(0)=""
//      }
//
//
//    }else{
//      if (sample.length > 0) {
//        for (i <- 1 to partiNum - 1) {
//          val index = (sample.length)*i/partiNum
//          tem(i-1) =sample(index)
//        }
//        if (tem1.length!=0) {
//          bounds(0) =tem1+" and "+indexColumn+"<"+tem(0)
//        }else{
//          bounds(0) =indexColumn+"<"+tem(0)
//        }
//        if (tem2.length!=0){
//          bounds(partiNum-1) =indexColumn+">="+tem(partiNum-2)+" and "+tem2
//        }else{
//          bounds(partiNum-1) =indexColumn+">="+tem(partiNum-2)
//        }
//        if(partiNum>2) {
//          for (i <- 1 to partiNum - 2) {
//            bounds(i) = indexColumn + ">=" + tem(i-1) + " and " + indexColumn + "<" + tem(i)
//          }
//        }
//      }
//    }
//    bounds
//  }
////  partition with  query  double  condition
//  def  doublePart(lowerBound:String,upperBound:String,sample:Array[Double],partiNum:Int,indexColumn:String):Array[String]={
//
//    val bounds = new Array[String](partiNum)
//    val tem=new Array[Double](partiNum-1)
//
//    val tem1 = lowerBound  //preliminary assignment, maybe  be  null
//    val tem2 = upperBound    // preliminary assignment, maybe  be null
//
//    if (partiNum<=1){
//
//
//      if(tem1.length==0 && tem2.length>0){
//        bounds(0)=tem2
//      }else if(tem2.length==0 && tem1.length>0){
//        bounds(0)=tem1
//      }else if (tem2.length>0 && tem1.length>0){
//        bounds(0)=lowerBound+" and "+upperBound
//      }  else{
//        bounds(0)=""
//      }
//
//
//    }else{
//
//      if (sample.length > 0) {
//        for (i <- 1 to partiNum - 1) {
//          val index = (sample.length)*i/partiNum
//          tem(i-1) =sample(index)
//        }
//        if (tem1.length > 0) {
//          bounds(0) =tem1+" and "+indexColumn+"<"+tem(0)
//        }else{
//          bounds(0) =indexColumn+"<"+tem(0)
//        }
//        if (tem2.length > 0){
//          bounds(partiNum-1) =indexColumn+">="+tem(partiNum-2)+" and "+tem2
//        }else{
//          bounds(partiNum-1) =indexColumn+">="+tem(partiNum-2)
//        }
//        if(partiNum>2) {
//          for (i <- 1 to partiNum - 2) {
//            bounds(i) = indexColumn + ">=" + tem(i-1) + " and " + indexColumn + "<" + tem(i)
//          }
//        }
//      }
//
//    }
//    bounds
//  }
//
//}
