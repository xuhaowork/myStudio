package com.self.core.naFill

import com.self.core.baseApp.myAPP
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Vector => BV}


object NAFill extends myAPP{
  override def run(): Unit = {
    import com.google.gson.{Gson, JsonParser}
    import org.apache.spark.broadcast.Broadcast
    import org.apache.spark.rdd.RDD
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.{DataFrame, Row}
    import scala.util.Success
    import scala.collection.mutable.ArrayBuffer

    val jsonparam = "<#zzjzParam#>"
    //val jsonparam = "{\"inputTableName\":\"关系型数据库_3I92NUSE\",\"mode\":{\"modeList\":[{\"fieldName\":\"age\",\"modeType\":{\"selectList\":{\"selectList\":\"del3\",\"fieldName3\":\"\"}}}]},\"rddTableName\":\"table1855366341\"}"
    //{"inputTableName":"关系型数据库_2_eVUCgh","mode":[{"modeList":{"value":"del1","fieldName":[{"name":"loc1","datatype":"int"}]}},
    //{"modeList":{"value":"del2","fieldName":[{"name":"loc1","datatype":"int"}]}},
    //{"modeList":{"value":"del6","fieldName":[{"name":"loc2","datatype":"int"}],"setter":"5"}}]}

    println(jsonparam)

    val gson = new Gson()
    val p: java.util.Map[String,String] = gson.fromJson(jsonparam, classOf[java.util.Map[String,String]])
    val inputTableName = p.get("inputTableName")
    val rddTableName = "<#zzjzRddName#>"
    val jsonParser = new JsonParser()
    val jsonObj = jsonParser.parse(jsonparam).getAsJsonObject()
    var dfsc = memoryMap.get(inputTableName).asInstanceOf[org.apache.spark.sql.DataFrame]
    //var dfsc=GetData.getData()
    val colsArr=dfsc.dtypes.toMap
    val getType=(str:String,colsMap:Map[String, String])=>colsMap.get(str).get.replace("Type","")

    val isNumber=(str:String ,cols: Map[String, String])=>{
      //   if(!cols.contains(str))throw new Exception(str+"不是该数据中的字段，请检查")
      //   val ctype= getType(str,cols)
      //   if(ctype!="Integer"&&ctype!="Double"&&ctype!="Float"&&ctype!="Long"&&ctype!="Byte"&&ctype!="Short"){
      //     throw new Exception(str+"字段不是数字类型的，请输入数字类型的字段！")
      //   }
    }
    def stringToDouble(str:String,df:DataFrame):DataFrame={
      val cols=df.dtypes.toMap
      if(!cols.contains(str))throw new Exception(str+"不是该数据中的字段，请检查")
      val ctype= getType(str,cols)
      try{
        df.selectExpr(str).collect().map(row => {
          val a = row(0)
          if (a == " ") {
            val mess = str + "字段中数据" + a + "是空格不是空值"
            println(mess)
          }
          if(a != "null" && a!= null && !a.toString.isEmpty) a.toString.toDouble
        })
      } catch {
        case e: Exception => throw new Exception(str + "字段有无法转型为数值型的数据,注意：空格不是空值\r\n" + e.getMessage)
      }

      df.withColumn(str,df(str).cast(DoubleType))
    }


    val jsonArray = jsonObj.getAsJsonArray("mode")//处理列表{"value":"del1","fieldName":[{"name":"loc1","datatype":"int"}]}}]}
    val mode:java.util.Map[String,String]=gson.fromJson(jsonArray.get(0),classOf[java.util.Map[String,String]])
    val len = jsonArray.size()
    for(i<- 0 until len){
      val obj = jsonArray.get(i).getAsJsonObject().getAsJsonObject("modeList")
      val filledColunmName = obj.getAsJsonArray("fieldName").get(0).getAsJsonObject().get("name").getAsString()
      val mtype=obj.get("value").getAsString()
      dfsc=stringToDouble(filledColunmName,dfsc)

      mtype match {
        //均值填充
        case "del1"   =>{
          isNumber(filledColunmName,colsArr)
          val age: DataFrame =dfsc.describe(filledColunmName)
          var ar: Array[Row] = age.collect()
          val mean = ar(1)(1).toString.toDouble
          //        dfsc.filter(filledColunmName+" is null").select(filledColunmName).limit(20).show()
          dfsc = dfsc.na.fill(value=mean,cols=Array(filledColunmName))
          println(mean)
          dfsc.show()
          println(111)
        }

        //中值填充
        case "del2" => {
          isNumber(filledColunmName,colsArr)
          val rdd = dfsc.select(filledColunmName).na.drop().map(_.getAs(0).toString.toDouble)
          val sorted = rdd.sortBy(identity).zipWithIndex().map {
            case (v, idx) => (idx, v)
          }

          val count = sorted.count()
          val median = if (count % 2 == 0) {
            val l = count / 2 - 1
            val r = l + 1
            (sorted.lookup(l).head.toString.toDouble  + sorted.lookup(r).head.toString.toDouble) / 2
          } else sorted.lookup(count / 2).head.toString.toDouble
          println(median)
          dfsc = dfsc.na.fill(value=median,cols=Array(filledColunmName))
          //        getType(filledColunmName,colsArr) match {
          //          case "Integer"=>dfsc = dfsc.na.fill(value=median.toInt,cols=Array(filledColunmName))
          //          case "Double"=>dfsc = dfsc.na.fill(value=median,cols=Array(filledColunmName))
          //          case "Short"=>dfsc = dfsc.na.fill(value=median.toShort,cols=Array(filledColunmName))
          //          case "Long"=>dfsc = dfsc.na.fill(value=median.toLong,cols=Array(filledColunmName))
          //          case "Float"=>dfsc = dfsc.na.fill(value=median.toFloat,cols=Array(filledColunmName))
          //          case "Byte"=>dfsc = dfsc.na.fill(value=median.toByte,cols=Array(filledColunmName))
          //          case _=>throw new Exception("字段"+filledColunmName+"的类型不是数字类型！")
          //        }
          println(median)
          dfsc.show()
          println(789)
        }

        //众数填充
        case "del3"   =>{
          isNumber(filledColunmName,colsArr)

          val rdd = dfsc.select(filledColunmName).na.drop().map(_.getAs(0).toString.toDouble)

          val rdd3 = rdd.map(x=>(x,1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortBy(_._1,false).top(5).map(x=>(x._2,x._1))

          val modeValue = rdd3(0)._1

          println("Mode = "+ modeValue)
          dfsc.filter(filledColunmName+" is null").select(filledColunmName).limit(20).show()
          dfsc = dfsc.na.fill(value=modeValue,cols=Array(filledColunmName))
          dfsc.show()
        }

        //线性插值法填充
        case "del4"   =>{
          isNumber(filledColunmName,colsArr)

          val a: RDD[(Row, Long)] = dfsc.select(filledColunmName).rdd.zipWithIndex()
          val b = dfsc.drop(filledColunmName).rdd.zipWithIndex().map(x=>(x._2,x._1))

          val f = a.map(x=> x._1.get(0)).collect()
          var start:Int=0
          var end=0
          var flag=false
          for(i <- 0 until  f.length){
            if(flag==false&&i!=0&&f(i-1)!=null&&f(i)==null){
              start=i
              flag=true
            }
            if(flag==true&&(i+1)!=f.length&&f(i+1)!=null&&f(i)==null){
              end=i
              flag=false
            }
            if(start!=0&&end!=0&&flag==false){
              val tmp=(f(end+1).toString.toDouble-f(start-1).toString.toDouble)/(end-start+2)
              val startValue=f(start-1).toString.toDouble
              for(j<-start to end){
                f(j)=(startValue+(j-start+1)*tmp)
              }
              start=0
              end=0
              flag=false
            }
          }
          val tmpArr =for(i <- 0 until  f.length)yield (i,f(i))
          val rdd: RDD[(Long, Any)] =sc.parallelize(tmpArr).map(x=>(x._1.toLong,x._2))
          val rdd1: RDD[(Long, (Row, Any))] = a.map(x=>(x._2,x._1)).join(rdd)
          val rdd2=rdd1.map(x=>{
            val tmpArr = if(x._2._2 == null) null else x._2._2.toString.toDouble
            (x._1,Row(tmpArr))
          })
          val ab: RDD[(Long, (Row, Row))] = b.join(rdd2)
          val dataRDD: RDD[Row] = ab.map(p=>{
            Row.fromSeq(p._2._1.toSeq ++: p._2._2.toSeq)
          })
          val df = sqlc.createDataFrame(dataRDD,StructType(dfsc.drop(filledColunmName).schema.fields++:Array[StructField](StructField(filledColunmName,DoubleType))))
          dfsc.show()
          dfsc = df
        }

        //线性预测法填充
        case "del5"   =>{
          println("To Be Continued!")

        }

        //临近点均值填充
        case "del6"   =>{
          isNumber(filledColunmName,colsArr)
          val range = obj.get("setter").getAsString()
          range match {
            case "1" =>{
              val r3: RDD[(Int, Row)] = dfsc.rdd.zipWithIndex().map(x=>(x._2.toInt,x._1))
              val list3: Array[Row] =dfsc.rdd.collect()
              val list = sc.broadcast(list3)
              val cou = r3.count()

              val r4: RDD[(Int, Row)] = r3.map(x=>{
                if(x._2.getAs(filledColunmName)==null){
                  if(x._1==0||x._1==cou-1){
                    x
                  }
                  else{
                    val up: Row = list.value(x._1-1)
                    val down = list.value(x._1+1)
                    if(up.getAs(filledColunmName)==null||down.getAs(filledColunmName)==null){
                      x
                    }else{

                      val x0 = up(up.fieldIndex(filledColunmName)).toString.toDouble
                      val y0 = down(down.fieldIndex(filledColunmName)).toString.toDouble
                      val temp = x._2.toSeq.toArray
                      temp(x._2.fieldIndex(filledColunmName))=(x0+y0)/2
                      (x._1,Row.fromSeq(temp.toSeq))
                    }
                  }


                }else{
                  val temp = x._2.toSeq.toArray
                  temp(x._2.fieldIndex(filledColunmName))= temp(x._2.fieldIndex(filledColunmName)).toString.toDouble
                  (x._1,Row.fromSeq(temp.toSeq))
                }
              })
              val r5=r4.map(x=>x._2)
              val colArr=dfsc.schema.fields.map(x=>{
                if(x.name==filledColunmName)StructField(filledColunmName,DoubleType)
                else x
              })
              val df: DataFrame = sqlc.createDataFrame(r5,StructType(colArr))
              dfsc = df
              println(666666)

            }
            case "2" =>{
              //************
              val r3: RDD[(Int, Row)] = dfsc.rdd.zipWithIndex().map(x=>(x._2.toInt,x._1))
              val list3: Array[Row] =  dfsc.rdd.collect()
              val list: Broadcast[Array[Row]] = sc.broadcast(list3)
              val cou = r3.count()
              val r4 = r3.map(x => {
                if (x._2.getAs(filledColunmName)==null) {
                  if (x._1 == 0 || x._1 == 1 || x._1 == cou - 1 || x._1 == cou - 2) {
                    x
                  } else {
                    val up0 = list.value(x._1 - 2)
                    val up1 = list.value(x._1 - 1)
                    val down0 = list.value(x._1 + 2)
                    val down1 = list.value(x._1 + 1)
                    if (up0.getAs(filledColunmName) == null || up1.getAs(filledColunmName) == null || down0.getAs(filledColunmName) == null || down1.getAs(filledColunmName) == null) {
                      x
                    } else {
                      val x0 = up0.getAs(filledColunmName).toString.toDouble
                      val x1 = up1.getAs(filledColunmName).toString.toDouble
                      val y0 = down0.getAs(filledColunmName).toString.toDouble
                      val y1 = down1.getAs(filledColunmName).toString.toDouble

                      val temp = x._2.toSeq.toArray
                      //                    val i = x._2.fieldIndex(filledColunmName)
                      temp(x._2.fieldIndex(filledColunmName))=(x0+x1+y0+y1)/4
                      (x._1,Row.fromSeq(temp.toSeq))
                    }


                  }
                } else {
                  val temp = x._2.toSeq.toArray
                  temp(x._2.fieldIndex(filledColunmName))= temp(x._2.fieldIndex(filledColunmName)).toString.toDouble
                  (x._1,Row.fromSeq(temp.toSeq))
                }
              })
              val r5=r4.map(x=>x._2)

              val colArr=dfsc.schema.fields.map(x=>{
                if(x.name==filledColunmName)StructField(filledColunmName,DoubleType)
                else x
              })
              val df: DataFrame = sqlc.createDataFrame(r5,StructType(colArr))

              dfsc = df
              dfsc.show()
              println(666666)

              println(66662)




              //*************
            }
            case "3" =>{
              //************
              val r3: RDD[(Int, Row)] = dfsc.rdd.zipWithIndex().map(x=>(x._2.toInt,x._1))
              val list3: Array[Row] =  dfsc.rdd.collect()
              val list: Broadcast[Array[Row]] = sc.broadcast(list3)
              val cou = r3.count()
              val r4 = r3.map(f = x => {
                if (x._2.getAs(filledColunmName) == null) {
                  if (x._1 == 0 || x._1 == 1 ||x._1==2|| x._1 == cou - 1 || x._1 == cou - 2||x._1==cou-3) {
                    x
                  } else {
                    val up0 = list.value(x._1 - 2)
                    val up1 = list.value(x._1 - 1)
                    val up2 = list.value(x._1 - 3)
                    val down0 = list.value(x._1 + 1)
                    val down1 = list.value(x._1 + 2)
                    val down2 = list.value(x._1 + 3)
                    if (up0.getAs(filledColunmName) == null || up1.getAs(filledColunmName) == null|| up2.getAs(filledColunmName) == null || down0.getAs(filledColunmName) == null || down1.getAs(filledColunmName) == null|| down2.getAs(filledColunmName) == null) {
                      x
                    } else {
                      val x0 = up0.getAs(filledColunmName).toString.toDouble
                      val x1 = up1.getAs(filledColunmName).toString.toDouble
                      val x2 = up2.getAs(filledColunmName).toString.toDouble
                      val y0 = down0.getAs(filledColunmName).toString.toDouble
                      val y1 = down1.getAs(filledColunmName).toString.toDouble
                      val y2 = down2.getAs(filledColunmName).toString.toDouble

                      val temp = x._2.toSeq.toArray
                      temp(x._2.fieldIndex(filledColunmName))=(x0+x1+x2+y0+y1+y2)/6
                      (x._1,Row.fromSeq(temp.toSeq))
                    }
                  }
                } else {
                  val temp = x._2.toSeq.toArray
                  temp(x._2.fieldIndex(filledColunmName))= temp(x._2.fieldIndex(filledColunmName)).toString.toDouble
                  (x._1,Row.fromSeq(temp.toSeq))
                }
              })
              val r5=r4.map(x=>x._2)
              val colArr=dfsc.schema.fields.map(x=>{
                if(x.name==filledColunmName)StructField(filledColunmName,DoubleType)
                else x
              })
              val df: DataFrame = sqlc.createDataFrame(r5,StructType(colArr))

              dfsc = df
              dfsc.show()
              println(666666)
              //            val rddx: RDD[Row] = dfsc.filter(filledColunmName+" is null").rdd
              println(66663)
              //*************
            }
            case "4" =>{
              val age: DataFrame =dfsc.describe(filledColunmName)
              val ar: Array[Row] = age.collect()
              val mean = ar(1)(1).toString.toDouble.toInt
              dfsc.filter(filledColunmName+" is null").select(filledColunmName).limit(20).show()
              dfsc = dfsc.na.fill(value=mean,cols=Array(filledColunmName))
              println(mean)
              dfsc.show()
              println(111)
            }
            case _=>throw new Exception("超出了范围[1,2,3,4]")
          }
          println(666)
        }

        //临近点中值填充
        case "del7"   =>{
          val range = obj.get("setter").getAsString()
          range match {
            case "1" =>{
              val r3: RDD[(Int, Row)] = dfsc.rdd.zipWithIndex().map(x=>(x._2.toInt,x._1))
              val list3: Array[Row] =dfsc.rdd.collect()

              val list = sc.broadcast(list3)

              val cou = r3.count()

              val r4: RDD[(Int, Row)] = r3.map(x=>{
                if(x._2.getAs(filledColunmName)==null){
                  if(x._1==0||x._1==cou-1){
                    x
                  }
                  else{
                    var up: Row = list.value(x._1-1)
                    var down = list.value(x._1+1)

                    if(up.getAs(filledColunmName)==null||down.getAs(filledColunmName)==null){
                      x
                    }else{
                      val x0 = up.getAs(filledColunmName).toString.toDouble
                      val y0 = down.getAs(filledColunmName).toString.toDouble

                      val temp = x._2.toSeq.toArray
                      temp(x._2.fieldIndex(filledColunmName))=(x0+y0)/2
                      (x._1,Row.fromSeq(temp.toSeq))
                    }
                  }
                }else{
                  val temp = x._2.toSeq.toArray
                  temp(x._2.fieldIndex(filledColunmName))= temp(x._2.fieldIndex(filledColunmName)).toString.toDouble
                  (x._1,Row.fromSeq(temp.toSeq))
                }
              })

              val r5=r4.map(x=>x._2)

              val colArr=dfsc.schema.fields.map(x=>{
                if(x.name==filledColunmName)StructField(filledColunmName,DoubleType)
                else x
              })
              val df: DataFrame = sqlc.createDataFrame(r5,StructType(colArr))

              dfsc = df
              dfsc.show()
              println(666666)
            }
            case "2" =>{
              println("here")
              //************
              val r3: RDD[(Int, Row)] = dfsc.rdd.zipWithIndex().map(x=>(x._2.toInt,x._1))
              val list3: Array[Row] =  dfsc.rdd.collect()
              val list: Broadcast[Array[Row]] = sc.broadcast(list3)
              val cou = r3.count()
              val r4 = r3.map(f = x => {
                if (x._2.getAs(filledColunmName) == null) {
                  if (x._1 == 0 || x._1 == 1 || x._1 == cou - 1 || x._1 == cou - 2) {
                    x
                  } else {
                    val up0 = list.value(x._1 - 2)
                    val up1 = list.value(x._1 - 1)
                    val down0 = list.value(x._1 + 2)
                    val down1 = list.value(x._1 + 1)
                    if (up0.getAs(filledColunmName) == null || up1.getAs(filledColunmName) == null || down0.getAs(filledColunmName) == null || down1.getAs(filledColunmName) == null) {
                      x
                    } else {
                      val x0= up0.getAs(filledColunmName).toString.toDouble
                      val x1= up1.getAs(filledColunmName).toString.toDouble
                      val y0= down0.getAs(filledColunmName).toString.toDouble
                      val y1= down1.getAs(filledColunmName).toString.toDouble
                      val a= List(x0,x1,y0,y1).sorted

                      val temp = x._2.toSeq.toArray
                      temp(x._2.fieldIndex(filledColunmName))=(a(1)+a(2))/2
                      (x._1,Row.fromSeq(temp.toSeq))
                    }


                  }
                } else {
                  val temp = x._2.toSeq.toArray
                  temp(x._2.fieldIndex(filledColunmName))= temp(x._2.fieldIndex(filledColunmName)).toString.toDouble
                  (x._1,Row.fromSeq(temp.toSeq))
                }
              })


              val r5=r4.map(x=>x._2)
              val colArr=dfsc.schema.fields.map(x=>{
                if(x.name==filledColunmName)StructField(filledColunmName,DoubleType)
                else x
              })
              val df: DataFrame = sqlc.createDataFrame(r5,StructType(colArr))

              dfsc = df
              dfsc.show()
              println(666666)
            }
            case "3" =>{

              //************
              val r3: RDD[(Int, Row)] = dfsc.rdd.zipWithIndex().map(x=>(x._2.toInt,x._1))
              val list3: Array[Row] =  dfsc.rdd.collect()
              val list: Broadcast[Array[Row]] = sc.broadcast(list3)
              val cou = r3.count()
              val r4 = r3.map(f = x => {
                if (x._2.getAs(filledColunmName) == null) {
                  if (x._1 == 0 || x._1 == 1 ||x._1==2|| x._1 == cou - 1 || x._1 == cou - 2||x._1==cou-3) {
                    x
                  } else {
                    val up0 = list.value(x._1 - 2)
                    val up1 = list.value(x._1 - 1)
                    val up2 = list.value(x._1 - 3)
                    val down0 = list.value(x._1 + 2)
                    val down1 = list.value(x._1 + 1)
                    val down2 = list.value(x._1 + 3)
                    if (up0.getAs(filledColunmName) == null || up1.getAs(filledColunmName) == null|| up2.getAs(filledColunmName) == null || down0.getAs(filledColunmName) == null || down1.getAs(filledColunmName) == null|| down2.getAs(filledColunmName) == null) {
                      x
                    } else {
                      val x0 = up0.getAs(filledColunmName).toString.toDouble
                      val x1 = up1.getAs(filledColunmName).toString.toDouble
                      val x2 = up2.getAs(filledColunmName).toString.toDouble
                      val y0 = down0.getAs(filledColunmName).toString.toDouble
                      val y1 = down1.getAs(filledColunmName).toString.toDouble
                      val y2 = down2.getAs(filledColunmName).toString.toDouble
                      val a = List(x0,x1,x2,y0,y1,y2)


                      val temp = x._2.toSeq.toArray
                      temp(x._2.fieldIndex(filledColunmName))=(a(2)+a(3))/2
                      (x._1,Row.fromSeq(temp.toSeq))
                    }
                  }
                } else {
                  val temp = x._2.toSeq.toArray
                  temp(x._2.fieldIndex(filledColunmName))= temp(x._2.fieldIndex(filledColunmName)).toString.toDouble
                  (x._1,Row.fromSeq(temp.toSeq))
                }
              })


              val r5=r4.map(x=>x._2)
              val colArr=dfsc.schema.fields.map(x=>{
                if(x.name==filledColunmName)StructField(filledColunmName,DoubleType)
                else x
              })
              val df: DataFrame = sqlc.createDataFrame(r5,StructType(colArr))

              dfsc = df
              dfsc.show()
              println(666666)
              //            val rddx: RDD[Row] = dfsc.filter(filledColunmName+" is null").rdd

              println(66663)
              //*************

            }
            case "4" =>{
              val rdd = dfsc.select(filledColunmName).na.drop().map(_.getAs(0).toString.toDouble)

              val sorted = rdd.sortBy(identity).zipWithIndex().map {
                case (v, idx) => (idx, v)
              }

              val count = sorted.count()
              val median: Double = if (count % 2 == 0) {
                val l = count / 2 - 1
                val r = l + 1
                (sorted.lookup(l).head + sorted.lookup(r).head) / 2


              } else sorted.lookup(count / 2).head
              println(median)
              dfsc = dfsc.na.fill(value=median,cols=Array(filledColunmName))
              println(median)
              dfsc.show()
              println(789)
            }
            case _=>throw new Exception("超出了范围[1,2,3,4]")
          }
          println(777)
        }
        //固定值填充
        case "del8"   =>{
          dfsc.filter(filledColunmName+" is null").select(filledColunmName).limit(5).show()
          val solidValue1 = obj.get("setter").getAsString()
          val r1=scala.util.Try(solidValue1.toInt)
          val result = r1 match {
            case Success(_) => "Int" ;
            case _ =>  "NotInt"
          }
          if("Int".equals(result)) {
            val solidValue = obj.get("setter").getAsString.toInt
            dfsc = dfsc.na.fill(value=solidValue,cols=Array(filledColunmName))
          } else if("NotInt".equals(result)) {
            val r2 = scala.util.Try(solidValue1.toDouble)
            val result2 = r2 match {
              case Success(_) => "Double" ;
              case _ =>  "String"
            }
            if("Double".equals(result2)) {
              val solidValue2 = obj.get("setter").getAsString.toDouble
              dfsc = dfsc.na.fill(value=solidValue2,cols=Array(filledColunmName))
            } else if("String".equals(result2)) {
              dfsc = dfsc.na.replace(filledColunmName, Map("" -> solidValue1))
            }
          }
          dfsc.filter(filledColunmName+" is null").select(filledColunmName).limit(20).show()

          dfsc.show()
          println(888)
        }
        case "del9" => {
          dfsc = dfsc.na.drop(Seq(filledColunmName))
        }
        case _=>throw new Exception("请选择填充方式")
      }
    }
    outputrdd.put(rddTableName,dfsc)

    dfsc.registerTempTable(rddTableName)
    sqlc.cacheTable(rddTableName)




  }
}
