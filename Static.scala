package org.apache.spark.ny


import java.io.Serializable
import java.util.logging.Logger

import scala.Tuple2
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lag, round}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ListBuffer}
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import java.nio.ByteBuffer

import com.alibaba.fastjson._
import org.apache.mahout.math
import scala.collection.JavaConverters._


object Static {
  val nullVolt = 0
  val cellnum = 500
  val step = 5
  val maxCurrent = 150
  val padding = Math.exp(-9)
  val cellName = "cell_volt"
  val currentName  = "current"
  val imei = "imei"
  val timeIn = "timeIn"
  val temp = "temps"
  val hbaseFamily = "c"
  val hbaseCol = "value"
  val maxVoltName = "maxVolt"
  val minVoltName = "mixVolt"
  val voltDiffName = "voltDiff"
  val sumPowerName = "sumPower"
  val voltAvgName = "voltAvg"
  val voltStdName = "voltStd"
  val maxVoltDiffName = "maxVoltDiff"
  val IRAvgName  = "IRAvg"
  val IRStdName = "IRStd"
  val SopAvgName = "SopAvg"
  val SopStdName = "SopStd"
  val idCol = List("current", "imei", "seq", "soc", "sumVolt", "timeData", "timeIn", "type")
  val voltCol = for (i <- 0 until cellnum)  yield cellName + i.toString
  val host = "localhost"
  val port =  "2181"
  val inputTable = "table"
  val outputTable = "todo"
  //    val startRow = ""
  //    val endRow = " "

  def main(args:Array[String]): Unit ={
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.property.clientPort", port)
    hbaseConf.set("hbase.zookeeper.quorum", host)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, inputTable)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, outputTable)
//    hbaseConf.set(TableInputFormat.SCAN_ROW_START, startRow)
//    hbaseConf.set(TableInputFormat.SCAN_ROW_STOP, endRow)

    //此处需要配置序列化的处理
    val sparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ss = SparkSession.builder
      .master("local")
      .appName("test")
      .config(conf=sparkConf)
      .getOrCreate()
    val sc = ss.sparkContext
    val rdd  =sc.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    val rdd1 = rdd.map(tuplerow => tuplerow._2).map(kvRow => (kvRow.getRow, kvRow.getValue(Bytes.toBytes(hbaseFamily),
      Bytes.toBytes(hbaseCol)))).map(row => (new String(row._1), new String(row._2))).map(row => parseJson(row))
    val df = map2DF(ss, rdd1).filter("type='225'")
    val df1 = internal_resistance(df)
    val df1Cols  =df1.columns  //array
    val rdd2 = df1.rdd.map(row =>statistics(row, df1Cols)).foreach(println)
//    val df2  = map2DF(ss, rdd2)
//    println(df2.show())
    ss.stop()
  }


  //注意单位
  def statistics(row:Row, cols:Array[String])= {
    val coupleList = cols.toList zip row.toSeq.toList
    var coupleMap = coupleList.toMap
    val current = coupleMap(currentName)
    var powerMap: Map[String, String] = Map()
    //单体功率 w
    for (name <- voltCol if coupleMap(name).toString.toInt > 200)
      powerMap += (name + "_power" -> (coupleMap(name).toString.toDouble * current.toString.toInt / 10000).toString)
    val powerList = powerMap.values.map(x => x.toDouble)
    coupleMap ++ powerMap
    //包功率 Kw
    coupleMap += (sumPowerName -> (powerList.sum / 1000))
    //有效单体的阈值是200
    val effectVolt = for (name <- voltCol if coupleMap(name).toString.toInt > 200)
      yield coupleMap(name).toString.toDouble
    val effectLength = effectVolt.length
    val sumVolt = effectVolt.sum
    val voltAvg = sumVolt / effectLength
    coupleMap += (voltAvgName -> voltAvg)
    //单体标准差
    val voltSqr = for (cell <- effectVolt) yield (cell - voltAvg) * (cell - voltAvg)
    val voltStd = Math.sqrt(voltSqr.sum / effectLength)
    coupleMap += (voltStdName -> voltStd)
    //内阻
    val effectIR = for (name <- voltCol if coupleMap(name).toString.toInt > 200)
      yield coupleMap(name + "_IR").toString.toDouble
    val sumIR = effectIR.sum
    val IRAvg = sumIR / effectLength
    coupleMap += (IRAvgName -> IRAvg)
    //内阻标准差
    val IRSqr = for (cell <- effectIR) yield (cell - IRAvg) * (cell - IRAvg)
    val IRStd = Math.sqrt(IRSqr.sum / effectLength)
    coupleMap += (IRStdName -> IRStd)
    //Sop
    val effectSop = for (name <- voltCol if coupleMap(name).toString.toInt > 200)
      yield coupleMap(name + "_Sop").toString.toDouble
    val sumSop = effectSop.sum
    val SopAvg = sumSop / effectLength
    coupleMap += (SopAvgName -> SopAvg)
    //sop标准差
    val SopSqr = for (cell <- effectSop) yield (cell - SopAvg) * (cell - SopAvg)
    val SopStd = Math.sqrt(SopSqr.sum / effectLength)
    coupleMap += (SopStdName -> SopStd)
    val coupleMapToStr = coupleMap.mapValues(v => v.toString.toDouble.formatted("%.2f"))
    coupleMapToStr
  }



  def parseJson(row:Tuple2[String, String]): Map[String, String] = {
    var idMap: Map[String, String] = Map()
    val data = JSON.parseObject(row._2)
    val voltJsonArray = data.getJSONArray("volt") //jsonArray,里面每个元素类型是java string
    val size = voltJsonArray.size()
    for(id <- idCol) idMap += (id -> data.getString(id))
    //后续计算最大最小温度需要 ℃
    val tempJsonArray = data.getJSONArray("temps")
    //ArrayBuffer此时temp和temp.toString是java.lang.String
    val tempArrayBuffer = for (temp <- tempJsonArray) yield temp.toString
    val maxTemp = tempArrayBuffer.max.toInt / 10
    val minTemp = tempArrayBuffer.min.toInt / 10
    val meanTemp = (maxTemp + minTemp) / 2
    idMap += ("maxTemp" -> maxTemp.toString) //覆盖原有的maxTemp字段
    idMap += ("minTemp" -> minTemp.toString)
    idMap += ("meantemp" -> meanTemp.toString)
    // 单体最大最小和压差 mv
    val maxCellJsonArray = data.getJSONArray("maxCell")
    val minCellJsonArray = data.getJSONArray("minCell")
    val maxCellArrayBuffer = for (cell <- maxCellJsonArray) yield cell.toString
    val minCellArrayBuffer = for (cell <- minCellJsonArray) yield cell.toString
    val maxVolt = maxCellArrayBuffer.max.toInt
    val minVolt = minCellArrayBuffer.min.toInt
    val voltDiff = maxVolt - minVolt
    idMap += (maxVoltName -> maxVolt.toString) //覆盖原有的maxTemp字段
    idMap += (minVoltName -> minVolt.toString)
    idMap += (voltDiffName -> voltDiff.toString)
    //单体扩充到同一长度
    val voltName = for (i <- 0 until cellnum) yield cellName + i.toString // 生成电池集合Vector
    for (i <- 0 until size) idMap += (voltName(i) -> voltJsonArray(i).toString) // 有数据的单体集合（包括跳线和实际单体）
    for (i <- size until cellnum) idMap += (cellName + i.toString -> nullVolt.toString) //null 单体集合
    for (id <- idCol) idMap += (id -> data.getString(id))
    idMap
  }

  def map2DF(spark: SparkSession, rdd:RDD[Map[String, String]]): DataFrame = {
    val cols = rdd.take(1).flatMap(_.keys)
    val resRDD = rdd.filter(_.nonEmpty).map { m =>val seq = m.values.toSeq; Row.fromSeq(seq)
    }
    val fields = cols.map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    spark.createDataFrame(resRDD, schema)
  }


//窗口聚合很耗时间
  def internal_resistance(df:DataFrame) = {
    val window = Window.partitionBy(imei).orderBy(timeIn)
    // 计算IR，单位毫欧 moh
    val df1 = df.withColumn("current_delt", lag("current", step, padding).over(window) - df.col(currentName))
    val IRSeq = for(name <- voltCol) yield {((lag(df1.col(name), step, padding).over(window)- df1.col(name))*10
      /(df.col(currentName)+ Math.exp(-9))).alias(name+"_IR") }  //IndexedSeq[Column]
    val idColList = for(name <- idCol) yield df1.col(name) //List(Column)
    val voltColList = for(i <- 0 until cellnum)  yield df1.col(cellName + i.toString)
    val idColArray = idColList.toArray
    val IRArray = IRSeq.toArray
    val voltColArray = voltColList.toArray
    val df2ColArray = idColArray ++  voltColArray ++ IRArray
    // scala中只能column和column操作,此处Imax可以根据电流温度关系表得到（后续模式匹配来处理）
    val df2 = df1.select(df2ColArray:_*).withColumn("maxCurrent", df1.col("cell_volt499")+150)
    //计算单体Sop，单位是W，所以单体电压除以1000， 电流除以10
    val sopSeq = for(name <- voltCol) yield {(df2.col("maxCurrent")*(df2.col(name)/1000 + (df2.col("maxCurrent")
      - df2.col(currentName)/10)*df2.col(name+"_IR")/1000)).alias(name+"_Sop") }
    val sopArray = sopSeq.toArray
    val df3ColArray = df2ColArray++sopArray
    val df3 = df2.select(df3ColArray:_*) //*args
    df3
  }



//  def saveHbase()={
//    val startRow = ByteBuffer.allocate(15)
//    val endRow = ByteBuffer.allocate(15)
//    val imei = 867330021927168L
//    val packType:Byte = 2
//    val tmBegin = 1515827200
//    val tmEnd   = 1515978360
//    val seqBegin:Short = 0
//    val seqEnd:Short = -1
//    //配置rowKey
//    val startRowkey = "rk1"
//    val endRowkey = " "
//    val scan = new Scan(Bytes.toBytes(startRowkey),Bytes.toBytes(endRowkey))
//    scan.setCacheBlocks(false)
//    scan.addFamily(Bytes.toBytes("ks"));
//    scan.addColumn(Bytes.toBytes("ks"), Bytes.toBytes("data"))
//    startRow.putLong(imei).put(packType).putInt(tmBegin).putShort(seqBegin)
//    endRow.putLong(imei).put(packType).putInt(tmEnd).putShort(seqEnd)
//    val scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow))
//    scan.addFamily(Bytes.toBytes("c"))
//    scan.addColumn(Bytes.toBytes("c"),Bytes.toBytes("m"))
//    scan.setCaching(1000)
//  }
}



