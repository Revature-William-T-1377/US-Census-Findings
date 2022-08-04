import org.apache.spark.sql.functions.{col, format_number, lit}
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType}

object cleaned extends App{
  val session = new SparkInit("Project3")
  var df1 = session.spark.read.option("header", "true").csv("Combine2000RG.csv")
  var df2 = session.spark.read.option("header", "true").csv("Combine2010RG.csv")
  var df3 = session.spark.read.option("header", "true").csv("Combine2020RG.csv")
  var df4 = session.spark.read.option("header", "true").csv("headers.csv")
  df1.createOrReplaceTempView("c2000")
  df2.createOrReplaceTempView("c2010")
  df3.createOrReplaceTempView("c2020")
  //slicing the RG files column names
  var dc1 = df1.columns.slice(7,151).toList
  var dc2 = df2.columns.slice(7,151).toList
  var dc3 = df3.columns.slice(7,151).toList
  //column names actual
  var dc4 = df4.columns.toList
  //maps
  var dm1 = (dc1 zip dc4).toMap
  var dm2 = (dc2 zip dc4).toMap
  var dm3 = (dc3 zip dc4).toMap
  //empty data frames
  var dfe1 = session.spark.emptyDataFrame
  //casting to int
  for(i <- dc1){
    df1 = df1.withColumn(s"$i", col(s"$i").cast(DecimalType(38,0)))
  }
  for(i <- dc2){
    df2 = df2.withColumn(s"$i", col(s"$i").cast(DecimalType(38,0)))
  }
  for(i <- dc3){
    df3 = df3.withColumn(s"$i", col(s"$i").cast(DecimalType(38,0)))
  }
  //reading data for tables
  //dfe1 = dfe1.withColumn("Data", lit("Total"))
  var data = session.spark.emptyDataFrame
  var data2 = session.spark.emptyDataFrame
  var prevc = ""
  var bool = true
  for(i <- dc1){
    data = session.spark.sql(s"Select SUM($i) as "+ dm1.get(i).get +" from c2000").toDF()
    data.withColumn("data1", lit("totalx")).show()
    prevc = data.columns(0)
    //data = data.join(data)
    if(!bool){
      data.as("datax").join(data2.as("data2"),
        col(s"datax.data1") === col("data2.data1"),"inner")
        .select(col(s"datax.$prevc"),col("data2."+dm1.get(i).get)
        )
        .show(false)
    }else{
      bool = false
    }
  }
  /*var data = session.spark.sql(s"Select SUM(P0010001) as P0010001 from c2000").toDF()
  dfe1.withColumn("total", data.col("P0010001"))*/
  dfe1.show()
  System.exit(0)
  println("end of 2000 tables")
  for(i <- dc2){
    session.spark.sql(s"Select SUM($i) as "+ dm2.get(i).get +" from c2010").show(1,false)
  }
  println("end of 2010 tables")
  for(i <- dc3){
    session.spark.sql(s"Select SUM($i) as "+ dm3.get(i).get +" from c2020").show(1,false)
  }
  println("end of 2020 tables")
  println(dc3.mkString(","))
  println(dc2.mkString(","))
  println(dc1.mkString(","))
  println(dc4.mkString(","))

}//end of app
