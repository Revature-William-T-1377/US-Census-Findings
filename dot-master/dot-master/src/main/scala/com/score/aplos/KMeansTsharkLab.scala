package com.score.aplos

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{count, udf, _}
import org.apache.spark.sql.types._

object UdfUtil extends Serializable {
  def distance(centers: Array[Vector]): UserDefinedFunction = udf((features: Vector, c: Int) =>
    Vectors.sqdist(features, centers(c)))
}

object KMeansTsharkLab extends App {

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  import spark.implicits._

  // schema of tshark
  val schema = StructType(
    StructField("ip_src", StringType, nullable = true) ::
      StructField("ip_dst", StringType, nullable = true) ::
      StructField("ip_len", DoubleType, nullable = true) ::
      StructField("eth_src", StringType, nullable = true) ::
      StructField("eth_dst", StringType, nullable = true) ::
      StructField("tcp_src_port", IntegerType, nullable = true) ::
      StructField("tcp_dst_port", IntegerType, nullable = true) ::
      StructField("frame_time_epoch", StringType, nullable = true) ::
      StructField("frame_len", DoubleType, nullable = true) ::
      StructField("frame_protocols", StringType, nullable = true) ::
      StructField("frame_time", TimestampType, nullable = true) ::
      Nil
  )

  // read tshark.csv file to data frame
  val df = spark.read.format("csv")
    .option("header", value = true)
    .option("delimiter", "|")
    .option("mode", "DROPMALFORMED")
    .option("timestampFormat", "MMM dd yyyy HH:mm:ss")
    .schema(schema)
    .load(getClass.getResource("/tshark.csv").getPath)
    .cache()
  df.printSchema()
  df.show(truncate = false)

  // add unix timestamp to data frame
  val tDf = df.withColumn("timestamp", unix_timestamp($"frame_time"))
  tDf.show(truncate = false)

  // group with 1 minute time, ip_src, frame_protocols
  // calculate matrix(count, average len, local anomaly)
  val gDf = tDf
    .groupBy(
      window($"frame_time", "1 minutes")
        .alias("time_window"), $"ip_src", $"frame_protocols"
    )
    .agg(
      count("*").as("count"),
      avg("ip_len").as("ip_avg_len"),
      avg("frame_len").as("frame_avg_len"),
      (avg("ip_len") / max("ip_len")).as("ip_local_anomaly"),
      (avg("frame_len") / max("frame_len")).as("frame_local_anomaly")
    )
    .withColumn("start", col("time_window")("start"))
    .withColumn("end", col("time_window")("end"))
  gDf.printSchema()
  gDf.show(truncate = false)

  // join tDf and gDf
  // join with ip_src, frame_protocols, frame_time (in range of start and end)
  // select only wanted field
  val cond = tDf("ip_src") === tDf("ip_src") &&
    tDf("frame_protocols") === tDf("frame_protocols") &&
    tDf("frame_time") >= gDf("start") && tDf("frame_time") <= gDf("end")
  val jDf = tDf
    .join(gDf, cond, "left")
    .select(tDf("ip_src"), tDf("ip_dst"), tDf("ip_len"), tDf("tcp_src_port"), tDf("tcp_dst_port"),
      tDf("frame_protocols"), tDf("frame_len"), tDf("frame_time"), tDf("timestamp"),
      gDf("ip_avg_len"), gDf("frame_avg_len"), gDf("ip_local_anomaly"), gDf("frame_local_anomaly"),
      gDf("count"))
  jDf.printSchema()
  jDf.show(truncate = false)

  // add feature column
  val cols = Array("ip_avg_len", "frame_avg_len", "ip_local_anomaly", "frame_local_anomaly", "count")
  val ass = new VectorAssembler().setInputCols(cols).setOutputCol("features")
  val fDf = ass.transform(jDf)
  fDf.printSchema()
  fDf.show(truncate = false)

  // split data set training(70%) and test(30%)
  val seed = 5043
  val Array(trnDta, tstDta) = fDf.randomSplit(Array(0.7, 0.3), seed)

  // kmeans model with 2 clusters
  val kmeans = new KMeans()
    .setK(2)
    .setFeaturesCol("features")
    .setPredictionCol("prediction")
  val model = kmeans.fit(trnDta)
  model.clusterCenters.foreach(println)

  // test the model with test data set
  val pDf = model.transform(tstDta)
  pDf.show(truncate = false)

  // data frame with with required fields(e.g for visualization)
  val vDf = pDf.select($"timestamp", $"features", $"prediction")
  vDf.show(truncate = false)

  // calculate distance of each point from cluster center and add to data frame
  // add sequence id field to data frame
  val dDf = vDf.withColumn("distance", UdfUtil.distance(model.clusterCenters)($"features", $"prediction"))
    .withColumn("id", monotonically_increasing_id())
  dDf.show(truncate = false)

  // create temp table for sql visualization
  dDf.createOrReplaceTempView("tshark")

}

