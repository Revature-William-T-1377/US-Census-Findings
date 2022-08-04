import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DateType, DecimalType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import Console.{CYAN, YELLOW, RED, WHITE, RESET, GREEN}
import org.apache.spark.sql
import scala.io.StdIn.readLine

object Main {
  def main(args: Array[String]): Unit = {
    val session = new SparkInit("Project3")
    var df = session.spark.read.option("header", "true").csv("2020.csv")
    var df2 = session.spark.read.option("header", "true").csv("2010.csv")
    var df3 = session.spark.read.option("header", "true").csv("2000.csv")
    df = df.withColumn("total", col("total").cast(DecimalType(18, 1)))
    df2 = df2.withColumn("total", col("total").cast(DecimalType(18, 1)))
    df3 = df3.withColumn("total", col("total").cast(DecimalType(18, 1)))
    df3 = df3.withColumn("Whitealone", col("Whitealone").cast(DecimalType(18, 1)))
    df.createOrReplaceTempView("c2020")
    df2.createOrReplaceTempView("c2010")
    df3.createOrReplaceTempView("c2000")
    //df.withColumn("total", col("total").cast(DecimalType(18, 1)))
    df3.printSchema()
    session.spark.sql("SELECT SUM(Whitealone),  FROM c2000").show()

    session.spark.sql("SELECT pop2000, pop2010, pop2020 FROM "+
      "(SELECT SUM(total) AS pop2020 FROM c2020) "+
      "join (SELECT SUM(total) AS pop2010 FROM c2010)"+
      "join (SELECT SUM(total) AS pop2000 FROM c2000)").show()

    /****************************CHANGING POP IN STATES OVER DECADES*******************************************************/
    session.spark.sql("SELECT t1.Label, pop2000, pop2010, ROUND((((pop2010 - pop2000)/pop2000)*100),1) AS pop2000_2010, " +
      "pop2020, ROUND((((pop2020 - pop2010)/pop2010)*100),1) AS pop2010_2020 FROM "+
      "(SELECT Label, total AS pop2020 FROM c2020) AS t1 "+
      "INNER JOIN (SELECT Label, total AS pop2010 FROM c2010) AS t2 "+
      "ON t1.Label = t2.Label " +
      "INNER JOIN (SELECT Label, total AS pop2000 FROM c2000) AS t3 " +
      "ON t1.Label = t3.Label").show()
    /**********************************************************************************/


    /*******************************TRENDLINE PREDICITON***************************************************/
    session.spark.sql("SELECT t1.Label, pop2000, pop2010, "+
      "ROUND(((((pop2010 - pop2000)/pop2000) * pop2010) + pop2010),1) AS pop2020Pred, pop2020 FROM "+
      "(SELECT Label, total AS pop2020 FROM c2020) AS t1 "+
      "INNER JOIN (SELECT Label, total AS pop2010 FROM c2010) AS t2 "+
      "ON t1.Label = t2.Label " +
      "INNER JOIN (SELECT Label, total AS pop2000 FROM c2000) AS t3 " +
      "ON t1.Label = t3.Label").show()


    /********************************************************************************************************/

    /*******************************FASTEST GROWING STATE***********************************************/
    session.spark.sql("SELECT t1.Label, pop2020, pop2010, pop2000, "+
      "ROUND((((pop2020 - pop2000)/pop2000) * 100),1) AS popGrowth FROM "+
      "(SELECT Label, total AS pop2020 FROM c2020) AS t1 "+
      "INNER JOIN (SELECT Label, total AS pop2010 FROM c2010) AS t2 "+
      "ON t1.Label = t2.Label " +
      "INNER JOIN (SELECT Label, total AS pop2000 FROM c2000) AS t3 " +
      "ON t1.Label = t3.Label ORDER BY popGrowth DESC").show()


    session.spark.sql("SELECT t1.Label, pop2020, pop2010, pop2000, "+
      "ROUND((((pop2020 - pop2000)/pop2000) * 100),1) AS popGrowth FROM "+
      /*"ROUND((((pop2020 - pop2010)/pop2010) * 100),1) AS popGrowt, " +
      "ROUND((((pop2010 - pop2000)/pop2000) * 100),1) AS popGrow FROM "+*/
      "(SELECT Label, total AS pop2020 FROM c2020) AS t1 "+
      "INNER JOIN (SELECT Label, total AS pop2010 FROM c2010) AS t2 "+
      "ON t1.Label = t2.Label " +
      "INNER JOIN (SELECT Label, total AS pop2000 FROM c2000) AS t3 " +
      "ON t1.Label = t3.Label AND ROUND((((pop2020 - pop2000)/pop2000) * 100),1) < 0 ORDER BY popGrowth ASC").show()
    //population prediction function
    //session.spark.sql("SELECT SUM() FROM yo WHERE label LIKE '%Hi%' AND NOT label LIKE '%Not%'").show()

    /****************************CHANGING POP IN STATES OVER DECADES*******************************************************/
    session.spark.sql("SELECT t1.Label, pop2000, pop2010, ROUND((((pop2010 - pop2000)/pop2000)*100),1) AS pop2000_2010, " +
      "pop2020, ROUND((((pop2020 - pop2010)/pop2010)*100),1) AS pop2010_2020 FROM "+
      "(SELECT Label, total AS pop2020 FROM c2020) AS t1 "+
      "INNER JOIN (SELECT Label, total AS pop2010 FROM c2010) AS t2 "+
      "ON t1.Label = t2.Label " +
      "INNER JOIN (SELECT Label, total AS pop2000 FROM c2000) AS t3 " +
      "ON t1.Label = t3.Label").show()
    /**********************************************************************************/
  }

}
