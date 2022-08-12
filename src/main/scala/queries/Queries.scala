package queries

object Queries {
  def query1(): String = {
    "SELECT CAST(pop2000 AS String), pop2010, pop2020 FROM " +
      "(SELECT SUM(p0010001) AS pop2020 FROM c2020) " +
      "join (SELECT SUM(p0010001) AS pop2010 FROM c2010)" +
      "join (SELECT SUM(p0010001) AS pop2000 FROM c2000)"
  }

  /** **************************CHANGING POP IN STATES OVER DECADES****************************************************** */
  def query2(): String = {
    "SELECT t1.STUSAB, pop2000, pop2010, ROUND((((pop2010 - pop2000)/pop2000)*100),1) AS pop2000_2010, " +
      "pop2020, ROUND((((pop2020 - pop2010)/pop2010)*100),1) AS pop2010_2020 FROM " +
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 " +
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB"
  }

  /** *****************************TRENDLINE PREDICITON************************************************** */
  def query3(): String = {
    "SELECT t1.STUSAB, pop2000, pop2010, " +
      "ROUND(((((pop2010 - pop2000)/pop2000) * pop2010) + pop2010),1) AS pop2020Pred, pop2020 FROM " +
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 " +
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB"
  }

  /** *****************************FASTEST GROWING STATE********************************************** */
  def query4(): String = {
    "SELECT t1.STUSAB, pop2020, pop2010, pop2000, " +
      "ROUND((((pop2020 - pop2000)/pop2000) * 100),1) AS popGrowth FROM " +
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 " +
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB ORDER BY popGrowth DESC"
  }

  /** *****************************STATES WITH DECREASING POPULATION********************************************** */
  def query5(): String = {
    "SELECT t1.STUSAB, pop2020, pop2010, pop2000, " +
      "ROUND((((pop2020 - pop2000)/pop2000) * 100),1) AS popGrowth FROM " +
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 " +
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB AND ROUND((((pop2020 - pop2000)/pop2000) * 100),1) < 0 ORDER BY popGrowth ASC"

  }

  /** ***************CHANGE IN POPULATION EACH DECADE******************************** */
  def query6(): String = {
    "SELECT t1.STUSAB, pop2000, (pop2010 - pop2000) AS change10, pop2020 - pop2010 AS change20" +
      " FROM " +
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 " +
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB"
  }

  /** **********************HIGHEST POPULATION REGION***************************** */
  def query7(): String = {
    "Select Region, pop2000, pop2010, pop2020 FROM region"
  }

  /** **************CHANGING POP IN REGION*********************888****** */
  def query8(): String = {
    "SELECT Region, pop2000, pop2010, ROUND((((pop2010 - pop2000)/pop2000)*100),1) AS pop2000_2010, " +
      "pop2020, ROUND((((pop2020 - pop2010)/pop2010)*100),1) AS pop2010_2020, " +
      "ROUND((((pop2020 - pop2000)/pop2000)*100),1) AS pop2000_2020 FROM region ORDER BY pop2010_2020 DESC"
  }

  /** ****************************************POP OF NORTHEAST************************************ */
  def queryNE(): String = {
    "SELECT pop2000, pop2010, pop2020 FROM " +
      "(SELECT SUM(p0010001) AS pop2020 FROM c2020 WHERE Region = 'Northeast') " +
      "join (SELECT SUM(p0010001) AS pop2010 FROM c2010 WHERE Region = 'Northeast') " +
      "join (SELECT SUM(p0010001) AS pop2000 FROM c2000 WHERE Region = 'Northeast')"
  }

  /** ****************************************POP OF SOUTHWEST************************************ */
  def querySW(): String = {
    "SELECT pop2000, pop2010, pop2020 FROM " +
      "(SELECT SUM(p0010001) AS pop2020 FROM c2020 WHERE Division = 'West_South_Central') " +
      "join (SELECT SUM(p0010001) AS pop2010 FROM c2010 WHERE Division = 'West_South_Central') " +
      "join (SELECT SUM(p0010001) AS pop2000 FROM c2000 WHERE Division = 'West_South_Central')"
  }


  /** ****************************************POP OF WEST************************************ */
  def queryW(): String = {
    "SELECT pop2000, pop2010, pop2020 FROM " +
      "(SELECT SUM(p0010001) AS pop2020 FROM c2020 WHERE Region = 'West') " +
      "join (SELECT SUM(p0010001) AS pop2010 FROM c2010 WHERE Region = 'West') " +
      "join (SELECT SUM(p0010001) AS pop2000 FROM c2000 WHERE Region = 'West')"
  }

  /** ****************************************POP OF SOUTHEAST************************************ */
  def querySE(): String = {
    "SELECT pop2000, pop2010, pop2020 FROM" +
      "(SELECT pop2020A + pop2020B AS pop2020 FROM " +
      "(SELECT SUM(p0010001) AS pop2020A FROM c2020 WHERE Division = 'East_South_Central') " +
      "join (SELECT SUM(p0010001) AS pop2020B FROM c2020 WHERE Division = 'South_Atlantic')) " +
      "join (SELECT pop2010A + pop2010B AS pop2010 FROM " +
      "(SELECT SUM(p0010001) AS pop2010A FROM c2010 WHERE Division = 'East_South_Central') " +
      "join (SELECT SUM(p0010001) AS pop2010B FROM c2010 WHERE Division = 'South_Atlantic')) " +
      "join (SELECT pop2000A + pop2000B AS pop2000 FROM " +
      "(SELECT SUM(p0010001) AS pop2000A FROM c2000 WHERE Division = 'East_South_Central') " +
      "join (SELECT SUM(p0010001) AS pop2000B FROM c2000 WHERE Division = 'South_Atlantic'))"
  }

  /** ****************************************POP OF MIDWEST************************************ */
  def queryMW(): String = {
    "SELECT pop2000, pop2010, pop2020 FROM " +
      "(SELECT SUM(p0010001) AS pop2020 FROM c2020 WHERE Region = 'Midwest') " +
      "join (SELECT SUM(p0010001) AS pop2010 FROM c2010 WHERE Region = 'Midwest') " +
      "join (SELECT SUM(p0010001) AS pop2000 FROM c2000 WHERE Region = 'Midwest')"
  }
}
