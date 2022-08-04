class Queries {
  def query1():String = {
    "SELECT CAST(pop2000 AS String), pop2010, pop2020 FROM " +
      "(SELECT SUM(p0010001) AS pop2020 FROM c2020) " +
      "join (SELECT SUM(p0010001) AS pop2010 FROM c2010)" +
      "join (SELECT SUM(p0010001) AS pop2000 FROM c2000)"
  }
  def query2():String = {
    "SELECT t1.STUSAB, pop2000, pop2010, ROUND((((pop2010 - pop2000)/pop2000)*100),1) AS pop2000_2010, " +
      "pop2020, ROUND((((pop2020 - pop2010)/pop2010)*100),1) AS pop2010_2020 FROM " +
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 " +
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB"
  }
  def query3():String = {
    "SELECT t1.STUSAB, pop2000, pop2010, " +
      "ROUND(((((pop2010 - pop2000)/pop2000) * pop2010) + pop2010),1) AS pop2020Pred, pop2020 FROM " +
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 " +
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB"
  }
  def query4():String = {
    "SELECT t1.STUSAB, pop2020, pop2010, pop2000, " +
      "ROUND((((pop2020 - pop2000)/pop2000) * 100),1) AS popGrowth FROM " +
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 " +
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB ORDER BY popGrowth DESC"
  }
  def query5():String = {
    "SELECT t1.STUSAB, pop2020, pop2010, pop2000, " +
      "ROUND((((pop2020 - pop2000)/pop2000) * 100),1) AS popGrowth FROM " +
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 " +
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB AND ROUND((((pop2020 - pop2000)/pop2000) * 100),1) < 0 ORDER BY popGrowth ASC"

  }
  def query6():String = {
    "SELECT pop2000, pop2010-pop2000 AS change10, pop2020-pop2010 AS change20 FROM " +
      "(SELECT SUM(p0010001) AS pop2020 FROM c2020) " +
      "join (SELECT SUM(p0010001) AS pop2010 FROM c2010)" +
      "join (SELECT SUM(p0010001) AS pop2000 FROM c2000)"

    "SELECT t1.STUSAB, pop2000, (pop2010 - pop2000) AS change10, pop2020 - pop2010 AS change20" +
      " FROM " +
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 " +
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB"
  }

  def query7():String = {
    "SELECT Division, SUM(P0010001) FROM c2020 GROUP BY Division"


    /*"SELECT Division, SUM(P0010001) FROM c2020 WHERE Division = 'West_South_Central' OR " +
      "Division = 'South_Atlantic' GROUP BY Division "*/
  }



}
