package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  

  def ST_Contains(queryRectangle:String, pointString:String): Boolean = {
    
    val diagonals = queryRectangle.split(",")
    var x1 = diagonals(0).toDouble
    var y1 = diagonals(1).toDouble
    var x2 = diagonals(2).toDouble
    var y2 = diagonals(3).toDouble   

    val points = pointString.split(",")
    var x = points(0).toDouble
    var y = points(1).toDouble

    if (diagonals == null || points == null)
      return false
    if(x>=x1 && x<=x2 && y>=y1 && y<=y2)
      return true
    else if(x>=x2 && x<=x1 && y<=y1 && y>=y2)
      return true
    
    return false

  } 
    
  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean = {
    val p1 = pointString1.split(",")
    var x1 = p1(0).toDouble
    var y1 = p1(1).toDouble
    val p2 = pointString2.split(",")
    var x2 = p2(0).toDouble
    var y2 = p2(1).toDouble
    
    val actual = (x1-x2)*(x1-x2) + (y1-y2)*(y1-y2)
    val dist = distance*distance
    if(actual<=dist)
      return true
    else
      return false

  }  
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
    
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{ST_Contains(queryRectangle,pointString)})

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{ST_Contains(queryRectangle,pointString)})

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{ST_Within(pointString1,pointString2,distance)})

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{ST_Within(pointString1,pointString2,distance)})
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
