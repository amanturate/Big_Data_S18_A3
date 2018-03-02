import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Assignment_18_3 extends App{

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local")
    .appName("example")
    .config("spark.sql.warehouse.dir","C://ACADGILD")
    .getOrCreate()

  // A CSV dataset is pointed to by path.
  // The path can be either a single text file or a directory storing text files.
  val dataset_1 = spark.sqlContext.read.csv("C:/ACADGILD/Big Data/SESSION_18/Big_Data_S18_A2/S18_Dataset_Holidays.txt")
    .toDF("id", "source", "destination", "transport_mode", "distance", "year" )

  // Register this DataFrame as a table.
  dataset_1.createOrReplaceTempView("holidays")

  val dataset_2 = spark.sqlContext.read.csv("C:/ACADGILD/Big Data/SESSION_18/Big_Data_S18_A2/S18_Dataset_Transport.txt")
    .toDF( "transport_mode", "cost_per_unit" )

  // Register this DataFrame as a table.
  dataset_2.createOrReplaceTempView("transport")

  val dataset_3 = spark.sqlContext.read.csv("C:/ACADGILD/Big Data/SESSION_18/Big_Data_S18_A2/S18_Dataset_User_Details.txt")
    .toDF(  "id", "name", "age" )

  // Register this DataFrame as a table.
  dataset_3.createOrReplaceTempView("details")

  //--------------------------------------------------------------------------------------------------------------------
  //------------------------------------------ PROBLEM 1 ---------------------------------------------------------------
  //--------------------------------------------------------------------------------------------------------------------

  //  The CASE function lets you evaluate conditions and return a value when the first condition is met
  val prob_1_1 = spark.sql("select case when age < 20 then '1-20'" +
    " when age >= 20 and age <= 35 then '20-35'" +
    " when age > 35 then '35+' end as Age_group, " +
    " holidays.id, year,(distance*cost_per_unit) cost  from holidays " +
    "left outer join details on holidays.id = details.id " +
    "left outer join transport on holidays.transport_mode = transport.transport_mode").toDF()

  prob_1_1.createOrReplaceTempView("age_dist")

  val prob_1 = spark.sql("select Age_group, sum(cost) as money_spent,rank() over (order by sum(cost) desc) as rnk " +
    "from age_dist group by Age_group").toDF()

  prob_1.createOrReplaceTempView("spend")

  spark.sql("select Age_group,money_spent from spend where rnk=1").show()

  //--------------------------------------------------------------------------------------------------------------------
  //------------------------------------------ PROBLEM 2 ---------------------------------------------------------------
  //--------------------------------------------------------------------------------------------------------------------

  val prob_2 = spark.sql("select year, Age_group, sum(cost) as money_spent " +
    "from age_dist group by year,Age_group").show()

}
