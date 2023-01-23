package spark.api

import org.apache.spark.sql.Row

import java.util.Properties


object ReadFromJDBC extends App {
  import org.apache.spark.sql.{DataFrame, SparkSession}

  /**
   * Which DataBase Spark can use to create a table?
   *
   * How to create a table with Spark SQL in JDBC source?
   * How to read data from JDBC ?
   * How to read data with pruning  from JDBC ?
   * How to query data in parallel with column?
   * How to query data in parallel with predicate?
   * How to write data to JDBC source?
   *
   * How to optimize joining data reading/writing to JDBC ?
   *
   * */

  val spark = SparkSession
    .builder()
    .appName("JDBC to DF")
    .config("spark.master", "local")
    .getOrCreate()



  // TODO: How to read data from JDBC ?

  val pgUrl = "jdbc:postgresql://localhost:5432/spark"
  val employees = "employees"
  val connectionProps = new Properties()
  connectionProps.put("user", "docker")
  connectionProps.put("password", "docker")
  connectionProps.put("driver", "org.postgresql.Driver")


  val fromPgDF = spark
    .read
    .jdbc(url = pgUrl, table = employees, properties = connectionProps)

  //  read all rows all tables ?
  fromPgDF.show()

  println(s"Read row count is ${fromPgDF.count()}")

  def checkReadTableTimeDF(testDF: DataFrame) =
    spark.time {
      testDF.foreachPartition { it: Iterator[Row] =>
        it.foreach { emp =>
          val newEmp = emp + "test"
          newEmp
        }
      }
    }

  //  checkReadTableTimeDF(fromPgDF) // Time taken: 440 ms

  // TODO: How to read data with pruning and projection for example 3 columns and half of the data ?
  val employees_pruned = """(select e.first_name, e.last_name, e.hire_date from public.employees e where e.gender = 'F') as new_emp"""


  val prunedJdbcDF = spark.read
    .format("jdbc")
    .option("url", pgUrl)
    .option("dbtable", employees_pruned)
    .option("user", "docker")
    .option("password", "docker")
    .load()

  //  checkReadTableTimeDF(prunedJdbcDF)  // Time taken: 320 ms


  // TODO: How to query data in parallel with column?
  //  how many partition we have?


  def printPartitionsNumber(inDF: DataFrame) = println(s"Partition number =  ${inDF.rdd.getNumPartitions}")

  //  printPartitionsNumber(fromPgDF)

  // How to increase number of partitions?
  //  printPartitionsNumber(fromPgDF.repartition(10)) // Good?


  val prunedParallelJdbcDF: DataFrame =
    spark.read.jdbc(
      url = pgUrl,
      table = employees,
      columnName = "emp_no",
      lowerBound = 10010,
      upperBound = 499990,
      numPartitions = 10,
      connectionProperties = connectionProps
    )
  //  prunedParallelJdbcDF.show()

  //  printPartitionsNumber(prunedParallelJdbcDF)

  //  checkReadTableTimeDF(prunedJdbcDF)  // Time taken: 84 ms


  // TODO: How to query data in parallel without column?


  val parallelPredicatedDF: DataFrame =
    spark.read.jdbc(
      url = pgUrl,
      table = employees,
      connectionProperties = connectionProps,
      predicates = Array("gender = 'F'", "gender = 'M'"))


  //  check time and count
  //      predicates = Array("gender = 'F'", "gender = 'M'", "gender = 'M'"))

  printPartitionsNumber(parallelPredicatedDF)

  checkReadTableTimeDF(parallelPredicatedDF)


  // TODO: How to write data to JDBC source?


  // TODO: How to optimize joining data reading/writing to JDBC ?


}
