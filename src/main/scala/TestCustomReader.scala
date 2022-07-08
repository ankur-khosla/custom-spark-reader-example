import org.apache.spark.sql.SparkSession

object TestCustomReader {
  import org.apache.spark.sql.types._
  var fields = Array(
    StructField("firstName", StringType, true),
    StructField("lastName", StringType, true),
    StructField("Gender", StringType, true))
  val schema = StructType(fields)

  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.executor.cores", 4)
      .config("spark.default.parallelism", 7)
      .appName("Custom spark reader")
      .getOrCreate()

    spark.read.schema(schema).format("customAnkur")
      .load("testFile.xyz").show(false)
    Thread.sleep(100000000)
  }
}