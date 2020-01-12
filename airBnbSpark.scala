import javax.annotation.meta.When
import jdk.jfr.Percentage
import org.apache.avro.generic.GenericData.StringType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.api.java.UDF0
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, Dataset, Row, SQLContext, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

  object airBnbSpark extends App  {

    //stop logging to console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //set spark configuration
    val conf= new SparkConf().setAppName("air_bnb_spark").setMaster("local")
    //build spark session
    val ss=SparkSession.builder().config(conf).getOrCreate();

    val path = "./src/data/listings-original.csv"
    //val path = "/FileStore/tables/listingsoriginal.csv"


    def reviews_per_month_granularity(){

      try{
        //read listing data set
        var rentalText=ss.read
          .option("header", "true")
          .option("treatEmptyValuesAsNulls", "true")
          .option("mode","DROPMALFORMED")
          .option("delimiter", ",")
          .option("inferSchema", "true")
          .csv(path)

        //write data into data frame
        var rentalDf = rentalText.toDF();

        //host id null data and drop null or empty values
        rentalDf = rentalDf.select("last_review").where("last_review IS NOT NULL")

        //cast to date and format
        //rentalDf=rentalDf.withColumn("last_review", to_date(col("last_review"), "MM/dd/yyyy").cast("date"))

        //filter values containing -
        rentalDf=rentalDf.filter(rentalDf("last_review").contains("-"))

        //user defined function to get year with month substring
        val udf_month=udf((x:String)=>x.slice(0,7))

        //add new column
        rentalDf=rentalDf.withColumn("last_review_with_month",udf_month(rentalDf("last_review")))

        //group last review and show count per month granularity
        rentalDf.groupBy("last_review_with_month").count().alias("countOfReview").sort("last_review_with_month").show(1000)

      }
      catch {
        case e: Exception =>
          println(e)
      }
    }
    //reviews_per_month_granularity()

    //percentage_of_rentalHost()
    def percentage_of_rentalHost(){

      try{
        //read listing data set
        var rentalText=ss.read
          .option("header", "true")
          .option("treatEmptyValuesAsNulls", "true")
          .option("mode","DROPMALFORMED")
          .option("delimiter", ",")
          .option("inferSchema", "true")
          .csv(path)

        // convert the string to date in date columns
        val df = rentalText.columns.filter(colName =>colName.endsWith("_review"))
          .foldLeft(rentalText) { (outputDF, columnName) =>
            outputDF.withColumn(columnName, to_date(col(columnName), "MM/dd/yyyy").cast("date"))
          }

        //write data into data frame
        var rentalDf = df.toDF();

        //filter host id with nulls
        val totalHosts=rentalDf.filter(rentalDf("host_id").isNotNull).select("host_id").distinct().count()

        //hosts with multiple rentals
        rentalDf=rentalDf.where("calculated_host_listings_count >1").select("host_id").distinct()

        //get count of hosts with more than one rentals
        rentalDf=rentalDf.groupBy("host_id").count()
          .agg(count("host_id").alias("count"))

        //convert count to percentage
        val udf_percentage=udf((x:Int)=>{
          (x*100)/totalHosts.toDouble
        })

        //add new column to show percentage
        rentalDf.withColumn("percentage",udf_percentage(rentalDf("count"))).show()
      }
      catch{
        case e:Exception=>
          println(e)
      }
    }

    top_five_average_rentals()
    def top_five_average_rentals(){

      try{
        //read listing data set
        var rentalText=ss.read
          .option("header", "true")
          .option("treatEmptyValuesAsNulls", "true")
          .option("mode","DROPMALFORMED")
          .option("delimiter", ",")
          .option("inferSchema", "true")
          .csv(path)

        //convert the string format to date in date columns
        //convert the string format to date in date columns
        val dft = rentalText.columns.filter(colName =>colName.endsWith("_review"))
          .foldLeft(rentalText) { (outputDF, columnName) =>
            outputDF.withColumn(columnName, to_date(col(columnName), "MM/dd/yyyy").cast("date"))
          }

        //write data into data frame
        var rentalDf = dft.toDF();

        //get rentals 365 grouped by neighborhood
        rentalDf = rentalDf.where("availability_365 =365");

        //create temporary view to store data from df
        rentalDf.createOrReplaceTempView("365AvailableRooms")

        rentalDf=rentalDf.sqlContext.sql("select neighbourhood, count(*) as rental_count from 365AvailableRooms" +
          " where neighbourhood in (select neighbourhood from 365AvailableRooms GROUP BY neighbourhood Order By avg(price)" +
          " Limit 5) GROUP BY neighbourhood")

        rentalDf.show(1000)

      }
      catch{
        case e:Exception=>
          println(e)
      }
    }

  }


