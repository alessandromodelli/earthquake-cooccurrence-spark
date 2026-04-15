import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object EarthquakeCooccurences {

  type Cell = (Double, Double)
  type DateStr = String
  type CellPair = (Cell, Cell)

  def main(args: Array[String]): Unit = {

    //Configuration for the parameters
    if(args.length < 2){
      System.err.println("Input Field Error: EarthquakeCooccurences <input-data-path> <num-partitions>")
      System.exit(1)
    }

    val inputDataPath = args(0)
    val numPartitions = args(1).toInt

    println(s"$inputDataPath - $numPartitions")

    // In locale: spark.master=local[*] viene passato via javaOptions in build.sbt
    // Su DataProc: spark.master viene impostato automaticamente dal cluster
    //val masterUrl = sys.props.getOrElse("spark.master", "local[*]")
    val spark = SparkSession.builder
      .appName("Earthquake Co-occurrence Analysis")
      //.master(masterUrl)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()

    // LOAD CSV AS RDD
    val rawData = spark.read
      .option("header", value = true)
      .csv(inputDataPath)
      //.limit(50000)    //Add limit for local testing
      .rdd

    // FORMAT RAW DATA
    // Input data: longitude, latitude, date
    //  - Round lat/lng to 1 decimal
    //  - Truncate date timestamp to YYYY-MM-DD
    // Output data: RDD[(Cell, DateStr)]
    val formattedData: org.apache.spark.rdd.RDD[(Cell, DateStr)] = rawData
      .flatMap{
        row => try{
          val lat = row.getAs[String]("latitude").toDouble
          val lng = row.getAs[String]("longitude").toDouble
          val date = row.getAs[String]("date")

          //Round lat,lng to 1 decimal
          val roundedLat = BigDecimal(lat).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble //math.round(lat * 10.0) / 10.0
          val roundedLng =  BigDecimal(lng).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble //math.round(lng * 10.0) / 10.0

          //Truncate timestamp and keep only YYYY-MM-DD (first 10 characters)
          val truncatedDate = date.trim.take(10)

          Some(((roundedLat, roundedLng),truncatedDate))
        } catch {
          case _: Exception => None
        }
      }

    println("--- Formatted Data (lat, lng, date) ---")

    // REMOVE DUPLICATES AND GROUP EVENTS BY DATE (1 shuffle)
    //  - Inverts data structure so that the date becomes the key (Cell, Date) => (Date, Cell).
    //    Necessary because in Spark the shuffle is based on the key, so all the occurrences happened in the same day must go to the same worker
    //  - aggregateByKey with Set as zero-value (a Set for each date so that it automatically removes duplicates, avoiding another shuffle from .distinct()):
    //      seqOp  = add cell to the local Set in the mapper before the shuffle (removes duplicates)
    //      combOp = combines the Set from different partitions inside the reducer post shuffle
    //    For each worker, before shuffling, the rows relative to each date get into the set (duplicates are removed instantly)
    //    After the shuffle the sets are combined together
    //   Output: RDD[(DateStr, Set[Cell])]
    val cellsByDate: org.apache.spark.rdd.RDD[(DateStr, Set[Cell])] =
      formattedData
        .map { case (cell, date) => (date, cell) }
        .aggregateByKey(Set.empty[Cell], numPartitions)(
          seqOp  = (cellSet, cell) => cellSet + cell,
          combOp = (set1, set2)   => set1 | set2
        )

    // FILTERS DATES WITH LESS THAN 2 CELLS
    // A single cell cannot make a pair, so it is useless
    val filteredCellsByDate = cellsByDate.filter(_._2.size >= 2)
    filteredCellsByDate.cache() //Needed later for the dates retrieval so that Spark does not have to read the entire CSV again

    println("\n--- Getting pairs ---")

    // GENERATES PAIRS WITH CO-OCCURRENCE COUNT
    // For each Date with N distinct cells, generates N*(N-1)/2 pairs with canonical ordering.
    // Counts co-occurrences with Long so that it shuffles only numbers instead of date strings
    val pairCounts: org.apache.spark.rdd.RDD[(CellPair, Long)] =
      filteredCellsByDate.flatMap { case (date, cells) =>
        val cellArray = cells.toArray
        val n = cellArray.length
        val out = new Array[(CellPair, Long)](n * (n - 1) / 2)
        var idx = 0
        var i = 0
        while (i < n) {
          var j = i + 1
          while (j < n) {
            val a = cellArray(i)
            val b = cellArray(j)
            val pair = if (a._1 < b._1 || (a._1 == b._1 && a._2 <= b._2)) (a, b) else (b, a)
            out(idx) = (pair, 1L)
            idx += 1
            j += 1
          }
          i += 1
        }
        out
      }.reduceByKey(_ + _, numPartitions) // Somma dei conteggi parziali su ciascuna partizione 

    // FIND THE PAIR WITH THE MAX COUNT OF CO-OCCURRENCES
    println("\n--- Finding the pair with highest cooccurence ---")
    val (bestPair, maxCount) = pairCounts.reduce((a, b) => if (a._2 >= b._2) a else b)

    // RETRIEVES AND SORTS THE DATES OF THE PAIR
    val sortedDates: Array[String] = filteredCellsByDate
      .filter { case (_, cellSet) =>
        cellSet.contains(bestPair._1) && cellSet.contains(bestPair._2)
      }
      .map(_._1) // Estraiamo solo la DateStr
      .collect()   // Portiamo al driver (sono poche stringhe ormai)
      .sorted      // Ordiniamo localmente

    println("\n--- RESULTS ---")

    sortedDates.foreach(println)
    println(s"Cell Pair: (${bestPair._1}, ${bestPair._2})")
    println(s"Total co-occurrences: $maxCount")

    println(s"Elapsed time: ${(System.currentTimeMillis() - startTime) / 1000.0} s")

    spark.stop()
  }

}