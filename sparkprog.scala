//ERROR FACED: the file paths had spaces in it or were incomplete
package org.sparkbasics.sparkcore
import org.apache.spark.sql.SparkSession
import org.apache.spark._
object sparkprog 
{
  
def main (args:Array[String])
{
      //val spark = SparkSession.builder().getOrCreate()
      val sparkconf = new SparkConf().setMaster("local[*]").setAppName("SparkCorePractice")
      val sc= new SparkContext(sparkconf)
      sc.setLogLevel("ERROR")
      println("**********PRINTING ALL THE RDD'S**********")
      
      println("\nHADOOPLINES\n")
      val hadooplines = sc.textFile("hdfs://localhost:54310/user/hduser/empdata.txt")
      hadooplines.foreach(println)
      
      println("\nLINES\n")
      var lines = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
      lines.foreach(println)
      
      println("\nALLLINES\n")
      val alllines = sc.textFile("file:/home/hduser/sparkdata/*data.txt")
      alllines.foreach(println)
      //print hadoplines, lines and alllines
      
      println("\nUSING FILTER ON LINES TO GET FILTERLINES\n")
      val filterlines = lines.filter{ l => l.length > 37}
      filterlines.foreach(println)
      // print filterlines
      
      filterlines.cache
      println("\nFILTERLINES IS CACHED\n")
      println("\nOUTPUT OF FILTERLINES.COUNT\n")
      println(filterlines.count)
      
      
      var lengths = lines map { l => l.length}
      println("\nPRINTING : lengths = lines map { l => l.length}\nPRINTS THE NUMBER OF CHARACTERS IN A RECORD\n")
      lengths.foreach(println)
      
      
      lengths = {lines.map(x=>x.split(",")).map(l => l.length)}
      println("\nCOUNTS THE NUMBER OF COLUMNS IN EACH ROW AFTER SPLITING BASED ON COMMA DELIMITER\n")
      lengths.foreach(println)

      val chennaiLines = lines.map(x=>x.split(",")).filter(l => l(1).toUpperCase=="CHENNAI")
      println("\nUSING COLLECT FUNCTION ON CHENNAILINES AFTER FILTERING THE ROWS WITH CHENNAI IN COLUMN l(1)\n")
      val x=chennaiLines.collect
      println(x.map(_.mkString(" ")).mkString("\n"))

      val fmrdd = lines.flatMap( l => l.split(",")).map(x=>x.toString.toUpperCase)
      println("\nFLATMAP ON LINES SPLIT BY , AND ALL OF THE DATA IS CONVERTED TO UPPERCASE\n")
      fmrdd.foreach(println)

      val lengths2 = lines.mapPartitions ( x => x.filter( l => l.length>20))
      println("\nlengths2 APPLIES MAPPARTITIONS ON lines AND FILTERS IF ROW LENGTH IS GREATER THAN 20\n")
      lengths2.foreach(println)
 
      val linesFile1 = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
      val linesFile2 = sc.textFile("file:/home/hduser/sparkdata/empdata1.txt")
      val linesFromBothFiles = linesFile1.union(linesFile2)
      println("\nDATA IN empdata.txt file\n")
      linesFile1.foreach(println)
      println("\nDATA IN empdata1.txt file\n")
      linesFile2.foreach(println)
      println("\nUNION FUNCTION ON empdata.txt and empdata1.txt files\n")
      linesFromBothFiles.foreach(println)

      val uniqueNumbers = linesFromBothFiles.distinct
      println("\nPRINT DISTINCT OF RDD linesFromBothFiles that is UNION OF empdta.txt and empdata1.txt\n")
      uniqueNumbers.foreach(println)

      val ziplines = linesFile1.zip(linesFile1)
      println("\nPRINTING linesFile1 (empdata.txt) zipped with itself\n")
      ziplines.foreach(println)

      case class Customer(name: String, city: String, age: Int)
      val customers = lines.map(x=>x.split(",")).map(x=>Customer(x(0), x(1), x(2).toInt))
      println("\nAPPLY Customer CASE CLASS ON lines RDD\n")
      val y = customers.collect
      y.foreach(println)

      val groupByZip = customers.groupBy { a => a.city}
      println("\nGROUPBYZIP : To group rows based on city\n")
      val z= groupByZip.collect
      z.foreach(println)
      
      println("\nPARTITION HANDLING USING PARALLELIZE AND COALESCE\n")
      val numbers = sc.parallelize((1 to 100).toList,5)
      
      //To see the number of partitions
      println("\nnumbers.partitions.size RETURNS THE NUMBER OF PARTITIONS\n")
      println(numbers.partitions.size)
      
      println("\nGLOM FUNCTION GIVES YOU AN ARRAY OF ARRAYS EVIDENTLY SHOWING YOU THE PARTITIONS:\n")
      val q=numbers.glom().collect
      println(q.map(_.mkString(" ")).mkString("\n"))
      
      //Coalesce reduced the partition number to 2 from 5
      val numbersWithTwoPartition = numbers.coalesce(2)
      println("\ncoalescing to 2 from 5\n")
      val z5=numbersWithTwoPartition.glom().collect
      println(z5.map(_.mkString(" ")).mkString("\n"))
      
      //Repartition :
      //The repartition method takes an integer as input and returns an RDD with specified number of
      //partitions. It is useful for increasing parallelism. It redistributes data, so it is an expensive operation.
      val numbersWithFourPartition = numbers.repartition(6)
      println("\nSEE NUMBER OF PARTITIONS after repartition to 6 from 2\n")
      println(numbersWithFourPartition.partitions.size)
      println("\nGLOM FUNCTION:\n")
      val z4= numbersWithFourPartition.glom().collect
      println(z4.map(_.mkString(" ")).mkString("\n"))

      val sortedByAge = customers sortBy( p => p.age, true)
      println("\nSORT CUSTOMERS BY AGE COLUMN IN ASCENDING ORDER\n")
      sortedByAge.foreach(println)

      println("\nJOIN FUNCTION\n")
      val pairRdd1 = sc.parallelize(List(("a", 1), ("b",2), ("c",3)))
      val pairRdd2 = sc.parallelize(List(("b", "second"), ("c","third"), ("d","fourth")))
      val joinRdd = pairRdd1.join(pairRdd2)
      println("\nJOIN FUNCTION TO GIVE joinRdd=pairRdd1.join(pairRdd2)\n")
      joinRdd.foreach(println)
      val joinRdd2 = pairRdd1.rightOuterJoin(pairRdd2)
      println("\nRIGHT OUTER JOIN FUNCTION TO GIVE joinRdd2=pairRdd1.rightOuterJoin(pairRdd2)\n")
      joinRdd2.foreach(println)
      val joinRdd3 = pairRdd1.fullOuterJoin(pairRdd2)
      println("\nFULL OUTER JOIN FUNCTION TO GIVE joinRdd3=pairRdd1.fullOuterJoin(pairRdd2)\n")
      joinRdd3.foreach(println)
      val joinRdd4 = pairRdd1.leftOuterJoin(pairRdd2)
      println("\nLEFT OUTER JOIN FUNCTION TO GIVE joinRdd4=pairRdd1.leftOuterJoin(pairRdd2)\n")
      joinRdd4.foreach(println)

      println("\nReduceByKey FUNCTION\n")
      val pairRdd = sc.parallelize(List(("a", 1), ("b",2), ("c",3), ("a", 11), ("b",22), ("a",111)))
      val sumByKeyRdd = pairRdd.reduceByKey((x,y) => x+y)
      val minByKeyRdd = pairRdd.reduceByKey((x,y) => if (x < y) x else y)
      println("\nsum found by ReduceByKey:\n")
      val z2= sumByKeyRdd.collect
      z2.foreach(println)
      println("\nmin found by ReduceByKey:\n")
      val z3= minByKeyRdd.collect
      z3.foreach(println)

      sc.setCheckpointDir("/tmp/ckptdir")
      val ckptrdd = sc.parallelize(1 to 4)
      ckptrdd.checkpoint
      println("\nprinting the number of checkpoints:")
      println(ckptrdd.count)
      println("\nHDFS:\n")
      sc.setCheckpointDir("hdfs://localhost:54310/tmp/ckptdir")

      
      val rdd = sc.parallelize((1 to 10000).toList)
      val filteredRdd = rdd filter { x => (x % 1000) == 0}
      val filterResult = filteredRdd.collect
      println("\ncollect method\n")
      filterResult.foreach(println)
      println("\nThe count method returns a count of the elements in the source RDD.\n")
      val RDD = sc.parallelize((1 to 10000).toList)
      val total = RDD.count
      println(total)
      
      val rdd2 = sc.parallelize(List(1, 2, 3, 4, 1, 2, 3, 1, 2, 1))
      val counts = rdd2.countByValue
      
      
      println("\nCOUNTS")
      println(counts)
      println ("\nreduce function to find product")
      val numbersRdd = sc.parallelize(List(2, 5, 3, 1))
      val sum = numbersRdd.reduce ((x, y) => x + y)
      val product = numbersRdd.reduce((x, y) => x * y)
      println(product)
      
      
      println("\ncountByKey")
      val pairRDD = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("a", 11), ("b", 22), ("a", 1)))
      val countOfEachKey = pairRDD.countByKey
      countOfEachKey.foreach(println)
      println("\nlookup method")
      val pairRDD1 = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("a", 11), ("b", 22), ("a",1)))
      val values = pairRDD1.lookup("a")
      
      
      println("\nSaveAsTextFile :")
      val logs = sc.textFile("file:/usr/local/hadoop/logs/hadoop-hduser-datanode-Inceptez.log")
      val errorsAndWarnings = logs filter { l => l.contains("ERROR") || l.contains("WARN")}
      val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:54310"),sc.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path("/user/hduser/errorsAndWarnings"),true)
      errorsAndWarnings.saveAsTextFile("hdfs://localhost:54310/user/hduser/errorsAndWarnings")
      println("\ncache errorsAndWarnings")
      errorsAndWarnings.cache()
      val errorLogs = errorsAndWarnings filter { l => l.contains("ERROR")}
      val warningLogs = errorsAndWarnings filter { l => l.contains("WARN")}
      val errorCount = errorLogs.count
      val warningCount = warningLogs.count
      println("\nerror count")
      println(errorCount)
      println("\nwarning count")
      println(warningCount)

      println("\nPersist/UnPersist :")

      val line = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
      line.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)
      line.unpersist()
      line.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY_2)
      line.unpersist()
      line.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_2)
      line.unpersist()
      line.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
      line.unpersist()
      line.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER_2)
      
      line.getStorageLevel.description
      
      val input = sc.parallelize(List(1, 2, 3))
      val broadcastVar = sc.broadcast(2)
      val added = input.map(x => broadcastVar.value + x)
      added.foreach(println)
      val multiplied = input.map(x => broadcastVar.value * x)
      multiplied.foreach(println)
      println("accumulators")
      val accum = sc.accumulator(0,"IZ Accumulator")
      sc.parallelize(Array(1, 2, 3)).foreach(x => accum += x)
      println(accum.value)

      
      val linesFILE1 = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
      val linesFILE2 = sc.textFile("file:/home/hduser/sparkdata/empdata1.txt")
      val linesPresentInBothFiles = linesFILE1.intersection(linesFILE2)
      println("\nintersection function")
      linesPresentInBothFiles.foreach(println)
      println("\nSubtract:")
      
      val linesInFile1Only = linesFILE1.subtract(linesFILE2)
      linesInFile1Only.foreach(println)
      
      println("\ncartesian product")
      val number = sc.parallelize(List(1, 2, 3, 4))
      val alphabets = sc.parallelize(List("a", "b", "c", "d"))
      val cartesianProduct = number.cartesian(alphabets)
      cartesianProduct.foreach(println)
      
      println("\nfirst to return first element in rdd")
      val rddX = sc.parallelize(List(10, 5, 3, 1))
      val firstElement = rddX.first
      println(firstElement)
      
      println("\nThe max method returns the largest element in an RDD.")
      val rddXX = sc.parallelize(List(2, 5, 3, 1))
      val maxElement =rddXX.max
      println(maxElement)
      
      println("\nThe min method returns the smallest element in an rdd")
      val rddXXX = sc.parallelize(List(2, 5, 3, 1))
      val minElement = rddXXX.min
      println(minElement)
      
      println("\nThe take method takes an integer N as input and returns an array containing the first N element in the source RDD.")
      val rddXXXX = sc.parallelize(List(2, 5, 3, 1, 50,100))
      val first3 = rddXXXX.take(3) 
      first3.foreach(println)
      
      println("\nThe takeOrdered method takes an integer N as input and returns an array containing the N smallest elements in the source RDD.")
      val rddXXXXX = sc.parallelize(List(2, 5, 3, 1, 50,100)) 
      val smallest3 = rddXXXXX.takeOrdered(3)
      smallest3.foreach(println)
      
      println("\nThe top method takes an integer N as input and returns an array containing the N largest elements in the source RDD.")
      val rddXXXXXX = sc.parallelize(List(2, 5, 3, 1, 50, 100))
      val largest3 = rddXXXXXX.top(3)
      largest3.foreach(println)
      
      println("\nusing fold function to find sum and product")
      val numbersRDD = sc.parallelize(List(2, 5, 3, 1))
      val su = numbersRDD.fold(0) ((partialSum, x) => partialSum + x)
      
      val pro = numbersRdd.fold(1) ((partialProduct, x) => partialProduct * x)
      println("\nsum")
      println(su)
      println("\nproduct")
      println(pro)
      println("\nmean")
      val numbersRDD1 = sc.parallelize(List(2, 5, 3, 1))
      val mean =numbersRDD1.mean 
      println(mean)
      
      println("\nStdev :")
      val numbersRDD2 = sc.parallelize(List(2, 5, 3,1)) 
      val stdev = numbersRDD2.stdev
      println(stdev)
      
      println("\nsum")
      val numbersRDD3 = sc.parallelize(List(2, 5, 3, 1))
      val s = numbersRDD3.sum
      println(s)
            
      println("\nThe variance method returns the variance of the elements in the source RDD.")
      val numbersRDD4 = sc.parallelize(List(2, 5, 3, 1))
      val variance = numbersRDD4.variance
      println(variance)
    }
}