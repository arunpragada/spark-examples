package scala.com.home.entrepreneur.sparkjobs.trasformations.mappartitions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.storage.StorageLevel

object WCMapPartitions {

  val log = LogManager.getRootLogger

  def main(args: Array[String]) = {
    println("from  " + args.length)

    val conf = new SparkConf()

    val sc = new SparkContext(conf)
    println("App name is -->> " + sc.getConf.get("spark.app.name"))
    log.info("App name is -->> " + sc.getConf.get("spark.app.name"))

    val data = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)

    println("output ---------------->>>>>>>> ")
    //data.map(sumfuncmap).collect()
    //data.mapPartitions(sumfuncpartition).collect
    println("--->> "+data.mapPartitionsWithIndex(sumfuncpartitionIndex).collect().mkString(","))

  }

  def sumfuncmap(numbers: Int): Int = {
    var sum = 1

    var total: Int = sum + numbers
    println("in sumfuncmap ---------------->>>>>>>> " + total)

    return sum + numbers
  }

  def sumfuncpartition(numbers: Iterator[Int]): Iterator[Int] =
    {
      var sum = 1
      while (numbers.hasNext) {
        sum = sum + numbers.next()
      }
      println("in sumfuncpartition ---------------->>>>>>>> "+sum)
      return Iterator(sum)
    }
  
  def sumfuncpartitionIndex(index: Int, numbers: Iterator[Int]): Iterator[Int] = {
    
      numbers.map(x => index)
    }

}