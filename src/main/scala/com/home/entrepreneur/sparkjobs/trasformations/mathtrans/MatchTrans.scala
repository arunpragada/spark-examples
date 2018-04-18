package scala.com.home.entrepreneur.sparkjobs.trasformations.mathtrans

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

object MatchTrans {

  val log = LogManager.getRootLogger

  def main(args: Array[String]) = {
    println("from  " + args.length)

    val conf = new SparkConf()

    val sc = new SparkContext(conf)
    println("App name is -->> " + sc.getConf.get("spark.app.name"))
    log.info("App name is -->> " + sc.getConf.get("spark.app.name"))

    val rdd1 = sc.parallelize(Seq((1, "jan", 2016), (3, "nov", 2014), (16, "feb", 2014)))
    val rdd2 = sc.parallelize(Seq((5, "dec", 2014), (17, "sep", 2015)))
    val rdd3 = sc.parallelize(Seq((6, "dec", 2011), (16, "may", 2015)))

    //unionTrans(rdd1, rdd1, rdd3, sc)
    intersectTrans(rdd1, rdd1, rdd3, sc)
  }

  def unionTrans(rdd1: RDD[(Int, String, Int)], rdd2: RDD[(Int, String, Int)],
                 rdd3: RDD[(Int, String, Int)], sc: SparkContext): Unit = {
    println("from unionTrans")

    val rddUnion = rdd1.union(rdd2)
    println("printing  ---->>>>>>>>   ")
    rddUnion.foreach(println)

    val rddAllUnion = rdd1.union(rdd2).union(rdd3)

    for (w <- rddAllUnion) {
      println("data   -->>  " + w)
    }

    val u1 = sc.parallelize(List("c", "c", "p", "m", "t"))
    val u2 = sc.parallelize(List("c", "m", "k"))
    val result = u1.union(u2).distinct()
    println("All -----")
    result.foreach { println }
  }

  def intersectTrans(rdd1: RDD[(Int, String, Int)], rdd2: RDD[(Int, String, Int)],
                     rdd3: RDD[(Int, String, Int)], sc: SparkContext): Unit = {
    println("from intersectTrans")

    val rddIntersect = rdd1.intersection(rdd2)
    println("printing  ---->>>>>>>>   ")
    rddIntersect.foreach(println)

    val rddAllIntersect = rdd1.union(rdd2).union(rdd3)

    for (w <- rddAllIntersect) {
      println("data   -->>  " + w)
    }

    val u1 = sc.parallelize(List("c", "c", "p", "m", "t"))
    val u2 = sc.parallelize(List("c", "m", "k"))
    val result = u1.intersection(u2).distinct()
    println("All -----")
    result.foreach { println }
  }
}