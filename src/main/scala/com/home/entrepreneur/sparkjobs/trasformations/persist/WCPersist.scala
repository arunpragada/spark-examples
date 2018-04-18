package scala.com.home.entrepreneur.sparkjobs.trasformations.persist

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.storage.StorageLevel

object WCPersist {
  
  val log = LogManager.getRootLogger
  
  def main(args: Array[String]) = {
    println("from  "+args.length)
    
    val conf = new SparkConf()
    
    val sc = new SparkContext(conf)
    println("App name is -->> "+sc.getConf.get("spark.app.name"))
    log.info("App name is -->> "+sc.getConf.get("spark.app.name"))
    
    var inputFile = "/media/enter/Storage1/workspace/SparkDen/pom.xml"
    if(args.length > 0)
      inputFile = args(0)
        
    val inputLines = sc.textFile(inputFile)
    val filterWordsRDD = inputLines.filter(line => line.contains("Spark")) //transformation
    
    //val defaultPersist = filterWordsRDD.persist() //persists memory
    //println("Saved Default WC  --------->>>>>>>> "+defaultPersist)
    val persist2 = filterWordsRDD.persist(StorageLevel.MEMORY_AND_DISK)
    println("Saved Persist2 WC  --------->>>>>>>> "+persist2)
    
    filterWordsRDD.unpersist(true)
  }
}