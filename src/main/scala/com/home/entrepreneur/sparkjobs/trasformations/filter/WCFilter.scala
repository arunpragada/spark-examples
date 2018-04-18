package scala.com.home.entrepreneur.sparkjobs.trasformations.filter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager

object WCFilter {
  
  val log = LogManager.getRootLogger
  
  def main(args: Array[String]) = {
    println("from  "+args.length)
    
    val conf = new SparkConf()
    //conf.setMaster("local")  //if ran from IDE
    
    val sc = new SparkContext(conf)
    println("App name is -->> "+sc.getConf.get("spark.app.name"))
    log.info("App name is -->> "+sc.getConf.get("spark.app.name"))
    
    var inputFile = "/media/enter/Storage1/workspace/SparkDen/pom.xml"
    if(args.length > 0)
      inputFile = args(0)
        
    log.info("Counting words for the input file -->>  "+inputFile)
    val inputLines = sc.textFile(inputFile)
    val filterWordsRDD = inputLines.filter(line => line.contains("Spark")) //transformation
    println(filterWordsRDD.count()+"  --------------->>>>> \n") //action
    
    val mapFile = inputLines.flatMap(lines => lines.split(" "))/*.filter(value => value=="spark")*/
    println(mapFile.count()+"  --------------->>>>> \n")
    for(w <- mapFile){
      println(w)
    }
    
    val mc = mapFile.filter(value => value=="spark")
    println("----------wc   "+mc.count())
    
    val mapFile1 = inputLines.flatMap(lines => lines.split(" ")).filter(value => value.contains("spark"))
    println("last  "+mapFile1.count())
  }
}