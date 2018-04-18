package scala.com.home.entrepreneur.sparkjobs.trasformations.map

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager

object WCMap {

  val log = LogManager.getRootLogger

  def main(args: Array[String]) = {
    println("from  " + args.length)

    val conf = new SparkConf()
    //conf.setMaster("local")  //if ran from IDE

    val sc = new SparkContext(conf)
    println("App name is -->> " + sc.getConf.get("spark.app.name"))
    log.info("App name is -->> " + sc.getConf.get("spark.app.name"))

    var inputFile = "/media/enter/Storage1/workspace/SparkDen/pom.xml"
    if (args.length > 0)
      inputFile = args(0)

    log.info("Counting words for the input file -->>  " + inputFile)
    val inputLines = sc.textFile(inputFile)
    val mapFile = inputLines.map(line => (line, line.length))/*.collect()*/
    mapFile.foreach(println)

    val linesRDD = sc.textFile(inputFile) //create an RDD called lines
    val lowerCaseWords = linesRDD.map(line => line.toLowerCase())
    val lowerCaseRDD = lowerCaseWords.collect() //applies transformation -- action

    //lowerCaseRDD.foreach(println) //print each element in RDD

    val LinessArrayRDD = linesRDD.map(line => line.split(" ")).collect() //gives array of words

    for (wordsArray <- LinessArrayRDD) { // Iterate through array of wrods
      for (word <- wordsArray)
        println("Map --->  " + word)
      print()
    }
    
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    println("Squared --------->>>    "+result.collect().mkString(","))
    
    val lines = sc.parallelize(List("hierotio q w","h u uuu oo "))
    val mapLines = lines.map(line => line.split("\n"))
    val flatMapLines = lines.flatMap(line => line.split("\n"))
    println("mapLines--------->>>>>  "+mapLines.first())  // [Ljava.lang.String;@62cba181
    println("flatMapLines----->>>>>  "+flatMapLines.collect()) // hierotio q w 
    
  }
}