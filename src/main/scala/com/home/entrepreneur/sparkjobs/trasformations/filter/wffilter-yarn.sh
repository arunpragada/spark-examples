cd /media/enter/Storage1/BigData-Den/TechStack/spark-2.0.2 
spark-submit \
--class "scala.com.home.entrepreneur.sparkjobs.trasformations.filter.WCFilter" \
--name "WCFilterYarn" \
--deploy-mode cluster \
--master yarn /media/enter/Storage1/workspace/SparkDen/target/SparkDen-1.0.jar \
/sparkjobs/input/README.md

