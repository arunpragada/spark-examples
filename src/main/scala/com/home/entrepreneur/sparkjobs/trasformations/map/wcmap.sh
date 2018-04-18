cd /media/enter/Storage1/BigData-Den/TechStack/spark-2.0.2 
spark-submit \
--class "scala.com.home.entrepreneur.sparkjobs.trasformations.map.WCMap" \
--name "wc map" \
--deploy-mode client \
--master local /media/enter/Storage1/workspace/SparkDen/target/SparkDen-1.0.jar \
/sparkjobs/input/README.md

