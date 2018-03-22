# Spark_Scala_WordCount

pom.xml -> include spark and scala dependecies, plugins... for maven build to jar

bin/spark-submit --class GetSrcIp 
                --num-executors 8  
                ~/hong/spark_getsrcip/target/GetSrcIp-1.0.jar 
                hdfs:///user/hpds/getsrcipfilter1 
                hdfs:///user/hpds/sparkscala_excutors11_getsrcip
