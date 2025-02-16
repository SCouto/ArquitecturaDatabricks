//Run this in one terminal

nc -lk 9999


//Streaming => Run this in the spark shell in another terminal
import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(5))

val linesDS = ssc.socketTextStream("localhost", 9999) 

val wordCounts = linesDS
            .flatMap(_.split(" "))
            .map(word => (word, 1))
           .reduceByKey(_ + _ )

wordCounts.print

ssc.start()
ssc.awaitTerminationOrTimeout(10)




//Structured Streaming => Run this in the spark shell in another terminal
val lines = spark.readStream
 	.format("socket")
 	.option("host", "localhost")
 	.option("port", 9998)
 .load().as[String]

val wordCounts = lines
            .flatMap(_.split(" "))
            .groupBy($"value")
           .count


val query = wordCounts.writeStream
 .outputMode("complete")
 .format("console")
 .start()

