
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.SomeColumns


object CassandraTest {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    val conf = new SparkConf();
    conf.set("spark.cassandra.connection.host","127.0.0.1")
    conf.setMaster("local[*]")
    conf.setAppName("Cassandra")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(5))
    
    //To read from local kafka
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = List("testtopic").toSet
    
    val lines = KafkaUtils.createDirectStream[
     String, String, StringDecoder, StringDecoder](
     ssc, kafkaParams, topics)
     
     //If you type "test" in kafka producer, below line will save "key" as null in question followed by value as "test" in id.
     lines.saveToCassandra("test", "test", SomeColumns("question", "id"))
     lines.print()
     
     ssc.start()
     ssc.awaitTermination()
  }
  
}