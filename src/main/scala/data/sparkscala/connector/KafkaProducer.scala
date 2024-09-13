package data.sparkscala.connector

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import java.util.Properties
object KafkaProducer {
  def main(args: Array[String]): Unit = {

    //Server Config

    val props = new Properties()
    props.put("bootstrap.servers", "192.168.1.128:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")


    //Topic Config
    val topic = "text_topic"

    val producer = producerCreation(props, topic)

    try {

      for (i <- Range(0, 15)) {

        val record = new ProducerRecord[String, String](topic, i.toString, "Omkar Nanaware " + i)

        val metadata = producer.send(record)
        printf(s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(), metadata.get().partition(),
          metadata.get().offset())
      }

    }catch
      {
        case e: Exception => e.printStackTrace()
      }
      finally
      {
        producer.close()
      }
    }



  def producerCreation(props: java.util.Properties, topic: String): KafkaProducer[String, String] = {


    val producer = new KafkaProducer[String, String](props)

    producer

  }
}

