package inutile;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CameraProducer {


    public CameraProducer() {

    }

    public void start_streaming() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<String, String>("test", "1", "Neyla x Ridha"));
        producer.close();
    }
}
