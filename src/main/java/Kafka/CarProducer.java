package Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class CarProducer {

    private static volatile Properties props;
    private static volatile Producer<String, String> producer;

    private static Properties getProperties(){
        if (props == null){
            props = new Properties();
            props.put("bootstrap.servers", "quickstart.cloudera:9092");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        }
        return props;
    }

    private static Producer<String, String> getProducer() {
        if (producer == null){
            getProperties();
            producer = new KafkaProducer<>(props);
        }
        return producer;
    }

    public static void sendRecord(String topic, String record){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy-HH-mm-ss");
        LocalDateTime date = LocalDateTime.now();
        getProducer().send(new ProducerRecord<String, String>(topic, formatter.format(date), record));
    }
}