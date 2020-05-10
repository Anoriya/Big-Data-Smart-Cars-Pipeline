package Kafka;

import Hdfs.HdfsWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public abstract class AbstractConsumer {

    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final Consumer<String, String> consumer;

    protected AbstractConsumer(String group_id, String offset_reset, String auto_commit) {
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset_reset);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, auto_commit);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Create the consumer using props.
        this.consumer = new KafkaConsumer<>(props);
    }

    protected void runConsumer(String topic, String filepath, String filename) throws IOException {
        this.consumer.subscribe(Collections.singletonList(topic));


        // Init Hadoop writer
        HdfsWriter csv_writer = new HdfsWriter();
        FSDataOutputStream outputStream = csv_writer.createFileAndOutputStream(filepath, filename);

        // While the csv did not attend the required size
        while ((outputStream.size() / 1024) / 1024 < 5) {
            ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                csv_writer.writeLineIntoOutputStream(record.value(), outputStream);
                this.consumer.commitSync();
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
        outputStream.close();
        csv_writer.closeFileSystem();
        System.out.println("Done");
    }

}
