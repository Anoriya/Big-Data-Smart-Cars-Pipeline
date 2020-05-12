package Kafka;

import Hdfs.HdfsWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;

public abstract class AbstractConsumer implements Runnable {

    private final static String BOOTSTRAP_SERVERS = "quickstart.cloudera:9092";
    protected final Consumer<String, String> consumer;
    protected String topic, filepath, filename;

    protected AbstractConsumer(String topic, String filepath, String filename, String group_id, String offset_reset, String auto_commit) {
        this.topic = topic;
        this.filename = filename;
        this.filepath = filepath;
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

    public void run() {
        try {
            this.runConsumer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void runConsumer() throws IOException {
        this.consumer.subscribe(Collections.singletonList(topic));

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy-HH-mm-ss");
        while (true) {
            // Init Hadoop writer
            HdfsWriter csv_writer = new HdfsWriter();
            // Init Date
            LocalDateTime date = LocalDateTime.now();

            this.storeData(csv_writer, filepath, filename, formatter, date);

            csv_writer.closeFileSystem();
            System.out.println("Done");
        }
    }

    protected void storeData(HdfsWriter csv_writer, String filepath, String filename, DateTimeFormatter formatter, LocalDateTime date) throws IOException {
        // Init Hadoop outputStream
        FSDataOutputStream outputStream = csv_writer.createFileAndOutputStream(filepath,
                filename + formatter.format(date) + ".csv");
        // While we're still at today
        while (LocalDateTime.now().isBefore(LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), date.getHour(), date.getMinute() + 1))) {
            ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key());
                csv_writer.writeLineIntoOutputStream(record.value(), outputStream);
                this.consumer.commitSync();
            }
        }
        outputStream.close();
    }

}
