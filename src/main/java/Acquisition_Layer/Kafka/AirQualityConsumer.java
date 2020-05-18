package Acquisition_Layer.Kafka;

import Hdfs.HdfsWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class AirQualityConsumer extends AbstractConsumer {

    private static final String TOPIC = "AirQuality";
    private static final String FILEPATH = "/user/hdfs/AirQuality/";
    private static final String FILENAME = "test";

    protected AirQualityConsumer(String group_id, String offset_reset, String auto_commit) {
        super(TOPIC,FILEPATH, FILENAME, group_id, offset_reset, auto_commit);
    }

    @Override
    protected void storeData(HdfsWriter csv_writer, String filepath, String filename, DateTimeFormatter formatter, LocalDateTime date) throws IOException {

        // Init Hadoop outputStream
        FSDataOutputStream outputStream_SGP = csv_writer.createFileAndOutputStream(filepath, filename + "-SGP-" + formatter.format(date) + ".csv");
        FSDataOutputStream outputStream_SPS = csv_writer.createFileAndOutputStream(filepath, filename + "-SPS-" + formatter.format(date) + ".csv");

        // While we're still at today
        while (LocalDateTime.now().isBefore(LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), 23, 59))) {
            ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                switch (record.key()) {
                    case "SGP":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStream_SGP);
                        this.consumer.commitSync();
                        break;
                    case "SPS":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStream_SPS);
                        this.consumer.commitSync();
                        break;

                }
            }
        }
        outputStream_SGP.close();
        outputStream_SPS.close();

    }
}
