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

    public AirQualityConsumer(String group_id, String offset_reset, String auto_commit) {
        super(TOPIC,FILEPATH, FILENAME, group_id, offset_reset, auto_commit);
    }

    @Override
    protected void storeData(HdfsWriter csv_writer, String filepath, String filename, DateTimeFormatter formatter, LocalDateTime date) throws IOException {

        // Init Hadoop outputStream
        FSDataOutputStream outputStream_QUANTITY = csv_writer.createFileAndOutputStream(filepath, filename + "-QUANTITY-" + ".csv");
        FSDataOutputStream outputStream_CONCENTRATION = csv_writer.createFileAndOutputStream(filepath, filename + "-CONCENTRATION-" + ".csv");

        // While we're still at today
        while (LocalDateTime.now().isBefore(LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), date.getHour()+1, date.getMinute()))) {
            ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(2000));
            for (ConsumerRecord<String, String> record : records) {
                switch (record.key()) {
                    case "Quantity":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStream_QUANTITY);
                        this.consumer.commitSync();
                        break;
                    case "Concentration":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStream_CONCENTRATION);
                        this.consumer.commitSync();
                        break;
                }
            }
        }
        outputStream_QUANTITY.close();
        outputStream_CONCENTRATION.close();
    }
}
