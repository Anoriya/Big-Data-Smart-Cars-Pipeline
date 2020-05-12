package Kafka;

import Hdfs.HdfsWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ZephyrConsumer extends AbstractConsumer {

    private static final String TOPIC = "Zephyr";
    private static final String FILEPATH = "/user/hdfs/Zephyr/";
    private static final String FILENAME = "test";

    public ZephyrConsumer(String group_id, String offset_reset, String auto_commit) {
        super(TOPIC, FILEPATH, FILENAME, group_id, offset_reset, auto_commit);
    }

    @Override
    protected void storeData(HdfsWriter csv_writer, String filepath, String filename, DateTimeFormatter formatter, LocalDateTime date) throws IOException {

        // Init Hadoop outputStream
        FSDataOutputStream outputStream_BR_RR = csv_writer.createFileAndOutputStream(filepath, filename + "-BR_RR-" + formatter.format(date) + ".csv");
        FSDataOutputStream outputStream_ECG = csv_writer.createFileAndOutputStream(filepath, filename + "-ECG-" + formatter.format(date) + ".csv");
        FSDataOutputStream outputStream_Event_Data = csv_writer.createFileAndOutputStream(filepath, filename + "-Event_Data-" + formatter.format(date) + ".csv");
        FSDataOutputStream outputStream_General = csv_writer.createFileAndOutputStream(filepath, filename + "-General-" + formatter.format(date) + ".csv");

        // While we're still at today
        while (LocalDateTime.now().isBefore(LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), date.getHour(), date.getMinute() + 1))) {
            ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key());
                switch (record.key()) {
                    case "BR_RR":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStream_BR_RR);
                        this.consumer.commitSync();
                        break;
                    case "ECG":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStream_ECG);
                        this.consumer.commitSync();
                        break;
                    case "Event_Data":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStream_Event_Data);
                        this.consumer.commitSync();
                        break;
                    case "General":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStream_General);
                        this.consumer.commitSync();
                        break;
                }
            }
        }
        outputStream_BR_RR.close();
        outputStream_ECG.close();
        outputStream_Event_Data.close();
        outputStream_General.close();
    }


}
