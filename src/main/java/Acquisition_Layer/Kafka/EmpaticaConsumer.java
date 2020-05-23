package Acquisition_Layer.Kafka;

import Hdfs.HdfsWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class EmpaticaConsumer extends AbstractConsumer {

    private static final String TOPIC = "Empatica";
    private static final String FILEPATH = "/user/hdfs/Empatica/";
    private static final String FILENAME = "test";

    public EmpaticaConsumer(String group_id, String offset_reset, String auto_commit) {
        super(TOPIC, FILEPATH, FILENAME, group_id, offset_reset, auto_commit);
    }

    @Override
    protected void storeData(HdfsWriter csv_writer, String filepath, String filename, DateTimeFormatter formatter, LocalDateTime date) throws IOException {
        // Init Hadoop outputStream
        FSDataOutputStream outputStreamACC = csv_writer.createFileAndOutputStream(filepath,
                "ACC" + formatter.format(date) + ".csv");

        FSDataOutputStream outputStreamBVP = csv_writer.createFileAndOutputStream(filepath,
                "BVP" + formatter.format(date) + ".csv");

        FSDataOutputStream outputStreamEDA = csv_writer.createFileAndOutputStream(filepath,
                "EDA" + formatter.format(date) + ".csv");

        FSDataOutputStream outputStreamHR = csv_writer.createFileAndOutputStream(filepath,
                "HR" + formatter.format(date) + ".csv");

        FSDataOutputStream outputStreamIBI = csv_writer.createFileAndOutputStream(filepath,
                "IBI" + formatter.format(date) + ".csv");

        FSDataOutputStream outputStreamTAGS = csv_writer.createFileAndOutputStream(filepath,
                "TAGS" + formatter.format(date) + ".csv");

        FSDataOutputStream outputStreamTEMP = csv_writer.createFileAndOutputStream(filepath,
                "TEMP" + formatter.format(date) + ".csv");

        // While we're still at today
        while (LocalDateTime.now().isBefore(LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), date.getHour(), date.getMinute() + 1))) {
            ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(2000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key());
                switch (record.key()) {
                    case "ACC":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStreamACC);
                        this.consumer.commitSync();
                        break;
                    case "BVP":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStreamBVP);
                        this.consumer.commitSync();
                        break;
                    case "EDA":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStreamEDA);
                        this.consumer.commitSync();
                        break;
                    case "HR":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStreamHR);
                        this.consumer.commitSync();
                        break;
                    case "TEMP":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStreamTEMP);
                        this.consumer.commitSync();
                        break;
                    case "IBI":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStreamIBI);
                        this.consumer.commitSync();
                        break;
                    case "TAGS":
                        csv_writer.writeLineIntoOutputStream(record.value(), outputStreamTAGS);
                        this.consumer.commitSync();
                        break;
                }
            }
        }
        outputStreamACC.close();
        outputStreamBVP.close();
        outputStreamEDA.close();
        outputStreamHR.close();
        outputStreamIBI.close();
        outputStreamTAGS.close();
        outputStreamTEMP.close();


    }
}
