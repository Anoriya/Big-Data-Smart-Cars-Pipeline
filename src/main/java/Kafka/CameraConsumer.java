package Kafka;

import java.io.IOException;


public class CameraConsumer extends AbstractConsumer {

    private static final String TOPIC = "test";
    private static final String filepath = "/user/hdfs/test/";
    private static final String filename = "test.csv";

    public CameraConsumer(String group_id, String offset_reset, String auto_commit) {
        super(group_id, offset_reset, auto_commit);
    }


    public void runConsumer() throws IOException {
        super.runConsumer(TOPIC, filepath, filename);
    }
}

