package Kafka;


import java.io.IOException;

public class AwConsumer extends AbstractConsumer {

    private static final String TOPIC = "test";
    private static final String FILEPATH = "/user/hdfs/sensors/AW/";
    private static final String FILENAME = "sth.csv";


    protected AwConsumer(String group_id, String offset_reset, String auto_commit) {
        super(group_id, offset_reset, auto_commit);
    }


    public void runConsumer() throws IOException {
        super.runConsumer(TOPIC, FILEPATH, FILENAME);
    }
}
