package Kafka;


import java.io.IOException;

public class AwConsumer extends AbstractConsumer {

    private static final String TOPIC = "Aw";
    private static final String FILEPATH = "/user/hdfs/AW/";
    private static final String FILENAME = "test";


    public AwConsumer(String group_id, String offset_reset, String auto_commit) {
        super(TOPIC,FILEPATH, FILENAME, group_id, offset_reset, auto_commit);
    }

}
