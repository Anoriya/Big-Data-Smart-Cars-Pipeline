package Acquisition_Layer.Kafka;


public class CameraConsumer extends AbstractConsumer{

    private static final String TOPIC = "Camera";
    private static final String FILEPATH = "/user/hdfs/Camera/";
    private static final String FILENAME = "test";

    public CameraConsumer(String group_id, String offset_reset, String auto_commit) {
        super(TOPIC,FILEPATH, FILENAME, group_id, offset_reset, auto_commit);
    }

}

