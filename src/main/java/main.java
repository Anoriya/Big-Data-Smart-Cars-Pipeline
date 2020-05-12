import Kafka.AwConsumer;
import Kafka.ZephyrConsumer;

import java.io.IOException;

public class main {
    public static void main(String[] args) throws InterruptedException, IOException {


//        CameraConsumer consumer = new CameraConsumer("test", "earliest", "false");
//        consumer.runConsumer();
        AwConsumer awConsumer = new AwConsumer("Aw", "earliest","false");
        ZephyrConsumer zephyrConsumer = new ZephyrConsumer("Zephyr", "earliest", "false");

        Thread aw = new Thread(awConsumer);
        Thread zephyr = new Thread(zephyrConsumer);

        aw.start();
        zephyr.start();

    }
}
