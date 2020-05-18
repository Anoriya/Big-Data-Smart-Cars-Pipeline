package Acquisition_Layer;
import Acquisition_Layer.Kafka.AwConsumer;
import Acquisition_Layer.Kafka.CameraConsumer;
import Acquisition_Layer.Kafka.EmpaticaConsumer;
import Acquisition_Layer.Kafka.ZephyrConsumer;

import java.io.IOException;

public class main {
    public static void main(String[] args) throws InterruptedException, IOException {


        CameraConsumer cameraConsumer = new CameraConsumer("camera", "earliest", "false");
        AwConsumer awConsumer = new AwConsumer("Aw", "earliest", "false");
        ZephyrConsumer zephyrConsumer = new ZephyrConsumer("Zephyr", "earliest", "false");
        EmpaticaConsumer empaticaConsumer = new EmpaticaConsumer("Empatica", "earliest", "false");

        Thread aw = new Thread(awConsumer);
        Thread zephyr = new Thread(zephyrConsumer);
        Thread camera = new Thread(cameraConsumer);
        Thread empatica = new Thread(empaticaConsumer);

        aw.start();
        zephyr.start();
        camera.start();
        empatica.start();
    }
}