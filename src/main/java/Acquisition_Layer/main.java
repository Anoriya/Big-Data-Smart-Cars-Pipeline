package Acquisition_Layer;
import Acquisition_Layer.Kafka.*;

import java.io.IOException;

public class main {
    public static void main(String[] args) throws InterruptedException, IOException {


        CameraConsumer cameraConsumer = new CameraConsumer("camera", "earliest", "false");
        AwConsumer awConsumer = new AwConsumer("Aw", "earliest", "false");
        ZephyrConsumer zephyrConsumer = new ZephyrConsumer("Zephyr", "earliest", "false");
        EmpaticaConsumer empaticaConsumer = new EmpaticaConsumer("Empatica", "earliest", "false");
        AirQualityConsumer airQualityConsumer=new AirQualityConsumer("AirQuality", "earliest", "false");

        Thread aw = new Thread(awConsumer);
        Thread zephyr = new Thread(zephyrConsumer);
        Thread camera = new Thread(cameraConsumer);
        Thread empatica = new Thread(empaticaConsumer);
        Thread airQuality = new Thread(airQualityConsumer);

        aw.start();
        zephyr.start();
        camera.start();
        empatica.start();
        airQuality.start();
    }
}