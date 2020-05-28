package Refinement_Layer;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkConfig {

    private static volatile SparkConf conf;
    private static volatile JavaStreamingContext ssc;

    private static SparkConf getConf(){
        if (conf == null) {
            conf = new SparkConf().setAppName("appName");
        }
        return conf;
    }

    protected static JavaStreamingContext getStreamingContext(){
        if (conf == null) {
            ssc =new JavaStreamingContext(SparkConfig.getConf(), new Duration(2000));
        }
        return ssc;
    }
}
