import DBSCAN.TestDBSCAN;
import Refinement_Layer.Spark;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

import java.util.HashMap;
import java.util.Map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class main {
    public static void main(String[] args) throws InterruptedException, IOException {


//      Spark spark = new Spark();
        HttpClient httpClient = new StdHttpClient.Builder()
                .url("http://localhost:5984").username("admin")
                .password("Inchalah1.")
                .build();
        CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
        CouchDbConnector db = new StdCouchDbConnector("test", dbInstance);
        db.createDatabaseIfNotExists();

        Map<String, Object> document = new HashMap<String, Object>();
        Map<String, Object> Empatica = new HashMap<String, Object>();
        Map<String, Object> Zephyr = new HashMap<String, Object>();
        Map<String, Object> Acc = new HashMap<String, Object>();
        int[] array = new int[2];
        array[0]=12;
        array[1]=15;
        Acc.put("val1", 60);
        Acc.put("val2", 69);
        Empatica.put("HR" , 32);
        Empatica.put("ACC" , Acc);
        Zephyr.put("BRR", array);
        document.put("Empatica", Empatica);
        document.put("Zephyr", Zephyr);
        db.create(document);

    }
}