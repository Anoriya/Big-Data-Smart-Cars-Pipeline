package Refinement_Layer;

import org.apache.spark.SparkConf;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CouchDB {
    private static volatile HttpClient connection;
    private static volatile CouchDbInstance dbInstance;
    private static volatile CouchDbConnector db;


    private static HttpClient getConnection() throws MalformedURLException {
        if (connection == null) {
            connection = new StdHttpClient.Builder()
                    .url("http://localhost:5984").username("admin")
                    .password("Inchalah1.")
                    .build();
        }
        return connection;
    }

    private static CouchDbInstance getDbInstance() throws MalformedURLException {
        if (dbInstance == null) {
            dbInstance = new StdCouchDbInstance(getConnection());
        }
        return dbInstance;
    }

    private static CouchDbConnector getDb() throws MalformedURLException {
        if (db == null){
            db = new StdCouchDbConnector("test", getDbInstance());
        }
        return db;
    }

    public static void createDocumentOfListMapped(List<Map<String, String>> doc) throws MalformedURLException {
        Map<String, List<Map<String, String>>> document = new HashMap<String, List<Map<String, String>>>();
        document.put("Camera", doc);
        getDb().createDatabaseIfNotExists();
        System.out.println(document);
        getDb().create(document);
    }

    public static void createDocument(Map<String, Map<String, Double>> doc) throws MalformedURLException {
        getDb().createDatabaseIfNotExists();
        System.out.println(doc);
        getDb().create(doc);
    }
}
