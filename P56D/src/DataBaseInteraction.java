
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Oteken
 */
public class DataBaseInteraction 
{
    private String host = "149.210.237.151";
    private int port = 27017;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;

    CarBrakeFunction func = new CarBrakeFunction();

    String[] unitTables = 
    {"UNIT14010206", "UNIT14100042",
     "UNIT14100064", "UNIT14100071",
     "UNIT14120026", "UNIT14120027", 
     "UNIT357566000058056", "UNIT357566000058064",
     "UNIT357566000058072", "UNIT357566000058106",
     "UNIT357566000058114", "UNIT357566040005620",
     "UNIT357566040005661", "UNIT357566040005687",
     "UNIT357566040024266"};

    /*
    {"UNIT14010206", "UNIT14100015", "UNIT14100042",
     "UNIT14100064", "UNIT14100071", "UNIT14120026",
     "UNIT14120027", "UNIT14120029", "UNIT14120031",
     "UNIT14120037", "UNIT15030000", "UNIT15030001",
     "UNIT357566000058056", "UNIT357566000058064",
     "UNIT357566000058072", "UNIT357566000058106",
     "UNIT357566000058114", "UNIT357566040005620",
     "UNIT357566040005653", "UNIT357566040005661",
     "UNIT357566040005687", "UNIT357566040024266"};
    */

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }
    
    public void setMongoDatabase(String databaseName) {
        this.mongoDatabase = mongoClient.getDatabase(databaseName);
    }
    
    public void connectToClient(){
        mongoClient = new MongoClient(getHost(), getPort()); 
    }

    public MongoCollection getCollection(String collectionName)
    {
        MongoCollection collection = getMongoDatabase().getCollection(collectionName);
        return collection;
    }
    
    public static void insertDocument(Document document, MongoCollection collection)
    {
        collection.insertOne(document);
    }
}
