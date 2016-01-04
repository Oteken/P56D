
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Oteken
 */
public class Main {

    
    
    public static void main(String[] args) 
    {                          
        DataBaseInteraction DBInteraction = new DataBaseInteraction();
        DBInteraction.setHost("149.210.237.151");
        DBInteraction.setPort(27017);
        DBInteraction.connectToClient();
        DBInteraction.setMongoDatabase("citygis");
        MongoCollection positionsTable = DBInteraction.getCollection("positions");
        String[] unitTables = 
        {"UNIT14010206", "UNIT14100042",
         "UNIT14100064", "UNIT14100071",
         "UNIT14120026", "UNIT14120027", 
         "UNIT357566000058056", "UNIT357566000058064",
         "UNIT357566000058072", "UNIT357566000058106",
         "UNIT357566000058114", "UNIT357566040005620",
         "UNIT357566040005661", "UNIT357566040005687",
         "UNIT357566040024266"};
        
        CarBrakeFunction carBrakeFunction = new CarBrakeFunction();

        carBrakeFunction.createCarTable(positionsTable);
        for(int i = 0 ; i < unitTables.length ; i++)
        {
            System.out.println(unitTables[i]);
            MongoCollection unitTable = DBInteraction.getCollection(unitTables[i]);
            carBrakeFunction.updateAverageSpeeds(unitTable);
            carBrakeFunction.updateDeltaSpeeds(unitTable);
            carBrakeFunction.updateBrakeSpeeds(unitTable);
        }
    }
    
}
