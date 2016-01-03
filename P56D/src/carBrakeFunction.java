
import com.mongodb.BasicDBObject;
import org.joda.time.DateTime;
import org.bson.Document;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.joda.time.Period;
/**
 *
 * @author Oteken
 */


public class carBrakeFunction {
    
    MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
    MongoDatabase db = mongoClient.getDatabase("citygis");
    MongoCollection positionsTable = db.getCollection("positions");


    public void createCarTable(MongoCollection parameterTable)
    { 
        MongoCursor table = parameterTable.find().iterator();
        Document row = new Document();
        while(table.hasNext())
        {
            row = (Document)table.next();
            String unitId = (String)row.get("UnitId");
            db.getCollection(unitId).insertOne(row);
            System.out.println(unitId + " - " + row);
        }
    }
    
    // LOOP AlL TABLES.
    public void updateAverageSpeeds(MongoCollection parameterTable)
    {
        int timeBuffer = 3;
        MongoCursor table = parameterTable.find().iterator();
        Document lastRow = (Document)table.next();
        Document row = new Document();
        while(table.hasNext())
        {
            row = (Document)table.next();

            DateTime date = new DateTime(row.getDate("DateTime"));
            DateTime lastDate = new DateTime(lastRow.getDate("DateTime"));
            Period period = new Period(lastDate, date);
            if(period.getSeconds() <= timeBuffer)
            {                                
                double avgSpeed = calculateAverageSpeed(
                                    row.getDouble("Rdx"), row.getDouble("Rdy"),
                                    lastRow.getDouble("Rdx"), lastRow.getDouble("Rdy"),
                                    period.getSeconds());

                row.put("avgSpeed", avgSpeed);
                parameterTable.replaceOne(new Document("_id", row.getObjectId("_id")), row);
            }

            lastRow = row;
        }
    }
    
    public double calculateAverageSpeed(double BposX, double BposY,
                                        double AposX, double AposY,
                                        int seconds)
    {
        double XDistance = Math.abs(BposX - AposX);
        double YDistance = Math.abs(BposY - AposY);

        //KM-->M
        XDistance = XDistance * 100;
        YDistance = YDistance * 100;

        double TotalDistance = (double)Math.sqrt((double)(Math.pow(XDistance, 2) +
                              (double)Math.pow(YDistance, 2)));

        // M/s
        double avgSpeed = TotalDistance/seconds;
        return avgSpeed;
    }

                
    public void updateDeltaSpeeds(MongoCollection parameterTable)
    {
        double decDelta = -1.00d;
        double ascDelta = 1.00d;
        int timeBuffer = 3;
        
        MongoCursor table = parameterTable.find().iterator();
        Document lastRow = (Document)table.next();
        Document row = new Document();
        
        while(table.hasNext())
        {
            row = (Document)table.next();
            
            DateTime date = new DateTime(row.getDate("DateTime"));
            DateTime lastDate = new DateTime(lastRow.getDate("DateTime"));
            Period period = new Period(lastDate, date);
            if(period.getSeconds() <= timeBuffer)
            {
                if((row.getDouble("avgSpeed") != null) && (lastRow.getDouble("avgSpeed") != null)
                    && (decDelta > row.getDouble("avgSpeed") - lastRow.getDouble("avgSpeed")||
                        ascDelta < row.getDouble("avgSpeed") - lastRow.getDouble("avgSpeed")))
                {
                    double deltaSpeed = calculateDeltaSpeed(row.getDouble("avgSpeed"),
                                                            lastRow.getDouble("avgSpeed"));

                    row.put("deltaSpeed", deltaSpeed);
                    row.put("deltaSecond", period.getSeconds());
                    parameterTable.replaceOne(new Document("_id", row.getObjectId("_id")), row);
                }
            }
            lastRow = row;
        }
    }
    
    public double calculateDeltaSpeed(double BavgSpeed, double AavgSpeed)
    {
        double deltaSpeed = BavgSpeed - AavgSpeed;
        return deltaSpeed;
    }
    
    public void updateBrakeSpeeds(MongoCollection parameterTable)
    {
        
        MongoCursor table = parameterTable.find().iterator();
        Document lastRow = (Document)table.next();
        Document row = new Document();
        
        while(table.hasNext())
        {
            row = (Document)table.next();
 
            if(calculateBrakeSpeed(row.getDouble("deltaSpeed"), row.getDouble("deltaSecond") >)
            row.put("deltaSpeed", deltaSpeed);
            parameterTable.replaceOne(new Document("_id", row.getObjectId("_id")), row);
            
            lastRow = row;
        }
        //if row has deltaSpeed && deltaSpeed > 10
        //insert row into carBrakeTable.
    }
    
    public double calculateBrakeSpeed(double deltaSpeed, double deltaSeconds)
    {
        double brakeSpeed = deltaSpeed / deltaSeconds;
        return brakeSpeed;
    }
    
    public void testFunction()
    {
        MongoCursor table = positionsTable.find().iterator();
        int timeBuffer = 3;
        
        Document lastRow = (Document)table.next();
        Document row = new Document();
        while(table.hasNext())
        {
            row = (Document)table.next();

            double f = row.getDouble("Rdx");
            
            f = f + 0.0001;
            System.out.println(f);
        }
    }
}
