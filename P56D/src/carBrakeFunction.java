
import org.joda.time.DateTime;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.joda.time.Period;
/**
 *
 * @author Oteken
 */


public class carBrakeFunction {
    
        String host = "149.210.237.151";
        MongoClient mongoClient = new MongoClient(host, 27017);
        MongoDatabase db = mongoClient.getDatabase("citygis");


    public void createCarTable(MongoCollection parameterTable)
    { 
        MongoCursor table = parameterTable.find().iterator();
        Document row = new Document();
        while(table.hasNext())
        {
            row = (Document)table.next();
            String unitId = (String)row.get("UnitId");
            unitId = "UNIT" + unitId;
            db.getCollection(unitId).insertOne(row);
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
        double brakeSpeedBuffer = -8.00f;
        
        MongoCursor table = parameterTable.find().iterator();
        Document lastRow = (Document)table.next();
        Document row = new Document();
        
        while(table.hasNext())
        {
            row = (Document)table.next();
            
            double brakeSpeed;

            try
            {
                brakeSpeed = calculateBrakeSpeed(row.getDouble("deltaSpeed"), row.getDouble("deltaSecond"));
            }
            catch(Exception e)
            {
                brakeSpeed = 0.00;
            }
            
            if(brakeSpeed < brakeSpeedBuffer)
            {
                Document document = new Document();
                document.put("UnitId", row.getString("UnitId"));
                document.put("DateTime", row.getDate("DateTime"));
                document.put("oldRdx", lastRow.getDouble("Rdx"));
                document.put("oldRdy", lastRow.getDouble("Rdy"));
                document.put("Rdx", row.getDouble("Rdx"));
                document.put("Rdy", row.getDouble("Rdy"));
                document.put("avgSpeed", row.getDouble("avgSpeed"));
                document.put("deltaSpeed", row.getDouble("deltaSpeed"));
                document.put("deltaSecond", row.getDouble("deltaSecond"));
                document.put("brakeSpeed", brakeSpeed);
                db.getCollection("brakeTable").insertOne(document);
            }            
            lastRow = row;
        }
    }
    
    public double calculateBrakeSpeed(double deltaSpeed, double deltaSeconds)
    {
        double brakeSpeed = deltaSpeed / deltaSeconds;
        return brakeSpeed;
    }

}
