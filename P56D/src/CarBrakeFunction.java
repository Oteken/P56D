
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


public class CarBrakeFunction {
    
    public void createCarTable(MongoCollection parameterTable, MongoDatabase database)
    { 
        MongoCursor table = parameterTable.find().iterator();
        Document row = new Document();
        while(table.hasNext())
        {
            row = (Document)table.next();
            String unitId = (String)row.get("UnitId");
            unitId = "UNIT" + unitId;
            MongoCollection unitTable = database.getCollection(unitId);
            DataBaseInteraction.insertDocument(row, unitTable);
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
                double avgSpeed = CarBrakeFunctionCalculations.calculateAverageSpeed(
                                    row.getDouble("Rdx"), row.getDouble("Rdy"),
                                    lastRow.getDouble("Rdx"), lastRow.getDouble("Rdy"),
                                    period.getSeconds());

                row.put("avgSpeed", avgSpeed);
                parameterTable.replaceOne(new Document("_id", row.getObjectId("_id")), row);
            }

            lastRow = row;
        }
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
                    double deltaSpeed = CarBrakeFunctionCalculations.calculateDeltaSpeed(row.getDouble("avgSpeed"),
                                                            lastRow.getDouble("avgSpeed"));

                    row.put("deltaSpeed", deltaSpeed);
                    row.put("deltaSecond", period.getSeconds());
                    parameterTable.replaceOne(new Document("_id", row.getObjectId("_id")), row);
                }
            }
            lastRow = row;
        }
    }
    
    public void updateBrakeSpeeds(MongoCollection parameterTable, MongoDatabase database)
    {
        double brakeSpeedBuffer = -0.01f;
        
        MongoCursor table = parameterTable.find().iterator();
        Document lastRow = (Document)table.next();
        Document row = new Document();
        
        while(table.hasNext())
        {
            row = (Document)table.next();
            
            double brakeSpeed;
            
            System.out.println(row.getDouble("deltaSpeed"));
            System.out.println(row.getInteger("deltaSecond"));
            try
            {
                System.out.println(1);
                brakeSpeed = CarBrakeFunctionCalculations.calculateBrakeSpeed(row.getDouble("deltaSpeed"), row.getInteger("deltaSecond"));
            }
            catch(Exception e)
            {
                brakeSpeed = 0.00;
            }
            
            if(brakeSpeed < brakeSpeedBuffer)
            {
                System.out.println(2);
                Document document = new Document();
                document.put("UnitId", row.getString("UnitId"));
                document.put("DateTime", row.getDate("DateTime"));
                document.put("oldRdx", lastRow.getDouble("Rdx"));
                document.put("oldRdy", lastRow.getDouble("Rdy"));
                document.put("Rdx", row.getDouble("Rdx"));
                document.put("Rdy", row.getDouble("Rdy"));
                document.put("avgSpeed", row.getDouble("avgSpeed"));
                document.put("deltaSpeed", row.getDouble("deltaSpeed"));
                document.put("deltaSecond", row.getInteger("deltaSecond"));
                document.put("brakeSpeed", brakeSpeed);
                System.out.println(document);
                MongoCollection brakeTable = database.getCollection("brakeTable");
                DataBaseInteraction.insertDocument(document, brakeTable);
            }            
            lastRow = row;
        }
    }
}
