/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Oteken
 */
public class CarBrakeFunctionCalculations {
    
    public static double calculateAverageSpeed(double BposX, double BposY,
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
    
    public static double calculateDeltaSpeed(double BavgSpeed, double AavgSpeed)
    {
        double deltaSpeed = BavgSpeed - AavgSpeed;
        return deltaSpeed;
    }
    
    public static double calculateBrakeSpeed(double deltaSpeed, double deltaSeconds)
    {
        double brakeSpeed = deltaSpeed / deltaSeconds;
        return brakeSpeed;
    }
}
