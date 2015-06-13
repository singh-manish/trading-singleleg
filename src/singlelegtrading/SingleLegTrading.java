/*
The MIT License (MIT)

Copyright (c) 2015 Manish Kumar Singh

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 
*/


package singlelegtrading;

/**
 * @author Manish Kumar Singh
 */

import redis.clients.jedis.*;
import java.util.*;

public class SingleLegTrading {

    private static boolean debugFlag = false;

    private static JedisPool jedisPool;
    private static MyUtils myUtils;
    private static String redisConfigurationKey = "INRSTR01CONFIGS";

    private IBInteraction ibInteractionClient;

    private static String ibOrderIDKeyName;
    private static String exchangeHolidayListKeyName;
    
    // Define class to store Manul Intervention Signals for each Open Position
    public class MyManualInterventionClass {
         int slotNumber;
         String targetValue;
         boolean squareOff, updateTakeProfit, updateStopLoss;
    }
    public MyManualInterventionClass[] myMIDetails;

   private static final int MAXALLOWEDOPENSLOTS = 50;  
 
   public static TimeZone exchangeTimeZone = TimeZone.getTimeZone("Asia/Calcutta");
   private static int exchangeStartTimeHHMM = 915;
   private static int exchangeStartTimeHHMMSS = 91501;
   private static int exchangeCloseTimeHHMM = 1529;
   private static int exchangeCloseTimeHHMMSS = 152959;
   
   
   SingleLegTrading(String redisIP, int redisPort, String redisConfigKey, String ibIP, int ibPort, int ibClientID, String exchangeCurrency, boolean debugIndicator){
        // Set Debug Flag 
        debugFlag = debugIndicator;
        // Create connection Pool for  Redis server. 
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setBlockWhenExhausted(false);
        jedisPool = new JedisPool(poolConfig, redisIP, redisPort); 

        myUtils = new MyUtils();
        redisConfigurationKey = redisConfigKey;

        if (exchangeCurrency.equalsIgnoreCase("inr")) {
            exchangeTimeZone = TimeZone.getTimeZone("Asia/Calcutta");
            exchangeStartTimeHHMM = 915;
            exchangeStartTimeHHMMSS = 91501;
            exchangeCloseTimeHHMM = 1529;
            exchangeCloseTimeHHMMSS = 152959;            
        } else if (exchangeCurrency.equalsIgnoreCase("usd")) {
            exchangeTimeZone = TimeZone.getTimeZone("America/New_York");            
            exchangeStartTimeHHMM = 930;
            exchangeStartTimeHHMMSS = 93001;
            exchangeCloseTimeHHMM = 1559;
            exchangeCloseTimeHHMMSS = 155959;            
        }
        TimeZone.setDefault(exchangeTimeZone);
        
        ibOrderIDKeyName = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "ORDERIDFIELDKEYNAME",false);        
        exchangeHolidayListKeyName = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "EXCHANGEHOLIDAYLISTKEYNAME",false);           
        ibInteractionClient = new IBInteraction(jedisPool,ibOrderIDKeyName,ibIP,ibPort,ibClientID, exchangeTimeZone);
        
        myMIDetails = new MyManualInterventionClass[MAXALLOWEDOPENSLOTS];                
        for (int index = 0; index < myMIDetails.length; index++ ) {         
            myMIDetails[index] = new MyManualInterventionClass();
            myMIDetails[index].slotNumber = 0;
            myMIDetails[index].targetValue = "0"; 
            myMIDetails[index].squareOff = false;
            myMIDetails[index].updateTakeProfit = false;                         
            myMIDetails[index].updateStopLoss = false;                
        }
        
   }
   
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // arg[0] is debug flag. 1 is true. 0 is false
        // arg[1] is ip address/machine name where Redis Server is running
        // arg[2] is port number on which Redis Server is listening for connections        
        // arg[3] is configKey in Redis Server to use   
        // arg[4] is ip address/machine name where TWS is running
        // arg[5] is port number on which TWS is listening for API calls
        // arg[6] is client id to be used for getting this info.
        // arg[7] is exchange currency to be used for this session. based on that timezone gets set..
        
        // Read the Command Line Arguments
        if (args.length <= 7) {
                System.err.println("Number of Arguments can not be Less than 8");
                System.out.println(" Usage : <fileName.jar> <debugFlag : 0 or 1> <redisServerIPAddress> <redisServerPortNumber> <redisServerConfigKey> <TWSIPAddress> <TWSPortNumber> <TWSClientID> <ExchangeCurrency>");
                System.exit(1);            
        }
        
        boolean tempFlag = false;
        try {
            tempFlag = (Integer.parseInt(args[0]) == 1);
        } catch (NumberFormatException e) {
            System.err.println("First Argument must be an integer indicating Debug flag. Exiting..");
            System.exit(1);
        }           
        
        SingleLegTrading myComboTradingSystem = new SingleLegTrading(args[1],Integer.parseInt(args[2]),args[3],args[4],Integer.parseInt(args[5]),Integer.parseInt(args[6]),args[7], tempFlag);
        
        // Set default timezone
        TimeZone.setDefault(exchangeTimeZone);
        // Consolidate all open positions to serial from first position        
        myUtils.defragmentOpenPositionsQueue(jedisPool, redisConfigurationKey, true);
                
        if ( (!myUtils.fallsOnExchangeHoliday("Exchange is closed today", myUtils.getKeyValueFromRedis(jedisPool, exchangeHolidayListKeyName, false), Calendar.getInstance(exchangeTimeZone), debugFlag)) ) {
            // Check if current time is outside Exchange Operating hours, then keep waiting for exchange to open

            myUtils.waitForStartTime(exchangeStartTimeHHMM, exchangeTimeZone, "Exchange to open", debugFlag);

            // Keep trying to establish Connection with IB Client till 15 minutes to market closing time 
            while ( (!myComboTradingSystem.ibInteractionClient.connectToIB(120)) &&
                    (Integer.parseInt(String.format("%1$tH%1$tM",Calendar.getInstance(exchangeTimeZone))) < (exchangeCloseTimeHHMM - 12) ) ) {
                myUtils.waitForNSeconds(300); 
            }

            if (debugFlag) {
                //myComboTradingSystem.ibInteractionClient.requestExecutionDetailsHistorical(1,31);
                System.out.println(" OrderID field " + ibOrderIDKeyName + " has been set for next Order Id as : " + myUtils.getKeyValueFromRedis(jedisPool, ibOrderIDKeyName, false));                                       
            }

            // Spawn a thread to monitor the manual intervention signals queue
            MonitorManualInterventionSignals monitorManualInterventionSignalsQueue = new MonitorManualInterventionSignals("MonitoringManualInterventionsSignalsThread", jedisPool, redisConfigurationKey, exchangeTimeZone, myComboTradingSystem.myMIDetails, debugFlag);
            monitorManualInterventionSignalsQueue.start();  
            
            // Spawn a thread to monitor the entry signal queue
            MonitorEntrySignals monitorEntrySignalsQueue = new MonitorEntrySignals("MonitoringEntrySignalsThread", jedisPool, myComboTradingSystem.ibInteractionClient, redisConfigurationKey, exchangeTimeZone, debugFlag);
            monitorEntrySignalsQueue.start();          

            // Spawn a thread to read the current open positions from Redis queue
            // For each open position, spawn another monitoring thread is spawned.
            MonitorOpenPositions4Exit monitorOpenPositionsQueue = new MonitorOpenPositions4Exit("MonitoringOpenPositionsForExitThread", jedisPool, myComboTradingSystem.ibInteractionClient, redisConfigurationKey, exchangeTimeZone, myComboTradingSystem.myMIDetails, debugFlag);
            monitorOpenPositionsQueue.start();          
            
            // Keep running the program till it is Exchange Closing time
            boolean exitNow = false;
            while (!exitNow) {
                Calendar timeNow = Calendar.getInstance(exchangeTimeZone);        
                // Provision for exiting if time has reached outside market hours or on weekends for NSE
                if (Integer.parseInt(String.format("%1$tH%1$tM%1$tS",timeNow)) > exchangeCloseTimeHHMMSS) {
                    exitNow = true;
                    System.out.println(String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow) + " : Exiting as reaching Outside market hours");                           
                    if (debugFlag) {
                        System.out.println("Reached End of Day at :" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow));                            
                    }
                }                           
                // Wait for three minutes before next round of checking for exchange closure
                myUtils.waitForNSeconds(120);
            }
            // Disconnect IB 
            myComboTradingSystem.ibInteractionClient.disconnectFromIB();   
        }

        // Consolidate all open positions to serial from first position
        myUtils.defragmentOpenPositionsQueue(jedisPool, redisConfigurationKey, true);

        // Consolidate all current closed positions to Archived queue (PAST CLOSED POSITIONS)
        myUtils.moveCurrentClosedPositions2ArchiveQueue(jedisPool, redisConfigurationKey, false);
        
        // Release the connection Pool of Redis
        jedisPool.destroy();
        // Exit with Success 
        System.exit(0);        
    }
}
