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

import java.util.Calendar;
import java.util.TimeZone;
import redis.clients.jedis.JedisPool;

/**
 * @author Manish Kumar Singh
 */

public class MonitorOpenPositions4OrderCompletionStatus extends Thread {
    
   private Thread t;
   private String threadName;
   private boolean debugFlag;
   private JedisPool jedisPool;

   private IBInteraction ibInteractionClient;
   
   private MyUtils myUtils;
   
   private String redisConfigurationKey;
   private TimeZone exchangeTimeZone;
   private String openPositionsQueueKeyName = "INRSTR01OPENPOSITIONS";
   private int MAXPOSITIONSTOTRACK = 25;
      
   MonitorOpenPositions4OrderCompletionStatus(String name, JedisPool redisConnectionPool, IBInteraction ibIntClient, String redisConfigKey, TimeZone exTZ, boolean debugIndicator){

        threadName = name;
        debugFlag = debugIndicator;
        // Establish connection Pool Redis Server. 
        jedisPool = redisConnectionPool;               
        ibInteractionClient = ibIntClient;                
        redisConfigurationKey = redisConfigKey; 
        exchangeTimeZone = exTZ;
        myUtils = new MyUtils();
        TimeZone.setDefault(exchangeTimeZone);
   }

    @Override 
    public void run() {

        // Initialize Values of queues and Max positions
        openPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "OPENPOSITIONSQUEUE", false);
        int MAXPOSITIONS = Integer.parseInt(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "MAXNUMPAIRPOSITIONS", false));        
        
        if ((3*MAXPOSITIONS) < 25) { 
            MAXPOSITIONSTOTRACK = 24;
        } else if (((3*MAXPOSITIONS) < 50) && ((3*MAXPOSITIONS) >= 25)) {
            MAXPOSITIONSTOTRACK = 3*MAXPOSITIONS;
        } else {
            MAXPOSITIONSTOTRACK = 49;
        }
        
        int firstExitOrderTime = 916;
        if (exchangeTimeZone.equals(TimeZone.getTimeZone("Asia/Calcutta"))) {
            firstExitOrderTime = 916;
        } else if (exchangeTimeZone.equals(TimeZone.getTimeZone("America/New_York"))) {
            firstExitOrderTime = 931;        
        }        
        myUtils.waitForStartTime(firstExitOrderTime, exchangeTimeZone, "Exit first order time", debugFlag);
        
        // Market is open. Now start monitoring the open positions queue
        int eodExitTime = 1530;
        String eodExitTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EODEXITTIME", false);
        if (( eodExitTimeConfigValue != null) && (eodExitTimeConfigValue.length() > 0)) {
            eodExitTime = Integer.parseInt(eodExitTimeConfigValue);
        }        
        
        while (myUtils.marketIsOpen(eodExitTime, exchangeTimeZone, false)) {            
            
            for (int slotNumber = 1; slotNumber <= MAXPOSITIONSTOTRACK; slotNumber++) {
                if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), false)) {
                    
                    TradingObject myTradeObject = new TradingObject(myUtils.getHashMapValueFromRedis(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber),debugFlag));

                    if (myTradeObject.getOrderState().equalsIgnoreCase("entryorderinitiated")) {
                        // Inititate the corrective steps if therre is gap of more than 15 minutes between order time and current time
                        String entryOrderTime = myTradeObject.getEntryTimeStamp();
                        String timeNow = String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone));
                        if (myUtils.checkIfStaleMessage(entryOrderTime,timeNow,20)) {
                            // Get the order IDs
                            int entryOrderID = Integer.parseInt(myTradeObject.getEntryOrderIDs());
                            // Get Execution details from IB
                            // update the openpositionqueue in redis to start monitoring
                        }
                    } else if (myTradeObject.getOrderState().equalsIgnoreCase("exitordersenttoexchange")) {
                        // Inititate the corrective steps if therre is gap of more than 15 minutes between order time and current time                        
                        String exitOrderTime = myTradeObject.getExitTimeStamp();
                        String timeNow = String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone));
                        if (myUtils.checkIfStaleMessage(exitOrderTime,timeNow,20)) {
                            // Get the order IDs
                            int exitOrderID = Integer.parseInt(myTradeObject.getExitOrderIDs());
                            // Get Execution details from IB
                            // update the open position as well as closed position queue in Redis to update the details appropriately                            
                        }
                    }
                } // end of checking of each slot                
            } // end of For loop of checking against each position

            // Wait for Five minutes before checking again
            myUtils.waitForNSeconds(300);            
        }    
     }
   
   @Override 
    public void start () {
        this.setName(threadName);        
        if (t == null)
        {
           t = new Thread (this, threadName);
           t.start ();
        }
    }
       
}
