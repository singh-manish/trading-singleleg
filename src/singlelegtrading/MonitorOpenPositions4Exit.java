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
import redis.clients.jedis.*;

/**
 * @author Manish Kumar Singh
 */

public class MonitorOpenPositions4Exit extends Thread {
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

   public singlelegtrading.SingleLegTrading.MyManualInterventionClass[] myMIDetails;      
      
   MonitorOpenPositions4Exit(String name, JedisPool redisConnectionPool, IBInteraction ibIntClient, String redisConfigKey, TimeZone exTZ, singlelegtrading.SingleLegTrading.MyManualInterventionClass[] miDetails, boolean debugIndicator){

        threadName = name;
        debugFlag = debugIndicator;
        // Establish connection Pool Redis Server. 
        jedisPool = redisConnectionPool;               
        ibInteractionClient = ibIntClient;                
        redisConfigurationKey = redisConfigKey; 
        exchangeTimeZone = exTZ;
        myUtils = new MyUtils();
        myMIDetails = miDetails;
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
        Thread[] exitThreads = new Thread[MAXPOSITIONSTOTRACK+1];
        SingleLegExit[] exitObjects = new SingleLegExit[MAXPOSITIONSTOTRACK+1];        
        for (int slotNumber = 1; slotNumber <= MAXPOSITIONSTOTRACK; slotNumber++) {
            exitThreads[slotNumber] = null;
            exitObjects[slotNumber] = null;            
        }

        int eodExitTime = 1530;
        String eodExitTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EODEXITTIME", false);
        if (( eodExitTimeConfigValue != null) && (eodExitTimeConfigValue.length() > 0)) {
            eodExitTime = Integer.parseInt(eodExitTimeConfigValue);
        }        
        
        while (myUtils.marketIsOpen(eodExitTime, exchangeTimeZone, false)) {
            
            for (int slotNumber = 1; slotNumber <= MAXPOSITIONSTOTRACK; slotNumber++) {
                if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), false)) {
                    
                    TradingObject myTradeObject = new TradingObject(myUtils.getHashMapValueFromRedis(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber),debugFlag));

                    if (myTradeObject.getOrderState().equalsIgnoreCase("entryorderfilled")) {
                        // Check if corresponding thread is running. if not running then start a thread to monitor the position
                        String exitMonitoringThreadName = "monitoringExit4Position_" + Integer.toString(slotNumber);
                        // if this is first time then thread object would be null. if it is null then create the instance
                        if (exitThreads[slotNumber] == null) {
                            exitObjects[slotNumber] = new SingleLegExit(exitMonitoringThreadName, jedisPool, ibInteractionClient, redisConfigurationKey, exchangeTimeZone, slotNumber, myMIDetails, debugFlag);                        
                            exitThreads[slotNumber] = new Thread(exitObjects[slotNumber]);
                            exitThreads[slotNumber].setName(exitMonitoringThreadName);                            
                            exitThreads[slotNumber].start();                        
                        } else if (exitThreads[slotNumber].getState() == Thread.State.TERMINATED)  {                        
                            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Info : Restarting Monitoring Thread for Leg " +  myTradeObject.getTradingObjectName() + " having state as " + exitThreads[slotNumber].getState() + " having Name as " + exitThreads[slotNumber].getName() + " having alive status as " + exitThreads[slotNumber].isAlive());
                            exitObjects[slotNumber] = new SingleLegExit(exitMonitoringThreadName, jedisPool, ibInteractionClient, redisConfigurationKey, exchangeTimeZone, slotNumber, myMIDetails, debugFlag);                        
                            exitThreads[slotNumber] = new Thread(exitObjects[slotNumber]);
                            exitThreads[slotNumber].setName(exitMonitoringThreadName);                            
                            exitThreads[slotNumber].start();                                                                       
                        }
                    } else {
                        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Info : Not Able to Start Monitoring for Leg " +  myTradeObject.getTradingObjectName() + " as Entry Order is not updated as completely filled in open positions queue" );
                    }
                } // end of checking of each slot                
            } // end of For loop of checking against each position

            // Wait for two minutes before checking again
            myUtils.waitForNSeconds(120);            
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
