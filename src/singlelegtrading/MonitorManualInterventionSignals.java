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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.*;

/**
 * @author Manish Kumar Singh
 */

public class MonitorManualInterventionSignals extends Thread {

   private Thread t;
   private String threadName = "MonitoringManualInterventionsThread";
   private boolean debugFlag;
   private JedisPool jedisPool;    
   
   private String redisConfigurationKey;
   private MyExchangeClass myExchangeObj;
   
   private MyUtils myUtils;
   
   private String strategyName = "pairstr01";
   private String manualInterventionSignalsQueueKeyName ="INRSTR01MANUALINTERVENTIONS";
   private String confOrderType = "MARKET";

   public ConcurrentHashMap<String, MyManualInterventionClass> myMIDetails;   
   
   MonitorManualInterventionSignals(String name, JedisPool redisConnectionPool, String redisConfigKey,  MyExchangeClass exchangeObj, ConcurrentHashMap<String, MyManualInterventionClass> miDetails, boolean debugIndicator){

        threadName = name;
        debugFlag = debugIndicator;

        jedisPool = redisConnectionPool;

        myUtils = new MyUtils();
        
        redisConfigurationKey = redisConfigKey;
        myExchangeObj = exchangeObj;
        TimeZone.setDefault(myExchangeObj.getExchangeTimeZone());
        
        //"pairstr01", "INRSTR01OPENPOSITIONS", "INRSTR01ENTRYSIGNALS"        
        strategyName = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "STRATEGYNAME",false);        
        manualInterventionSignalsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "MANUALINTERVENTIONQUEUE",false);
        
        myMIDetails = miDetails;
        
        // Debug Message
        System.out.println(" Started Monitoring for Manual Signal for Strategy Name " + strategyName + " queue Name " + manualInterventionSignalsQueueKeyName);

   }
        
    void setTradeLevelSquareOff(int slotNumber) {
        String openPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "OPENPOSITIONSQUEUE",false);
        String key = Integer.toString(slotNumber);
        if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, key, false)) {
            // Set the variable to square Off the position
            if (myMIDetails.containsKey(key)) {
                myMIDetails.get(key).setSlotNumber(slotNumber);
                myMIDetails.get(key).setActionIndicator(MyManualInterventionClass.SQUAREOFF);
                // ..                
            }
        }          
    }

    void setTradeLevelStopLoss(int slotNumber, String stopLossValue) {
        String openPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "OPENPOSITIONSQUEUE",false);
        String key = Integer.toString(slotNumber);
        if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, key, false)) {
            // Set the variable to update stop loss of the position
            if (myMIDetails.containsKey(key)) {
                myMIDetails.get(key).setSlotNumber(slotNumber);
                myMIDetails.get(key).setTargetValue(stopLossValue);
                myMIDetails.get(key).setActionIndicator(MyManualInterventionClass.UPDATESTOPLOSS); 
            }
        }          
    }

    void setTradeLevelTakeProfit(int slotNumber, String takeProfitValue) {
        String openPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "OPENPOSITIONSQUEUE",false);
        String key = Integer.toString(slotNumber);
        if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, key, false)) {
            // Set the variable to update take profit of the position
            if (myMIDetails.containsKey(key)) {
                myMIDetails.get(key).setSlotNumber(slotNumber);
                myMIDetails.get(key).setTargetValue(takeProfitValue);
                myMIDetails.get(key).setActionIndicator(MyManualInterventionClass.UPDATETAKEPROFIT); 
            }
        }          
    }

    void setTradeLevelStopMonitoring(int slotNumber) {
        String openPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "OPENPOSITIONSQUEUE",false);
        String key = Integer.toString(slotNumber);
        if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, key, false)) {
            // Set the variable to square Off the position
            if (myMIDetails.containsKey(key)) {
                myMIDetails.get(key).setSlotNumber(slotNumber);
                myMIDetails.get(key).setActionIndicator(MyManualInterventionClass.STOPMONITORING);
                // ..                
            }
        }          
    }
    
    void takeTradeLevelAction(int slotNum, int actionCode, String targetValue) {
        // If not stale then check if position at given slot number still exists
        // If position exists and matches the entrytime_name then use the action information to update 
        // corresponding shared array parameters to update  
        //
        //   1  - Square Off the trade / position at given slot number 
        //   2  - Update trade level stop loss to given value 
        //   3  - Update trade level take profit to given value
        //   4  - stop monitoring. (useful for graceful exit)
        String key = Integer.toString(slotNum);
        if (!(myMIDetails.containsKey(key))) { 
            myMIDetails.put(key, new MyManualInterventionClass(slotNum, "", 0));            
        }       
        switch (actionCode) {
            case 1: setTradeLevelSquareOff(slotNum);
                     break;
            case 2: setTradeLevelStopLoss(slotNum, targetValue);
                     break;
            case 3: setTradeLevelTakeProfit(slotNum, targetValue);
                     break;
            case 4: setTradeLevelStopMonitoring(slotNum);
                     break;                
            default: 
                     break;
        }
        
    }
    
    void setTradeLevelSquareOffAllOpenPositions() {

        String openPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "OPENPOSITIONSQUEUE", false);
        Jedis jedis = jedisPool.getResource(); 
        
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            for (String keyMap : retrieveMap.keySet()) {
                int slotNum = Integer.parseInt(keyMap);
                if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, keyMap, false)) {
                    if (!(myMIDetails.containsKey(keyMap))) { 
                        myMIDetails.put(keyMap, new MyManualInterventionClass(slotNum, "", 0));            
                    }                       
                    setTradeLevelSquareOff(slotNum);    
                }
            }  
        } catch (JedisException e) {  
            //if something wrong happen, return it back to the pool
            if (null != jedis) {  
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            }  
        } finally {
            //Return the Jedis instance to the pool once finished using it  
            if (null != jedis)
                jedisPool.returnResource(jedis);
        }
        
    }

    void setTradeLevelStopMonitoringAllOpenPositions() {

        String openPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "OPENPOSITIONSQUEUE", false);
        Jedis jedis = jedisPool.getResource(); 
        
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            for (String keyMap : retrieveMap.keySet()) {
                int slotNum = Integer.parseInt(keyMap);
                if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, keyMap, false)) {
                    if (!(myMIDetails.containsKey(keyMap))) { 
                        myMIDetails.put(keyMap, new MyManualInterventionClass(slotNum, "", 0));            
                    }                       
                    setTradeLevelStopMonitoring(slotNum);    
                }
            }  
        } catch (JedisException e) {  
            //if something wrong happen, return it back to the pool
            if (null != jedis) {  
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            }  
        } finally {
            //Return the Jedis instance to the pool once finished using it  
            if (null != jedis)
                jedisPool.returnResource(jedis);
        }
        
    }
    
    void setStrategyLevelMaxPositionSize(String targetValue) {
        if (myUtils.checkIfExistsHashMapField(jedisPool, redisConfigurationKey, "MAXNUMPAIRPOSITIONS", false)) {
            Jedis jedis = jedisPool.getResource();
            jedis.hset(redisConfigurationKey, "MAXNUMPAIRPOSITIONS", targetValue);       
            jedisPool.returnResource(jedis);            
        }          
    }
    
    void setStrategyLevelMinZScore(String targetValue) {
        if (myUtils.checkIfExistsHashMapField(jedisPool, redisConfigurationKey, "MINZSCORE", false)) {
            Jedis jedis = jedisPool.getResource();
            jedis.hset(redisConfigurationKey, "MINZSCORE", targetValue);       
            jedisPool.returnResource(jedis);            
        }                        
    }
    
    void setStrategyLevelMaxZScore(String targetValue) {
        if (myUtils.checkIfExistsHashMapField(jedisPool, redisConfigurationKey, "MAXZSCORE", false)) {
            Jedis jedis = jedisPool.getResource();
            jedis.hset(redisConfigurationKey, "MAXZSCORE", targetValue);       
            jedisPool.returnResource(jedis);            
        }                
    }
            
    void setStrategyLevelMinHalfLife(String targetValue) {
        if (myUtils.checkIfExistsHashMapField(jedisPool, redisConfigurationKey, "MINHALFLIFE", false)) {
            Jedis jedis = jedisPool.getResource();
            jedis.hset(redisConfigurationKey, "MINHALFLIFE", targetValue);       
            jedisPool.returnResource(jedis);            
        }          
    }
    
    void setStrategyLevelMaxHalfLife(String targetValue) {
        if (myUtils.checkIfExistsHashMapField(jedisPool, redisConfigurationKey, "MAXHALFLIFE", false)) {
            Jedis jedis = jedisPool.getResource();
            jedis.hset(redisConfigurationKey, "MAXHALFLIFE", targetValue);       
            jedisPool.returnResource(jedis);            
        }          
    }
    
    void setStrategyLevelMaxSpread(String targetValue) {
        if (myUtils.checkIfExistsHashMapField(jedisPool, redisConfigurationKey, "MAXALLOWEDPAIRSPREAD", false)) {
            Jedis jedis = jedisPool.getResource();
            jedis.hset(redisConfigurationKey, "MAXALLOWEDPAIRSPREAD", targetValue);       
            jedisPool.returnResource(jedis);            
        }          
    }   
    
    void takeStrategyLevelAction(int actionCode, String targetValue) {
        /*
            101  - square off all open positions / trade
            102  - update maximum position size as given value 0 - 10 (0 means no new position)
            103  - update Minimum Z Score to given value (0.5 as minmum, 3.0 as maximum)
            104  - update Maximum Z Score to given value (0.5 as minmum, 3.0 as maximum)
            105  - update Minimum Half life to given value (5 as minmum, 100 as maximum)
            106  - update Maximum Half life to given value (5 as minmum, 100 as maximum)
            107  - update Max allowed spread
            108  - stop monitoring all open positions
        */
        switch (actionCode) {
            case 101: setTradeLevelSquareOffAllOpenPositions();
                     break;
            case 102: setStrategyLevelMaxPositionSize(targetValue);
                     break;
            case 103: setStrategyLevelMinZScore(targetValue);
                     break;
            case 104: setStrategyLevelMaxZScore(targetValue);
                     break;
            case 105: setStrategyLevelMinHalfLife(targetValue);
                     break;
            case 106: setStrategyLevelMaxHalfLife(targetValue);
                     break;
            case 107: setStrategyLevelMaxSpread(targetValue);
                     break;
            case 108: setTradeLevelStopMonitoringAllOpenPositions();
                     break;
            default: 
                     break;
        }
    }

    @Override 
    public void run() {
        
        // Enter an infinite loop with blocking pop call to retireve messages from queue
        String manualInterventionSignalReceived = null;

        int eodExitTime = myExchangeObj.getExchangeCloseTimeHHMM();
        String eodExitTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EODEXITTIME", debugFlag);
        if (( eodExitTimeConfigValue != null) && (eodExitTimeConfigValue.length() > 0)) {
            eodExitTime = Integer.parseInt(eodExitTimeConfigValue);
        }        

        // while market is open. Now start monitoring the open positions queue
        while (myUtils.marketIsOpen(eodExitTime, myExchangeObj.getExchangeTimeZone(), false)) {
            manualInterventionSignalReceived = myUtils.popKeyValueFromQueueRedis(jedisPool,manualInterventionSignalsQueueKeyName,60,debugFlag);
            if (manualInterventionSignalReceived != null) {

                // Debug Message
                System.out.println(" Received Manual Intervention Signal as " + manualInterventionSignalReceived);                

                ManualInterventionSignalObject miSignal = new ManualInterventionSignalObject(manualInterventionSignalReceived);
                // check if current time is within stipulated is not stale by more than 5 minutes for trade level signal.

                if (miSignal.getApplyingLevel() == "S") {
                    // This signal is strategy level signal
                    takeStrategyLevelAction(miSignal.getActionCode(), miSignal.getTargetValue());
                } else if (miSignal.getApplyingLevel() == "T") {
                    // This signal is trade level signal
                    // Check if it is not more than 5 minutes stale 
                    Calendar timeNow = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());
                    if (!myUtils.checkIfStaleMessage(miSignal.getEntryTimeStamp(), String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow),5)) {
                        takeTradeLevelAction(miSignal.getSlotNumber(), miSignal.getActionCode(), miSignal.getTargetValue());
                    }
                }
            }
        }
        // Day Over. Now Exiting.
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