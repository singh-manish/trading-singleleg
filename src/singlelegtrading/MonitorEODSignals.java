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
import java.util.Map;
import java.util.TimeZone;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;

/**
 * @author Manish Kumar Singh
 */
public class MonitorEODSignals extends Thread {

    private Thread t;
    private String threadName = "MonitoringEndOfDayEntryExitSignalsThread";
    private boolean debugFlag;
    private JedisPool jedisPool;

    private String redisConfigurationKey;

    private IBInteraction ibInteractionClient;
    private MyExchangeClass myExchangeObj;

    private MyUtils myUtils;

    private String strategyName = "singlestr01";
    private String openPositionsQueueKeyName = "INRSTR01OPENPOSITIONS";
    private String entrySignalsQueueKeyName = "INRSTR01ENTRYSIGNALS";  
    private String manualInterventionSignalsQueueKeyName = "INRSTR01MANUALINTERVENTIONS";    
    private String eodEntryExitSignalsQueueKeyName = "INRSTR01EODENTRYEXITSIGNALS";

    public String exchangeHolidayListKeyName;
    
    public class MyExistingPosition {
        String symbolName;
        boolean exists;
        int slotNumber;
        int positionSideAndSize;
    }
    
    public class MyEntrySignalParameters {
        String elementName;
        int tradeSide, elementLotSize, halflife, positionRank;
        double zscore, dtsma200, onePctReturn, qscore, spread;
    }

    
    MonitorEODSignals(String name, JedisPool redisConnectionPool, String redisConfigKey, MyUtils utils, MyExchangeClass exchangeObj, IBInteraction ibInterClient, boolean debugIndicator) {

        threadName = name;
        debugFlag = debugIndicator;
        jedisPool = redisConnectionPool;
        ibInteractionClient = ibInterClient;
        myUtils = utils;
        redisConfigurationKey = redisConfigKey;

        myExchangeObj = exchangeObj;
        TimeZone.setDefault(myExchangeObj.getExchangeTimeZone());

        //"singlestr01", "INRSTR01OPENPOSITIONS", "INRSTR01ENTRYSIGNALS"        
        strategyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "STRATEGYNAME", false);
        openPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "OPENPOSITIONSQUEUE", false);
        entrySignalsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "ENTRYSIGNALSQUEUE", false);
        manualInterventionSignalsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "MANUALINTERVENTIONQUEUE", false);        
        eodEntryExitSignalsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EODENTRYEXITSIGNALSQUEUE", false);

        exchangeHolidayListKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EXCHANGEHOLIDAYLISTKEYNAME", false);

        // Debug Message
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Info : Monitoring End of Day Entry/Exit Signals for Strategy " + strategyName );
    }

    boolean hhmmWithinTenMinutes(String currentYYYYMMDDHHMMSS,String entryYYYYMMDDHHMMSS) {
        boolean retValue = false;
        
        if (currentYYYYMMDDHHMMSS.substring(8, 10).matches(entryYYYYMMDDHHMMSS.substring(8, 10))) {
            int currentmm = Integer.parseInt(currentYYYYMMDDHHMMSS.substring(10, 12));
            int entrymm = Integer.parseInt(entryYYYYMMDDHHMMSS.substring(10, 12));
            if (Math.abs(entrymm - entrymm) < 12) {
                retValue = true;
            }            
        }        
        return(retValue);
    }    
    
    void getExistingPositionDetails(String openPositionsQueueKeyName, MyExistingPosition currentPos) {

        currentPos.exists = false;
        currentPos.positionSideAndSize = 0;
        currentPos.slotNumber = 0;
        
        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            // Go through all open position slots to check for existance of Current Symbol        
            for (String keyMap : retrieveMap.keySet()) {
                // Do Stuff here             
                TradingObject myTradeObject = new TradingObject(retrieveMap.get(keyMap));
                if (myTradeObject.getTradingObjectName().matches(currentPos.symbolName)) {
                    // position for existing symbol exists. 
                    // Find the current Hour and minute.
                    String currentTimeStamp = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", Calendar.getInstance(myExchangeObj.getExchangeTimeZone()));
                    // Find the hour and minute at the time of posiion entry
                    String entryTimeStamp = myTradeObject.getEntryTimeStamp();
                    if (hhmmWithinTenMinutes(currentTimeStamp,entryTimeStamp)) {
                        // if hour is same and minute is within 10 minute of each other, then
                        // position belongs to current timeslot else not
                        // Timing slot of current position matches. Set the values of parameters...
                        currentPos.exists = true;
                        currentPos.positionSideAndSize = Integer.parseInt(myTradeObject.getSideAndSize());
                        currentPos.slotNumber = Integer.parseInt(keyMap);                        
                    }
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
            if (null != jedis) {
                jedisPool.returnResource(jedis);
            }
        }
        // Debug Messages if any
    }    

    void sendSquareOffSignal(String manualInterventionSignalsQueue, int slotNumber) {
        // perl code to push square off signal
        //$MISignalForRedis = $YYYYMMDDHHMMSS.",trade,".$positionID.",1,0";
        //$redis->lpush($queueKeyName, $MISignalForRedis);

        Calendar timeNow = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());        
        String squareOffSignal = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow) + ",trade," + slotNumber + ",1,0";

        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // push the square off signal
            jedis.lpush(manualInterventionSignalsQueue, squareOffSignal);
        } catch (JedisException e) {
            //if something wrong happen, return it back to the pool
            if (null != jedis) {
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            //Return the Jedis instance to the pool once finished using it  
            if (null != jedis) {
                jedisPool.returnResource(jedis);
            }
        }        
        
    }

    void sendEntrySignal(String entrySignalsQueue, MyEntrySignalParameters signalParam) {
        // R code to form entry signal
        //entrySignalForRedis <- paste0(timestamp,",","ON_",elementName,",",tradeSide,",",elementName,"_",elementLotSize,","
        //,zscore[1],",",t_10min$dtsma_200[lastIndex],",",halflife,",",onePercentReturn,",",positionRank,",",abs(predSVM),
        //",1,",t_10min$spread[lastIndex]);
        
        Calendar timeNow = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());        
        String timeStamp = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow);

        String entrySignal = timeStamp + "," + "ON_" + signalParam.elementName + "," + signalParam.tradeSide + "," + signalParam.elementName + "_" + signalParam.elementLotSize + ","
                + signalParam.zscore + "," + signalParam.dtsma200 + "," + signalParam.halflife + "," + signalParam.onePctReturn + "," + signalParam.positionRank + "," + signalParam.qscore 
                + ",1," + signalParam.spread;
        
        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // push the entry signal
            jedis.lpush(entrySignalsQueue, entrySignal);
        } catch (JedisException e) {
            //if something wrong happen, return it back to the pool
            if (null != jedis) {
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            //Return the Jedis instance to the pool once finished using it  
            if (null != jedis) {
                jedisPool.returnResource(jedis);
            }
        }        
        
    }
    
    @Override
    public void run() {

        int firstEntryOrderTime = 1515;
        String firstEntryOrderTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "FIRSTENTRYORDERTIME", debugFlag);
        if ((firstEntryOrderTimeConfigValue != null) && (firstEntryOrderTimeConfigValue.length() > 0)) {
            firstEntryOrderTime = Integer.parseInt(firstEntryOrderTimeConfigValue);
        }

        myUtils.waitForStartTime(firstEntryOrderTime, myExchangeObj.getExchangeTimeZone(), "first entry order time", false);

        String eodSignalReceived = null;

        int eodExitTime = 1530;
        String eodExitTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EODEXITTIME", debugFlag);
        if ((eodExitTimeConfigValue != null) && (eodExitTimeConfigValue.length() > 0)) {
            eodExitTime = Integer.parseInt(eodExitTimeConfigValue);
        }

        // Enter an infinite loop with blocking pop call to retireve messages from queue
        // while market is open keep monitoring eod signals queue
        while (myUtils.marketIsOpen(eodExitTime, myExchangeObj.getExchangeTimeZone(), false)) {
            eodSignalReceived = myUtils.popKeyValueFromQueueRedis(jedisPool, eodEntryExitSignalsQueueKeyName, 60, false);
            if (eodSignalReceived != null) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Info : Received EOD Signal as : " + eodSignalReceived);

                MyEntrySignalParameters signalParam = new MyEntrySignalParameters();
                String[] eodSignal = eodSignalReceived.split(",");
                // Structure of signal is SYMBOLNAME,LOTSIZE,RLSIGNAL,TSISIGNAL,HALFLIFE,CURRENTSTATE,ZSCORE,DTSMA200,ONEPCTRETURN,QSCORE,SPREAD
                // RLSIGNAL - Reinforced Learning Signal - can take values 0, +1, -1 
                // 0 meaning non action necessary
                // +1 meaning recommended Buy if no position/Square off if existing Short position
                // -1 meaning recommended Sell if no position/Square off if existing Long position
                // TSISIGNAL - True Strength Index Signal - can take values 0, +1, -1 
                // 0 meaning non action necessary
                // +1 meaning recommended Square off if existing Short position
                // -1 meaning recommended Square off if existing Long position
                
                // Structure of signal is SYMBOLNAME,LOTSIZE,RLSIGNAL,TSISIGNAL,HALFLIFE,CURRENTSTATE,ZSCORE,DTSMA200,ONEPCTRETURN,QSCORE,SPREAD

                Integer RLSignal = Integer.parseInt(eodSignal[2]);
                Integer TSISignal = Integer.parseInt(eodSignal[3]);

                signalParam.elementName = eodSignal[0];
                signalParam.elementLotSize = Integer.parseInt(eodSignal[1]);
                signalParam.tradeSide = RLSignal;
                signalParam.halflife = Integer.parseInt(eodSignal[4]);
                signalParam.positionRank = Integer.parseInt(eodSignal[5]); // currently state is populated in positionrank
                
                signalParam.zscore = Double.parseDouble(eodSignal[6]);
                signalParam.dtsma200 = Double.parseDouble(eodSignal[7]);
                signalParam.onePctReturn = Double.parseDouble(eodSignal[8]);
                signalParam.qscore = Double.parseDouble(eodSignal[9]);
                signalParam.spread = Double.parseDouble(eodSignal[10]);                
                
                MyExistingPosition currentExistingPos = new MyExistingPosition();
                currentExistingPos.symbolName = signalParam.elementName;

                getExistingPositionDetails(openPositionsQueueKeyName, currentExistingPos);
                
                // trading rule implemented :
                // if no position for given symbol then take position as per RLSIGNAL
                // else if LONG position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are -1
                // else if SHORT position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are +1
                
                // for taking position, send form the signal and send to entrySignalsQueue
                // for square off, send signal to manual intervention queue
                if (currentExistingPos.exists) {
                    //if LONG position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are -1
                    if (currentExistingPos.positionSideAndSize > 0) {
                        if ( (RLSignal < 0 ) || (TSISignal < 0) ) {
                            sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentExistingPos.slotNumber);
                            // if exit is due to RLSignal, then take opposite position ater exiting
                            if ( (RLSignal < 0 ) && (signalParam.tradeSide != 0)) {
                                sendEntrySignal(entrySignalsQueueKeyName, signalParam);                        
                            }                                                        
                        }
                    }
                    //if SHORT position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are +1                        
                    if (currentExistingPos.positionSideAndSize < 0 ) {
                        if ( (RLSignal > 0 ) || (TSISignal > 0) ) {
                            sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentExistingPos.slotNumber);
                            // if exit is due to RLSignal, then take do opposite position after exiting
                            if ( (RLSignal > 0 ) && (signalParam.tradeSide != 0)) {
                                sendEntrySignal(entrySignalsQueueKeyName, signalParam);                        
                            }                                                                                    
                        }                                                
                    }
                } else {
                    // since no position for given symbol so take position as per RLSIGNAL
                    if (signalParam.tradeSide != 0) {
                        sendEntrySignal(entrySignalsQueueKeyName, signalParam);                        
                    }
                }

            }
        }
        // Day Over. Now Exiting.
    }

    @Override
    public void start() {
        this.setName(threadName);
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }

}
