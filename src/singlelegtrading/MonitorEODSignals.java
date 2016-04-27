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

import java.util.Arrays;
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
        String elementName, elementStructure, signalTimeStamp, currentState, signalType;
        int tradeSide, elementLotSize, halflife, timeSlot, RLSignal, TSISignal;
        double zscore, dtsma200, onePctReturn, qscore, spread;
        
        // Structure of signal is SIGNALTYPE,SYMBOLNAME,LOTSIZE,RLSIGNAL,TSISIGNAL,HALFLIFE,CURRENTSTATE,ZSCORE,
        //    DTSMA200,ONEPCTRETURN,QSCORE,SPREAD,STRUCTURE,TIMESTAMP,SIGNALTYPE,TIMESLOT

        // Index Values for Object
        final int EODSIGNALSYMBOLNAME_INDEX = 0;
        final int EODSIGNALLOTSIZE_INDEX = 1;
        final int EODSIGNALRLSIGNAL_INDEX = 2;
        final int EODSIGNALTSISIGNAL_INDEX = 3;
        final int EODSIGNALHALFLIFE_INDEX = 4;
        final int EODSIGNALCURRENTSTATE_INDEX = 5;
        final int EODSIGNALZSCORE_INDEX = 6;
        final int EODSIGNALDTSMA200_INDEX = 7;
        final int EODSIGNALONEPCTRETURN_INDEX = 8;
        final int EODSIGNALQSCORE_INDEX = 9;
        final int EODSIGNALSPREAD_INDEX = 10;
        final int EODSIGNALSTRUCTURE_INDEX = 11;
        final int EODSIGNALTIMESTAMP_INDEX = 12;
        final int EODSIGNALSIGNALTYPE_INDEX = 13;
        final int EODSIGNALTIMESLOT_INDEX = 14;
        
        final int EODSIGNALMAX_ELEMENTS = 15;
        String[] eodSignalObjectStructure;        
        
        MyEntrySignalParameters(String incomingSignal) {
            eodSignalObjectStructure = new String[EODSIGNALMAX_ELEMENTS];

            if (incomingSignal.length() > 0) {
                String[] tempObjectStructure = incomingSignal.split(",");
                for (int index = 0; (index < tempObjectStructure.length) && (index < EODSIGNALMAX_ELEMENTS); index++) {
                        eodSignalObjectStructure[index] = tempObjectStructure[index];
                }
            }
            
            this.elementName = eodSignalObjectStructure[EODSIGNALSYMBOLNAME_INDEX];
            this.elementLotSize = Integer.parseInt(eodSignalObjectStructure[EODSIGNALLOTSIZE_INDEX]);
            this.RLSignal = Integer.parseInt(eodSignalObjectStructure[EODSIGNALRLSIGNAL_INDEX]);
            this.TSISignal = Integer.parseInt(eodSignalObjectStructure[EODSIGNALTSISIGNAL_INDEX]);            
            this.tradeSide = this.RLSignal;
            this.halflife = Integer.parseInt(eodSignalObjectStructure[EODSIGNALHALFLIFE_INDEX]);
            this.currentState = eodSignalObjectStructure[EODSIGNALCURRENTSTATE_INDEX];               
            this.zscore = Double.parseDouble(eodSignalObjectStructure[EODSIGNALZSCORE_INDEX]);
            this.dtsma200 = Double.parseDouble(eodSignalObjectStructure[EODSIGNALDTSMA200_INDEX]);
            this.onePctReturn = Double.parseDouble(eodSignalObjectStructure[EODSIGNALONEPCTRETURN_INDEX]);
            this.qscore = Double.parseDouble(eodSignalObjectStructure[EODSIGNALQSCORE_INDEX]);
            this.spread = Double.parseDouble(eodSignalObjectStructure[EODSIGNALSPREAD_INDEX]);
            this.elementStructure = eodSignalObjectStructure[EODSIGNALSTRUCTURE_INDEX];
            this.signalTimeStamp = eodSignalObjectStructure[EODSIGNALTIMESTAMP_INDEX];           
            this.signalType = eodSignalObjectStructure[EODSIGNALSIGNALTYPE_INDEX];
            this.timeSlot = Integer.parseInt(eodSignalObjectStructure[EODSIGNALTIMESLOT_INDEX]);            
        } 
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
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Info : Monitoring End of Day Entry/Exit Signals for Strategy " + strategyName + " queue Name " + eodEntryExitSignalsQueueKeyName);
    }

    int getNearestHundredStrikePrice(double indexLevel) {
        int returnValue = -1;
        
        returnValue = 100 * (int) (Math.round(indexLevel + 50) / 100);
        
        return(returnValue);
    }
    
    boolean subscribedToGivenTimeslot(String YYYYMMDDHHMMSS) {

        boolean retValue = false;
        String[] subscribedTimeslotList = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "SUBSCRIBEDTIMESLOTS", false).split(",");
        String HHMM = YYYYMMDDHHMMSS.substring(8, 10);
        int MM = Integer.parseInt(YYYYMMDDHHMMSS.substring(10, 12));
        if ((MM >= 0) && (MM < 15)) {
            HHMM = HHMM + "00";
        } else if ((MM >= 15) && (MM < 30)) {
            HHMM = HHMM + "15";
        } else if ((MM >= 30) && (MM < 45)) {
            HHMM = HHMM + "30";
        } else if (MM >= 45) {
            HHMM = HHMM + "45";
        }                    
        // Provision to check if current HHMM falls on any of the subsribed timeslots
        for (int index = 0; index < subscribedTimeslotList.length; index++) {
            if ( HHMM.equalsIgnoreCase(subscribedTimeslotList[index]) ) {
                retValue = true;
            }
        }

        if (retValue) {
            System.out.println("Found Timeslot Subscription for : " + YYYYMMDDHHMMSS);
        } else {
            System.out.println("Did not Find Timeslot Subscription for : " + YYYYMMDDHHMMSS + " Existing Subscriptions : " + Arrays.toString(subscribedTimeslotList));            
        }
        
        return (retValue);
    } // End of method
        
    boolean hhmmMatchesTimeSlot(int HHMM, String entryYYYYMMDDHHMMSS) {
        boolean retValue = false;

        int timeSlotHH = HHMM / 100;
        int timeSlotMM = HHMM % 100;        
        
        int entryHH = Integer.parseInt(entryYYYYMMDDHHMMSS.substring(8, 10));
        int entryMM = Integer.parseInt(entryYYYYMMDDHHMMSS.substring(10, 12));        
        if (entryHH == timeSlotHH) {
            if ((timeSlotMM == 0) && (entryMM >= 0) && (entryMM < 15)) {
                retValue = true;
            } else if ((timeSlotMM == 15) && (entryMM >= 15) && (entryMM < 30)) {
                retValue = true;            
            } else if ((timeSlotMM == 30) && (entryMM >= 30) && (entryMM < 45)) {
                retValue = true;            
            } else if ((timeSlotMM == 45) && (entryMM >= 45)) {
                retValue = true;            
            }                    
        }        

        // Debug Messages if any
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " Within given timeSlot OR not : " + retValue + " timeSlot " + HHMM + " timeSlotHH " + timeSlotHH + " timeSlotMM " + timeSlotMM + " for entry timeStamp as " + entryYYYYMMDDHHMMSS);
        
        return(retValue);
    }    
    
    void getExistingPositionDetails(String openPositionsQueueKeyName, String signalSymbolName, MyExistingPosition currentPos, int timeSlot) {

        currentPos.exists = false;
        currentPos.positionSideAndSize = 0;
        currentPos.slotNumber = 0;
        currentPos.symbolName = "";        
        
        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            // Go through all open position slots to check for existance of Current Symbol        
            for (String keyMap : retrieveMap.keySet()) {
                // Do Stuff here             
                TradingObject myTradeObject = new TradingObject(retrieveMap.get(keyMap));
                String entryTimeStamp = myTradeObject.getEntryTimeStamp();                
                if ( (!currentPos.exists) && 
                        (myTradeObject.getTradingObjectName().matches("ON_" + signalSymbolName)) && 
                        (hhmmMatchesTimeSlot(timeSlot, entryTimeStamp)) &&
                        (myTradeObject.getOrderState().equalsIgnoreCase("entryorderfilled")) ) {
                    // if already not found and position for existing symbol exists. 
                    // and if position belongs to timeslot
                    //Timing slot of current position matches. Set the values of parameters...
                    currentPos.exists = true;
                    currentPos.positionSideAndSize = myTradeObject.getSideAndSize();
                    currentPos.slotNumber = Integer.parseInt(keyMap);
                    currentPos.symbolName = myTradeObject.getTradingObjectName();
                    // Debug Messages if any
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " Found Existing Position " + currentPos.symbolName + " " + currentPos.exists + " " + currentPos.positionSideAndSize + " " + currentPos.slotNumber + " for timeSlot " + timeSlot);
                }
                // Debug Messages if any
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " Position Match Status " + signalSymbolName + " " + keyMap + " " + currentPos.exists + " " + currentPos.positionSideAndSize + " " + currentPos.slotNumber + " for timeSlot " + timeSlot);                
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
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Existing pos Vs New Position " + signalSymbolName + " " + currentPos.exists + " " + currentPos.positionSideAndSize + " " + currentPos.slotNumber);
        
    }    

    void sendUpdateStoplossLimitSignal(String manualInterventionSignalsQueue, int slotNumber) {
        // perl code to push stop loss signal

        Calendar timeNow = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());        
        String stopLossSignal = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow) + ",trade," + slotNumber + ",1,0";

        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // push the square off signal
//            jedis.lpush(manualInterventionSignalsQueue, stopLossSignal);
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

        // Check for subscription for given timeslot. Proceed only if subscribed
        if (!subscribedToGivenTimeslot(signalParam.signalTimeStamp)) {
            return;
        }
        
        String entrySignal = null;

        if ( signalParam.elementStructure.contains("_FUT") || signalParam.elementStructure.contains("_STK") ) {
            // this is for STK or FUT type of contract
            entrySignal = signalParam.signalTimeStamp + "," 
                    + "ON_" + signalParam.elementName + "," 
                    + signalParam.tradeSide + "," 
                    + signalParam.elementStructure + "_" + signalParam.elementLotSize + ","
                    + String.format( "%.2f", signalParam.zscore ) + "," 
                    + String.format( "%.2f", signalParam.dtsma200 ) + "," 
                    + signalParam.halflife + "," 
                    + String.format( "%.2f", signalParam.onePctReturn ) + "," 
                    + signalParam.currentState + "," 
                    + String.format( "%.5f", signalParam.qscore ) + ","
                    + "1" + "," 
                    + String.format( "%.2f", signalParam.spread );
        } else if (signalParam.elementStructure.contains("_OPT")) {
            // this is for OPT type of contract
            String optionRightType = "CALL";
            int optionTradeSide = 0;
            int optionStrikePrice = -1;
            if (signalParam.tradeSide > 0) {
                optionRightType = "CALL";
                optionTradeSide = 1 * signalParam.tradeSide;               
            } else if (signalParam.tradeSide < 0) {
                optionRightType = "PUT";
                optionTradeSide = -1 * signalParam.tradeSide;
            }
            optionStrikePrice = getNearestHundredStrikePrice(signalParam.spread/signalParam.elementLotSize);
            String optionStructure = signalParam.elementName + "_" + signalParam.elementLotSize + "_" + "OPT" + "_" + optionRightType + "_" + optionStrikePrice;
            entrySignal = signalParam.signalTimeStamp + "," 
                    + "ON_" + signalParam.elementName + "," 
                    + optionTradeSide + "," 
                    + optionStructure + ","
                    + String.format( "%.2f", signalParam.zscore ) + "," 
                    + String.format( "%.2f", signalParam.dtsma200 ) + "," 
                    + signalParam.halflife + "," 
                    + String.format( "%.2f", signalParam.onePctReturn ) + "," 
                    + signalParam.currentState + ","
                    + String.format( "%.5f", signalParam.qscore ) + ","
                    + "1" + "," 
                    + String.format( "%.2f", signalParam.spread );
        }

        if (entrySignal != null) {
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
        
    }
    
    void processEntryExitSignal(MyEntrySignalParameters signalParam) {
        // RLSIGNAL - Reinforced Learning Signal - can take values 0, +1, -1 
        // 0 meaning non action necessary
        // +1 meaning recommended Buy if no position/Square off if existing Short position
        // -1 meaning recommended Sell if no position/Square off if existing Long position
        // TSISIGNAL - True Strength Index Signal - can take values 0, +1, -1 
        // 0 meaning non action necessary
        // +1 meaning recommended Square off if existing Short position
        // -1 meaning recommended Square off if existing Long position

        MyExistingPosition currentExistingPos = new MyExistingPosition();
        getExistingPositionDetails(openPositionsQueueKeyName, signalParam.elementName, currentExistingPos, signalParam.timeSlot);
        // trading rule implemented :
        // if no position for given symbol then take position as per RLSIGNAL
        // else if LONG position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are -1
        // else if SHORT position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are +1

        // for taking position, send form the signal and send to entrySignalsQueue
        // for square off, send signal to manual intervention queue
        if (currentExistingPos.exists) {
            //if LONG position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are -1
            if (currentExistingPos.positionSideAndSize > 0) {
                if ( (signalParam.RLSignal < 0 ) || (signalParam.TSISignal < 0) ) {
                    sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentExistingPos.slotNumber);
                    // if exit is due to RLSignal, then take opposite position after exiting
                    if ( (signalParam.RLSignal < 0 ) && (signalParam.tradeSide != 0)) {
                        sendEntrySignal(entrySignalsQueueKeyName, signalParam);                        
                    }                                                        
                }
            }
            //if SHORT position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are +1                        
            if (currentExistingPos.positionSideAndSize < 0 ) {
                if ( (signalParam.RLSignal > 0 ) || (signalParam.TSISignal > 0) ) {
                    sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentExistingPos.slotNumber);
                    // if exit is due to RLSignal, then take do opposite position after exiting
                    if ( (signalParam.RLSignal > 0 ) && (signalParam.tradeSide != 0)) {
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

    void processExitSignalForMatchingPosition(MyEntrySignalParameters signalParam) {
        // symbolName, timeSlot and signalType are important. Other signal fields are redundant
        MyExistingPosition currentExistingPos = new MyExistingPosition();
        getExistingPositionDetails(openPositionsQueueKeyName, signalParam.elementName, currentExistingPos, signalParam.timeSlot);
        // trading rule implemented :
        // if matching position found then square off
        // for square off, send signal to manual intervention queue
        if (currentExistingPos.exists) {
            sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentExistingPos.slotNumber);
            // Debug Messages if any
           System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " Sent Square Off Signal for  " + currentExistingPos.slotNumber + " for symbol " + signalParam.elementName);           
        }                        
    }
    
    void processExitSignalForAllPositions(MyEntrySignalParameters signalParam) {
        // symbolName and signalType are important. Other signal fields are redundant
        // trading rule implemented :
        // square off all positions
        // for square off, send signal to manual intervention queue
        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            // Go through all open position slots to check for existance of Current Symbol        
            for (String keyMap : retrieveMap.keySet()) {
                // Do Stuff here             
                TradingObject myTradeObject = new TradingObject(retrieveMap.get(keyMap));
                if ( myTradeObject.getOrderState().equalsIgnoreCase("entryorderfilled") ) {
                    // position exists and suitable for square off
                    sendSquareOffSignal(manualInterventionSignalsQueueKeyName,Integer.parseInt(keyMap));                    
                    // Debug Messages if any
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " Squaring off Position " + keyMap + " for symbol " + signalParam.elementName);
                }
                // Debug Messages if any
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " Processed Square Off for " + keyMap + " for symbol " + signalParam.elementName);                
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

    }    

    void processUpdateStoplossLimitForMatchingPosition(MyEntrySignalParameters signalParam) {
        // symbolName, timeSlot and signalType are important. Other signal fields are redundant
        MyExistingPosition currentExistingPos = new MyExistingPosition();
        getExistingPositionDetails(openPositionsQueueKeyName, signalParam.elementName, currentExistingPos, signalParam.timeSlot);
        // trading rule implemented :
        // if matching position found then update stop loss limit
        // for square off, send signal to manual intervention queue
        if (currentExistingPos.exists) {
            sendUpdateStoplossLimitSignal(manualInterventionSignalsQueueKeyName,currentExistingPos.slotNumber);
            // Debug Messages if any
           System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " Sent Update Stop Loss Signal for  " + currentExistingPos.slotNumber + " for symbol " + signalParam.elementName);                            
        }                        
    }
    
    void processUpdateStoplossLimitForAllPositions(MyEntrySignalParameters signalParam) {
        // symbolName and signalType are important. Other signal fields are redundant
        // trading rule implemented :
        // square off all positions
        // for square off, send signal to manual intervention queue
        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            // Go through all open position slots to check for existance of Current Symbol        
            for (String keyMap : retrieveMap.keySet()) {
                // Do Stuff here             
                TradingObject myTradeObject = new TradingObject(retrieveMap.get(keyMap));
                if ( myTradeObject.getOrderState().equalsIgnoreCase("entryorderfilled") ) {
                    // position exists and suitable for square off
                    sendUpdateStoplossLimitSignal(manualInterventionSignalsQueueKeyName,Integer.parseInt(keyMap));                    
                    // Debug Messages if any
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " Updating Stop Loss for " + keyMap + " for symbol " + signalParam.elementName);
                }
                // Debug Messages if any
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " Processed Stop Loss Signal " + keyMap + " for symbol " + signalParam.elementName);                
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

                MyEntrySignalParameters signalParam = new MyEntrySignalParameters(eodSignalReceived);                
                // Structure of signal is SYMBOLNAME,LOTSIZE,RLSIGNAL,TSISIGNAL,HALFLIFE,CURRENTSTATE,ZSCORE,DTSMA200,ONEPCTRETURN,QSCORE,SPREAD,,STRUCTURE,TIMESTAMP,SIGNALTYPE,TIMESLOT
                System.out.println("timeSlot Subscription Status : " + subscribedToGivenTimeslot(signalParam.signalTimeStamp));
                
                if ( signalParam.signalType.equalsIgnoreCase("exitentry") || 
                        signalParam.signalType.equalsIgnoreCase("exitandentry") ||
                        signalParam.signalType.equalsIgnoreCase("entryexit") ||
                        signalParam.signalType.equalsIgnoreCase("entryandexit") ) {                   
                    processEntryExitSignal(signalParam);
                } else if (signalParam.signalType.equalsIgnoreCase("exitonlymatchingtimeslot")) {
                    processExitSignalForMatchingPosition(signalParam);               
                } else if (signalParam.signalType.equalsIgnoreCase("exitalltimeslots")) {
                    processExitSignalForAllPositions(signalParam);
                } else if (signalParam.signalType.equalsIgnoreCase("updatestoplosslimitmatchingtimeslot")) {
                    processUpdateStoplossLimitForMatchingPosition(signalParam);
                } else if (signalParam.signalType.equalsIgnoreCase("updatestoplosslimitalltimeslots")) {
                    processUpdateStoplossLimitForAllPositions(signalParam);
                }
                //write code to update take profit for all as well as matching timeslots
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
