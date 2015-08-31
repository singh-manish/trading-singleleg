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

import java.io.FileWriter;
import java.io.IOException;
import redis.clients.jedis.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Manish Kumar Singh
 */

public class SingleLegExit implements Runnable {
    private String threadName;
    private volatile boolean quit = false;   
    private String legDetails;
    private boolean debugFlag;
    private JedisPool jedisPool;
    private String redisConfigurationKey;
    private TimeZone exchangeTimeZone;
    private MyUtils myUtils;

    private IBInteraction ibInteractionClient;    

    private String strategyName;
    private String openPositionsQueueKeyName;
    private String closedPositionsQueueKeyName;
    private int slotNumber;
    private int positionQty = 0;

    private String entryOrderStatus;
    
    private boolean mktDataSubscribed = false;
    
    // Define class to store definition
    private class MyLegObj {
        String symbol, futExpiry;
        int legId, lotSize, qty, halfLife;
        String legEntryTimeStamp;
        double legEntryPrice;
        MyLegObj (int legIdentification, String newLegDetails) {
            legId = legIdentification;

            String pairElements[] = newLegDetails.split(",");
            legEntryTimeStamp = pairElements[0];
            qty = Integer.parseInt(pairElements[2]);        
            legEntryPrice = Double.parseDouble(pairElements[11]);
            halfLife = Math.round(Float.parseFloat(pairElements[6]));

            String pairStructure[] = pairElements[3].split("_");
            symbol = pairStructure[0];
            lotSize = Math.abs(Integer.parseInt(pairStructure[1])*qty);
            futExpiry = pairElements[12]; 
        }
    }

    // Define class to store last updated prices, volume and time of it
    private class MyTickObj {
        double firstSymbolLastPrice, firstSymbolClosePrice;
        double firstSymbolBidPrice, firstSymbolAskPrice;        
        long lastPriceUpdateTime,closePriceUpdateTime;
        double comboLastPrice, comboClosePrice;        
        MyTickObj() {
            lastPriceUpdateTime = -1;
            firstSymbolLastPrice = -1;
            closePriceUpdateTime = -1;
            firstSymbolClosePrice = -1;
            firstSymbolBidPrice = -1;
            firstSymbolAskPrice = -1;
            comboLastPrice = 0.0;
            comboClosePrice = 0.0;
        }
    }
    
    // Define class to store Range for each pair
    private class MyRangeActionObj {
        double stopLossLimit,takeProfitLimit;
        int pairId, deviation;
        boolean stopLossLimitBreached, takeProfitLimitBreached;
        String stopLossBreachActionStatus, takeProfitBreachActionStatus;
        long updatedtime;
        MyRangeActionObj(int identification, long lastUpdateTime) {
            pairId = identification;
            deviation = 50;
            stopLossLimitBreached = false;
            takeProfitLimitBreached = false;
            stopLossBreachActionStatus = "None";
            takeProfitBreachActionStatus = "None";              
            updatedtime = lastUpdateTime;                
        }        
    }   

    private int legOrderId = -1;
    private MyLegObj legObj;
    private MyTickObj tickObj;
    private MyRangeActionObj rangeLimitObj;        

    private double legFilledPrice = 0.0;
    private String bidAskDetails = "";    
    private String strategyExitType = "halflifebased";
    private String orderTypeToUse = "market"; // market, relativewithzeroaslimitwithamountoffset, relativewithmidpointaslimitwithamountoffset, relativewithzeroaslimitwithpercentoffset, relativewithmidpointaslimitwithpercentoffset 
    private double initialStopLoss = 6000.0;
    private double initialTakeProfit = 5000.0;
    private String fileNameForLimitsStatusUpdates;
    private String miKey;

    private ConcurrentHashMap<String, MyManualInterventionClass> myMIDetails;
    
    public String exchangeHolidayListKeyName;

    private int IBTICKARRAYINDEXOFFSET = 50;
    
  SingleLegExit(String name, JedisPool redisConnectionPool, IBInteraction ibIntClient, String redisConfigKey, TimeZone exTZ, int slotNum, ConcurrentHashMap<String, MyManualInterventionClass> miSignalMap, boolean debugIndicator){

        threadName = name;
        debugFlag = debugIndicator;
        jedisPool = redisConnectionPool;      
    
        myUtils = new MyUtils();
        
        ibInteractionClient = ibIntClient;         
        mktDataSubscribed = false;
        
        redisConfigurationKey = redisConfigKey; 
        exchangeTimeZone = exTZ;
        slotNumber = slotNum;
        miKey = Integer.toString(slotNumber);

        myMIDetails = miSignalMap;

        TimeZone.setDefault(exchangeTimeZone);
        
        strategyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "STRATEGYNAME", false);            
        openPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "OPENPOSITIONSQUEUE", false);            
        closedPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "CLOSEDPOSITIONSQUEUE", false);            
        strategyExitType = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EXITSTRATEGYTYPE", false);  
        orderTypeToUse = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "EXITORDERTYPE",false);
        initialStopLoss = Double.parseDouble(myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "INITIALSTOPLOSSAMOUNT",false));
        initialTakeProfit = Double.parseDouble(myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "INITIALTAKEPROFITAMOUNT",false));
        exchangeHolidayListKeyName = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "EXCHANGEHOLIDAYLISTKEYNAME",false); 
       
        String logDirectory = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "LOGDIRECTORY",false);
        if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), false)) {
            // Since position exists, get position details
            legDetails = myUtils.getHashMapValueFromRedis(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), debugFlag);

            legObj = new MyLegObj(slotNumber, legDetails);
            TradingObject myTradeObject = new TradingObject(legDetails);

            entryOrderStatus = myTradeObject.getOrderState();
            fileNameForLimitsStatusUpdates = logDirectory + "/" + strategyName + "/";
            fileNameForLimitsStatusUpdates = fileNameForLimitsStatusUpdates + myTradeObject.getEntryTimeStamp().substring(0, myTradeObject.getEntryTimeStamp().length()-2) + "00_ON_" + legObj.symbol + ".csv";
            
            rangeLimitObj = new MyRangeActionObj(slotNumber,Long.parseLong(myTradeObject.getLastUpdatedTimeStamp()));

            if (myUtils.checkIfExistsHashMapField(jedisPool,redisConfigurationKey, "INITIALSTOPLOSSTYPE",false)) {
                String initialStopLossType = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "INITIALSTOPLOSSTYPE",false);
                if (initialStopLossType.equalsIgnoreCase("fixedamount")) {
                    initialStopLoss = Double.parseDouble(myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "INITIALSTOPLOSSAMOUNT",false));
                } else if (initialStopLossType.equalsIgnoreCase("sigmafactor")) {
                    double stopLossFactor = Double.parseDouble(myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "INITIALSTOPLOSSSIGMAFACTOR",false));                            
                    initialStopLoss = Math.abs(stopLossFactor * Double.parseDouble(myTradeObject.getEntryStdDev()));                            
                }
            }         

            if (myUtils.checkIfExistsHashMapField(jedisPool,redisConfigurationKey, "INITIALTAKEPROFITTYPE",false)) {
                String initialTakeProfitType = myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "INITIALTAKEPROFITTYPE",false);
                if (initialTakeProfitType.equalsIgnoreCase("fixedamount")) {
                    initialStopLoss = Double.parseDouble(myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "INITIALTAKEPROFITAMOUNT",false));
                } else if (initialTakeProfitType.equalsIgnoreCase("sigmafactor")) {
                    double takeProfitFactor = Double.parseDouble(myUtils.getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "INITIALTAKEPROFITSIGMAFACTOR",false));                            
                    initialTakeProfit = Math.abs(takeProfitFactor * Double.parseDouble(myTradeObject.getEntryStdDev()));                            
                }
            }         
            
            if (rangeLimitObj.updatedtime < 0) {
                // This is first time monitoring has started as last update time is -1 or less than zero
                if (legObj.qty > 0) { 
                    // Pair is bought. Lower Breach is lower than current level while upper breach is higher than current level
                    rangeLimitObj.stopLossLimit = Double.parseDouble(myTradeObject.getEntrySpread()) - initialStopLoss;
                    rangeLimitObj.takeProfitLimit = Double.parseDouble(myTradeObject.getEntrySpread()) + initialTakeProfit;
                } else if (legObj.qty < 0 ) {
                    // Pair is Shorted. Lower Breach is higher than current level while upper breach is lower than current level
                    rangeLimitObj.stopLossLimit = Double.parseDouble(myTradeObject.getEntrySpread()) + initialStopLoss;
                    rangeLimitObj.takeProfitLimit = Double.parseDouble(myTradeObject.getEntrySpread()) - initialTakeProfit;                
                }                
            } else {
                // restarting the monitoring as last updated time is positive/greater than zero
                rangeLimitObj.stopLossLimit = Double.parseDouble(myTradeObject.getLowerBreach());
                rangeLimitObj.takeProfitLimit = Double.parseDouble(myTradeObject.getUpperBreach());                
            }            

        }

        positionQty = legObj.qty;         
        tickObj = new MyTickObj();
                
   }
   
   public void terminate() {
       quit = true;
   }

    @Override 
    public void run() {     
              
        quit = false;
        
        if (entryOrderStatus.matches("entryorderfilled")) {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Info : Starting Monitoring for Leg " +  legObj.symbol );            
        } else {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Info : Not Starting Monitoring for Leg " +  legObj.symbol + " as Entry Order is not updated as filled (should be entryorderfilled) in open positions queue. Current Status : " + entryOrderStatus);                        
            terminate();           
        }
        
        TimeZone.setDefault(exchangeTimeZone);
        while (!quit){
            myUtils.waitForNMiliSeconds(1000); // Check every 1 sec. Can be made more frequent but not sure if adds any value.                       
            Calendar timeNow = Calendar.getInstance(exchangeTimeZone);        
            int lastExitOrderTime = 1528;
            String lastExitOrderTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "LASTEXITORDERTIME", false);
            if (( lastExitOrderTimeConfigValue != null) && (lastExitOrderTimeConfigValue.length() > 0)) {
                lastExitOrderTime = Integer.parseInt(lastExitOrderTimeConfigValue);
            } 
            // Provision for exiting if time has reached outside market hours for exchange - say NSE or NYSE
            if (Integer.parseInt(String.format("%1$tH%1$tM",timeNow)) >= lastExitOrderTime) {
                if (debugFlag) {
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + legObj.symbol + " : " + "Reached last Exit Order Time at : " + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow) + " for Leg : " + legObj.symbol + " with lastExitOrderTIme as : " + lastExitOrderTime);                            
                }
                if (checkForSquareOffAtEOD()) {
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Exiting Position : " + legObj.symbol + " as square Off at EOD is true - either due to last day of expiry OR it is intra-day strategy");                            
                    orderTypeToUse = "market"; // Since it is end of Day trade with Markets about to close, use market order type irresepctive of what is configured
                    squareOffLegPosition(legObj);
                    if (positionQty == 0) {
                        updatePositionStatusInQueues(String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow)); 
                    }                    
                }
                terminate();
            }

            if ((myMIDetails.get(miKey).getActionIndicator() == MyManualInterventionClass.STOPMONITORING) && (!quit)) {
                terminate();
            }            
            
            if ((myMIDetails.get(miKey).getActionIndicator() == MyManualInterventionClass.SQUAREOFF) && (!quit)){
                System.out.println("Exiting Leg Position : " + legObj.symbol + " as manual Intervention Signal to Square Off received");                            
                squareOffLegPosition(legObj);
                if (positionQty == 0) {
                    updatePositionStatusInQueues(String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow));
                    terminate();
                }
            }

            if ((myMIDetails.get(miKey).getActionIndicator() == MyManualInterventionClass.UPDATESTOPLOSS) && (!quit)) {
                double tempLimit;
                try {
                    tempLimit = Double.parseDouble(myMIDetails.get(miKey).getTargetValue());
                    rangeLimitObj.stopLossLimit = tempLimit;
                } catch (Exception ex) {}
            }

            if ((myMIDetails.get(miKey).getActionIndicator() == MyManualInterventionClass.UPDATETAKEPROFIT) && (!quit)) {
                double tempLimit;
                try {
                    tempLimit = Double.parseDouble(myMIDetails.get(miKey).getTargetValue());
                    rangeLimitObj.takeProfitLimit = tempLimit;
                } catch (Exception ex) {}
            }
            
            if (!quit) {            
                if (!ibInteractionClient.waitForConnection(180)) {
                    // Even After waiting 180 seconds Connection is not available. Quit now...
                    terminate();
                    if (debugFlag) {
                        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "IB connection not available to monitor. Exiting the thread for Leg : " + legObj.symbol);                            
                    }                                    
                } else {
                    if (!mktDataSubscribed) {
                        ibInteractionClient.cancelMktDataSubscription(slotNumber); // In case subscription is still available.. 
                        if (legObj.futExpiry.equalsIgnoreCase("000000")) {
                            ibInteractionClient.requestStkMktDataSubscription(slotNumber, legObj.symbol);
                        } else {
                            ibInteractionClient.requestFutMktDataSubscription(slotNumber, legObj.symbol, legObj.futExpiry);
                        }
                        mktDataSubscribed = true;                 
                    }
                    if (mktDataSubscribed) {
                        updateTickObj();                        
                        int timeOut = 0;
                        while ( (tickObj.firstSymbolLastPrice <= 0) &&
                            (timeOut < 35) ) {
                            // Wait for 30 seconds before resubscribing
                            myUtils.waitForNSeconds(5);
                            timeOut += 5;
                            updateTickObj();                                                    
                        }
                        if (tickObj.firstSymbolLastPrice == 0) {
                            // resubscribe
                            mktDataSubscribed = false;                            
                        }
                    }
                    // Update prices
                    updateTickObj();
                    // Calculate the Breach Status 
                    calculateBreach(legObj,rangeLimitObj);
                    // Act on Breach based on action parameter
                    actOnBreach(legObj,rangeLimitObj);
                    if (positionQty == 0) {
                        updatePositionStatusInQueues(String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow)); 
                        terminate();
                    } else {
                        // Update position status every few seconds - currently 10 seconds. Works well with 20.
                        if (Integer.parseInt(String.format("%1$tH%1$tM%1$tS",timeNow)) % 10 == 0 ) {
                            updatePositionStatus(rangeLimitObj);
                        }
                    }
                } // else of if ibInteractionClient.waitForConnection(180)
            } // if !quit

            // Check if last updated timestamp for rangeLimitObj and current timestamp is more than 5 minutes
            // if it is more than 5 minutes, means subscribed rates are not coming in.
            // if rates are not coming in then make quit true so that thread gets terminated.
            if ((Integer.parseInt(String.format("%1$tS",timeNow)) % 30 == 5 ) &&  (rangeLimitObj.updatedtime > 0) ){
                long updatedTimeStalenessInSeconds = (System.currentTimeMillis() - rangeLimitObj.updatedtime) / 1000;
                if (updatedTimeStalenessInSeconds > 300) {
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Subscribed Rates are getting Stale. Exiting the thread for symbol : " + legObj.symbol );                            
                    terminate();
                    //quit = true;
                }
            }
            
            if ((debugFlag) &&
                (Integer.parseInt(String.format("%1$tS",timeNow)) % 30 == 1 ) &&
                (tickObj.firstSymbolLastPrice > 0)
            ) {
                // Write to file the data
                String timeOne = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(rangeLimitObj.updatedtime);                
                String outputToWrite = rangeLimitObj.pairId +
                        "," + timeOne +
                        "," + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow) +
                        "," + rangeLimitObj.stopLossLimitBreached +
                        "," + rangeLimitObj.takeProfitLimitBreached +
                        "," + rangeLimitObj.deviation + 
                        "," + rangeLimitObj.stopLossLimit +
                        "," + rangeLimitObj.takeProfitLimit + 
                        "," + rangeLimitObj.stopLossBreachActionStatus +
                        "," + rangeLimitObj.takeProfitBreachActionStatus +
                        "," + tickObj.comboLastPrice +
                        "," + tickObj.comboClosePrice +
                        "," +
                        "," + positionQty;
                updateLimitsStatusToFile(fileNameForLimitsStatusUpdates, outputToWrite);
            }            
        }        
        
        
        // Exited Position or Markets are closing. Now Exiting.

        // Cancel Market Data Request     
        ibInteractionClient.cancelMktDataSubscription(slotNumber); // In case subscription is still available..
        
        if (debugFlag) {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Info : Stopped Monitoring of Leg " + legObj.symbol + ". Exiting thread Now. Thread name : " + threadName );
        }
    }    
    

    void updateLimitsStatusToFile(String fileNameIdentifier, String dataToWrite) {

        try {
            FileWriter writer = new FileWriter(fileNameIdentifier,true);

            writer.append(dataToWrite);
            writer.append('\n');
            writer.flush();
            writer.close();
        }
        catch(IOException ex)
        {
             ex.printStackTrace();
        } 

    }

    
    boolean checkForSquareOffAtEOD() {
    
        boolean returnValue = false;
        
        if (!legObj.futExpiry.equalsIgnoreCase("000000")) {
            String previousFutExpiry = myUtils.getKeyValueFromRedis(jedisPool, "INRFUTPREVIOUSEXPIRY", debugFlag);
            // return true if expiry is same as previous expiry - usually to be set on day of expiry
            if (previousFutExpiry.length() > 0) {
                if (legObj.futExpiry.matches(previousFutExpiry)) {
                    returnValue = true;
                }
            }            
        }
        String localString = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EODEXPIRY", debugFlag);
        if (localString != null) {
            int endOfDayExpiry = Integer.parseInt(localString);
            if (endOfDayExpiry > 0) {
                returnValue = true;            
            }            
        }

        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Info : Checking for Square Off at EOD for Leg " + legObj.symbol + ". Returning : " + returnValue );
        return(returnValue);
    }   

    boolean checkForIndividualLimitsAtEOD() {
    
        boolean returnValue = false;
        // legObj has leg details;
        // tickObj has tick details;
        // rangeLimitObj has range details; 
        
        if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), debugFlag)) {
            if (strategyExitType.equalsIgnoreCase("halflifebased")) {
                    returnValue = false;                
            } else {
                // Since position exists, get exit status in case some profits are made or it is already at some stop loss beyond limit
                if (legObj.qty > 0) {
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Info : EOD PnL for Leg " + legObj.symbol + " : " + (tickObj.comboLastPrice - legObj.legEntryPrice) + " i.e. " + "tickObj.comboLastPrice as " + tickObj.comboLastPrice + "legObj.legEntryPrice as " + legObj.legEntryPrice + " against 0.4 * Take Profit : " + (0.4 * initialTakeProfit) + " and 0.8 * -1 * Stop Loss : " + (0.8 * -1 * initialStopLoss) );                
                    // Leg is bought. Current price should be higher than current level for it to be in profit
                    if ((tickObj.comboLastPrice - legObj.legEntryPrice) > 0.4 * initialTakeProfit) {
                        returnValue = true;
                    } else if ((tickObj.comboLastPrice - legObj.legEntryPrice) < 0.8 * -1 * initialStopLoss) {
                        returnValue = true;                    
                    }
                } else if (legObj.qty < 0 ) {
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Info : EOD PnL for Leg " + legObj.symbol + " : " + (legObj.legEntryPrice - tickObj.comboLastPrice) + " i.e. " + "legObj.legEntryPrice as " + legObj.legEntryPrice + "tickObj.comboLastPrice as " + tickObj.comboLastPrice + " against 0.4 * Take Profit : " + (0.4 * initialTakeProfit) + " and 0.8 * -1 * Stop Loss : " + (0.8 * -1 * initialStopLoss) );                
                    // Leg is Shorted. Current price should be Lower than current level for it to be in profit
                    if ((legObj.legEntryPrice - tickObj.comboLastPrice) > 0.4 * initialTakeProfit) {
                        returnValue = true;
                    } else if ((legObj.legEntryPrice - tickObj.comboLastPrice) < 0.8 * -1 * initialStopLoss) {
                        returnValue = true;                    
                    }
                }                                 
            }            
        }
        return(returnValue);
    }
    
    void updatePositionStatusInQueues(String timeNow) {

        String myLegDetails = myUtils.getHashMapValueFromRedis(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), debugFlag);                    
        if (getOpenPositionSlotOrderStatus().equalsIgnoreCase("exitorderfilled")) {
            // update Redis queues with squared off order
            updateClosedPositionsQueue(closedPositionsQueueKeyName, myLegDetails, legFilledPrice, timeNow, legOrderId);
            // To update Open position queue, delete the key name from Redis Open Position queue to indicate availability of slot.
            Jedis jedis = jedisPool.getResource();
            jedis.hdel(openPositionsQueueKeyName, Integer.toString(slotNumber));
            jedisPool.returnResource(jedis);
        } else if (getOpenPositionSlotOrderStatus().equalsIgnoreCase("exitordersenttoexchange")) {
            // update Redis queues with squared off order
            updateOpenPositionsQueueWithIncompletelyFilledOrderStatus(openPositionsQueueKeyName, myLegDetails, legFilledPrice, timeNow, legOrderId);            
        } 
    }   

    void updateOpenPositionsQueueWithIncompletelyFilledOrderStatus(String queueKeyName, String updateDetails, double exitSpread, String exitTimeStamp, int exitOrderId) {
        
        TradingObject myTradeObject = new TradingObject(updateDetails);
                
        myTradeObject.setExitSpread(exitSpread);
        myTradeObject.setExitTimeStamp(exitTimeStamp);
        myTradeObject.setExitOrderIDs(exitOrderId);
        myTradeObject.setExitBidAskFillDetails(bidAskDetails);
                
        if (debugFlag) {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Updating Open Position Details for Incompletely Filled Order for Slot Number : " + slotNumber);
        }
        
        // Update the closed position with exit Details Signal
        Jedis jedis = jedisPool.getResource();
        jedis.hset(queueKeyName, Integer.toString(slotNumber), myTradeObject.getCompleteTradingObjectString());
        jedisPool.returnResource(jedis);
    }
    
    void updateClosedPositionsQueue(String queueKeyName, String updateDetails, double exitSpread, String exitTimeStamp, int exitOrderId) {

        TradingObject myTradeObject = new TradingObject(updateDetails);
                
        myTradeObject.setExitSpread(exitSpread);
        myTradeObject.setExitTimeStamp(exitTimeStamp);
        myTradeObject.setExitOrderIDs(exitOrderId);
        myTradeObject.setExitBidAskFillDetails(bidAskDetails);
        
        // Find the field value of hash to update exited position
        int indexNumber = 0;
        boolean found = false;
        while (found == false) {
            indexNumber++;
            if (!myUtils.checkIfExistsHashMapField(jedisPool, queueKeyName, Integer.toString(indexNumber), debugFlag)) {
                found = true;
            }
        }        
        if (debugFlag) {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Updating and Closing Position Details for Completely Filled Order for Slot Number : " + slotNumber);
        }
        // Update the closed position with exit Details Signal
        Jedis jedis = jedisPool.getResource();
        jedis.hset(queueKeyName, Integer.toString(indexNumber), myTradeObject.getCompleteTradingObjectString());
        jedisPool.returnResource(jedis);
    }

    void updateTickObj(){

        tickObj.firstSymbolBidPrice = ibInteractionClient.myTickDetails.get(slotNumber).getSymbolBidPrice();
        tickObj.firstSymbolAskPrice = ibInteractionClient.myTickDetails.get(slotNumber).getSymbolAskPrice();
        tickObj.firstSymbolLastPrice = ibInteractionClient.myTickDetails.get(slotNumber).getSymbolLastPrice();
        tickObj.firstSymbolClosePrice = ibInteractionClient.myTickDetails.get(slotNumber).getSymbolClosePrice();
        tickObj.lastPriceUpdateTime = ibInteractionClient.myTickDetails.get(slotNumber).getLastPriceUpdateTime();
        tickObj.closePriceUpdateTime = ibInteractionClient.myTickDetails.get(slotNumber).getClosePriceUpdateTime();        

        if (tickObj.firstSymbolLastPrice > 0) {
            tickObj.comboLastPrice = tickObj.firstSymbolLastPrice * legObj.lotSize;            
        }
        
        if ((legObj.qty > 0) && (tickObj.firstSymbolBidPrice > 0) ) {
            // Leg was bought at the the time of taking position. Leg would be sold for squaring off
            tickObj.comboLastPrice = tickObj.firstSymbolBidPrice * legObj.lotSize;                            
        } else if ((legObj.qty < 0) && (tickObj.firstSymbolAskPrice > 0) ) {
            // Leg was shorted at the the time of taking position. Leg would be bought for squaring off
            tickObj.comboLastPrice = tickObj.firstSymbolAskPrice * legObj.lotSize;                                            
        }
            
        if (tickObj.firstSymbolClosePrice > 0) {
            tickObj.comboClosePrice = tickObj.firstSymbolClosePrice * legObj.lotSize;            
        }
    }    
    
    void calculateBreach(MyLegObj legDef, MyRangeActionObj rangeLimits){

        rangeLimits.stopLossLimitBreached = false;
        rangeLimits.takeProfitLimitBreached = false;
        double legLastPrice = 0;    

        if (tickObj.firstSymbolLastPrice > 0) {
            if (tickObj.lastPriceUpdateTime > 0 ) {
                legLastPrice = tickObj.comboLastPrice;
            } 
            rangeLimits.updatedtime = tickObj.lastPriceUpdateTime;

            if ( legDef.qty > 0 ) {
                if ( legLastPrice <= rangeLimits.stopLossLimit ) {
                    if (debugFlag) {
                        System.out.println(legObj.symbol + " : " + "Breached Stop Loss Limit on positive Quantity. legLastPrice : " + legLastPrice + " stopLossBreachLimit :" + rangeLimits.stopLossLimit);
                    }
                    rangeLimits.stopLossLimitBreached = true;
                    rangeLimits.deviation = - 99;               
                } else if ( legLastPrice > rangeLimits.takeProfitLimit ) {
                    if (debugFlag) {
                        System.out.println(legObj.symbol + " : " + "Breached Take Profit Limit on positive Quantity. legLastPrice : " + legLastPrice + " takeProfitBreachLimit :" + rangeLimits.takeProfitLimit);
                    }                
                    rangeLimits.takeProfitLimitBreached = true;
                    rangeLimits.deviation = 199;
                } else {
                    rangeLimits.deviation = (int) (100 * (legLastPrice - rangeLimits.stopLossLimit) / (rangeLimits.takeProfitLimit - rangeLimits.stopLossLimit));
                }            
            } else if ( legDef.qty < 0 ) {
                if ( legLastPrice > rangeLimits.stopLossLimit ) {
                    if (debugFlag) {
                        System.out.println(legObj.symbol + " : " + "Breached Stop Loss Limit on negative Quantity. legLastPrice : " + legLastPrice + " stopLossBreachLimit :" + rangeLimits.stopLossLimit);
                    }
                    rangeLimits.stopLossLimitBreached = true;
                    rangeLimits.deviation = - 99;               
                } else if ( legLastPrice < rangeLimits.takeProfitLimit ) {
                    if (debugFlag) {
                        System.out.println(legObj.symbol + " : " + "Breached Take Profit Limit on positive Quantity. legLastPrice : " + legLastPrice + " stopLossBreachLimit :" + rangeLimits.takeProfitLimit);
                    }                                
                    rangeLimits.takeProfitLimitBreached = true;
                    rangeLimits.deviation = 199;
                } else {
                    rangeLimits.deviation = (int) (100 * (rangeLimits.stopLossLimit - legLastPrice) / (rangeLimits.stopLossLimit - rangeLimits.takeProfitLimit));
                }                        
            } else {
                if (debugFlag) {
                    System.out.println(legObj.symbol + " : " + "Zero Quantity. Inside Calculate Breach - Which is Error Condition");
                }            
            }            
        }

    } // End of calculateBreach    

    void actOnBreach(MyLegObj legDef, MyRangeActionObj rangeLimits){

        if (rangeLimits.stopLossLimitBreached && rangeLimits.stopLossBreachActionStatus.equalsIgnoreCase("none")) {
            // Actions to be taken with respect to stopLossLimitBreach
            takeActionIfLimitsBreached(legDef,rangeLimits);           
        }
        if (rangeLimits.takeProfitLimitBreached && rangeLimits.takeProfitBreachActionStatus.equalsIgnoreCase("none")) {
            // Actions to be taken with respect to takeProfitLimitBreach
            takeActionIfLimitsBreached(legDef,rangeLimits);                        
        }

    } // End of actOnBreach
    
    void takeActionIfLimitsBreached(MyLegObj legDef, MyRangeActionObj rangeLimit){

        if ((rangeLimit.stopLossLimitBreached) || (rangeLimit.takeProfitLimitBreached)) {
            updateActionTakenStatus(rangeLimit,"Initiated");
            if (rangeLimit.stopLossLimitBreached) {
                squareOffLegPosition(legDef);
                updateActionTakenStatus(rangeLimit,"SquaredOff");                    
            } else if (rangeLimit.takeProfitLimitBreached) {
                squareOffLegPosition(legDef);
                updateActionTakenStatus(rangeLimit,"SquaredOff");                        
            }
        }            
    } // End of takeActionIfLimitsBreached   

    boolean exitOrderCompletelyFilled(int orderId, int maxWaitTime) {

        ibInteractionClient.ibClient.reqOpenOrders();
        int timeOut = 0;
        while ( (ibInteractionClient.myOrderStatusDetails.get(orderId).getRemainingQuantity() != 0) &&
                (timeOut < maxWaitTime)) {
            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Waiting for Order to be filled for  Order id "  +  ibInteractionClient.myOrderStatusDetails.get(orderId).getOrderId() + " for " + timeOut + " seconds");
            } 
            timeOut += 10;                
            myUtils.waitForNSeconds(5);
            // Check if following needs to be commented
            ibInteractionClient.ibClient.reqOpenOrders();
            myUtils.waitForNSeconds(5);
        }
        if (ibInteractionClient.myOrderStatusDetails.get(orderId).getRemainingQuantity() == 0) {
            return(true);
        } else {
            return(false);
        }
        
    }

    int placeConfiguredOrder(String symbolName, int quantity, String expiry, String mktAction) {

        int returnOrderId = 0;
        // Possible order types are as follows
        // market, relativewithzeroaslimitwithamountoffset, relativewithmidpointaslimitwithamountoffset, relativewithzeroaslimitwithpercentoffset, relativewithmidpointaslimitwithpercentoffset 
        if (orderTypeToUse.equalsIgnoreCase("market")) {
            if (expiry.equalsIgnoreCase("000000")) {
                // Place order for STK type
                returnOrderId = ibInteractionClient.placeStkOrderAtMarket(symbolName, quantity, mktAction, strategyName, true);                
            } else {
                // Place Order for FUT type
                returnOrderId = ibInteractionClient.placeFutOrderAtMarket(symbolName, quantity, expiry, mktAction, strategyName, true);                
            }
        } else if (orderTypeToUse.equalsIgnoreCase("relativewithzeroaslimitwithamountoffset")) {
            double limitPrice = 0.0; // For relative order, Limit price is suggested to be left as zero
            double offsetAmount = 0.0; // zero means it will take default value based on exchange / timezone
            if (expiry.equalsIgnoreCase("000000")) {
                // Place order for STK type
                returnOrderId = ibInteractionClient.placeStkOrderAtRelative(symbolName, quantity, mktAction, strategyName, limitPrice, offsetAmount, true);            
            } else {
                // Place Order for FUT type
                returnOrderId = ibInteractionClient.placeFutOrderAtRelative(symbolName, quantity, expiry, mktAction, strategyName, limitPrice, offsetAmount, true);            
            }
        }
        
        return(returnOrderId);
    }    
    
    void squareOffLegPosition(MyLegObj legDef) {

        if (positionQty != 0) {
            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Squaring Off legId :" + legDef.legId + " : Symbol :" + legDef.symbol);
            }

            setOpenPositionSlotOrderStatus("exitorderinitiated");                                                                    
            // Place market Order with IB for squaring Off
            if (legDef.qty > 0) {
                // Leg was bought at the the time of taking position. It would be sold for squaring off
                // Place Order and get the order ID
                if (legDef.futExpiry.equalsIgnoreCase("000000")) {
                    // for STK type
                    ibInteractionClient.getBidAskPriceForStk(slotNumber + IBTICKARRAYINDEXOFFSET, legDef.symbol);
                } else {
                    // for FUT type
                    ibInteractionClient.getBidAskPriceForFut(slotNumber + IBTICKARRAYINDEXOFFSET, legDef.symbol, legDef.futExpiry);
                }  
                legOrderId = placeConfiguredOrder(legDef.symbol, Math.abs(legDef.lotSize), legDef.futExpiry, "SELL");
                bidAskDetails = legDef.symbol + "_" + ibInteractionClient.myBidAskPriceDetails.get(slotNumber).getSymbolBidPrice() + "_" + ibInteractionClient.myBidAskPriceDetails.get(slotNumber).getSymbolAskPrice() ;                
            } else if (legDef.qty < 0){
                // Leg was shorted at the the time of taking position. leg would be bought for squaring off
                if (legDef.futExpiry.equalsIgnoreCase("000000")) {
                    // for STK type
                    ibInteractionClient.getBidAskPriceForStk(slotNumber + IBTICKARRAYINDEXOFFSET, legDef.symbol);
                } else {
                    // for FUT type
                    ibInteractionClient.getBidAskPriceForFut(slotNumber + IBTICKARRAYINDEXOFFSET, legDef.symbol, legDef.futExpiry);
                }  
                legOrderId = placeConfiguredOrder(legDef.symbol, Math.abs(legDef.lotSize), legDef.futExpiry, "BUY");
                bidAskDetails = legDef.symbol + "_" + ibInteractionClient.myBidAskPriceDetails.get(slotNumber).getSymbolBidPrice() + "_" + ibInteractionClient.myBidAskPriceDetails.get(slotNumber).getSymbolAskPrice() ;                                
            }

            if (legOrderId > 0)  {
                setOpenPositionSlotOrderStatus("exitordersenttoexchange");                
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Square Off Order for " + legDef.symbol + " initiated with orderid as " + legOrderId );  

                // Wait for orders to be completely filled            
                if  (exitOrderCompletelyFilled(legOrderId, 750)) {                  
                    setOpenPositionSlotOrderStatus("exitorderfilled");                                                        
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Exit Order filled for Order id " +  legOrderId + " at avg filled price " + ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice());
                    if (legDef.qty > 0) {
                        // Leg was bought at the the time of taking position. Leg would be sold for squaring off
                        legFilledPrice = ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice() * legObj.lotSize;                
                        bidAskDetails = legDef.symbol + "_" + ibInteractionClient.myBidAskPriceDetails.get(slotNumber).getSymbolBidPrice() + "_" + ibInteractionClient.myBidAskPriceDetails.get(slotNumber).getSymbolAskPrice() ;                
                        bidAskDetails = bidAskDetails + "__" + legOrderId + "_" + legDef.symbol + "_" + ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice();                        
                    } else if (legDef.qty < 0){
                        // Leg was shorted at the the time of taking position. leg would be bought for squaring off
                        legFilledPrice =  ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice() * legObj.lotSize ;                
                        bidAskDetails = legDef.symbol + "_" + ibInteractionClient.myBidAskPriceDetails.get(slotNumber).getSymbolBidPrice() + "_" + ibInteractionClient.myBidAskPriceDetails.get(slotNumber).getSymbolAskPrice() ;                                
                        bidAskDetails = bidAskDetails + "__" + legOrderId + "_" + legDef.symbol + "_" + ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice();                        
                    }
                } else {                  
                    ibInteractionClient.requestExecutionDetailsHistorical(legOrderId,1);
                    myUtils.waitForNSeconds(30);
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Exit Order filled for Order id " +  legOrderId + " at avg filled price " + ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice());
                    if (legDef.qty > 0) {
                        // Leg was bought at the the time of taking position. Leg would be sold for squaring off
                        legFilledPrice = ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice() * legObj.lotSize;                
                        bidAskDetails = legDef.symbol + "_" + ibInteractionClient.myBidAskPriceDetails.get(slotNumber).getSymbolBidPrice() + "_" + ibInteractionClient.myBidAskPriceDetails.get(slotNumber).getSymbolAskPrice() ;                
                        bidAskDetails = bidAskDetails + "__" + legOrderId + "_" + legDef.symbol + "_" + ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice();                        
                    } else if (legDef.qty < 0){
                        // Leg was shorted at the the time of taking position. leg would be bought for squaring off
                        legFilledPrice =  ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice() * legObj.lotSize ;                
                        bidAskDetails = legDef.symbol + "_" + ibInteractionClient.myBidAskPriceDetails.get(slotNumber).getSymbolBidPrice() + "_" + ibInteractionClient.myBidAskPriceDetails.get(slotNumber).getSymbolAskPrice() ;                                
                        bidAskDetails = bidAskDetails + "__" + legOrderId + "_" + legDef.symbol + "_" + ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice()  ;                        
                    }                    
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Please update manually as exit Order initiated but did not receive Confirmation for Orders filling for Order id " +  legOrderId);                
                }
            }
            //ibInteractionClient.stopGettingBidAskPriceForFut(slotNumber + IBTICKARRAYINDEXOFFSET);            
            // Make the position quantity as zero to indicate that square off Order has been placed. This would be used to exit the thread
            positionQty = 0;                  
        }
        
    } // End of squareOffLegPosition

    void setOpenPositionSlotOrderStatus(String orderStatus) {

        if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), debugFlag)) {
            // Since position exists, get position details
            TradingObject myTradingObject = new TradingObject(myUtils.getHashMapValueFromRedis(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), debugFlag));
            myTradingObject.setOrderState(orderStatus);

            Jedis jedis = jedisPool.getResource();
            jedis.hset(openPositionsQueueKeyName, Integer.toString(slotNumber),myTradingObject.getCompleteTradingObjectString());
            jedisPool.returnResource(jedis);            
        }
        
    }
    
    String getOpenPositionSlotOrderStatus() {

        String returnOrderStatus = "";

        if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), debugFlag)) {
            // Since position exists, get position details
            TradingObject myTradingObject = new TradingObject(myUtils.getHashMapValueFromRedis(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), debugFlag));
            returnOrderStatus = myTradingObject.getOrderState();
        }
        
        return(returnOrderStatus);
    }
    
    void updatePositionStatus(MyRangeActionObj rangeLimit) {
        
        if (myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), debugFlag)) {
            // Since position exists, get position details
            TradingObject myTradingObject = new TradingObject(myUtils.getHashMapValueFromRedis(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), debugFlag));

            double legLastPrice;
            if ((tickObj.lastPriceUpdateTime > 0 ) && (tickObj.lastPriceUpdateTime >= tickObj.closePriceUpdateTime)) {
                legLastPrice = tickObj.comboLastPrice;
            } else {
                legLastPrice = tickObj.comboClosePrice;            
            }
            String timeWhenUpdated = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(tickObj.lastPriceUpdateTime);
            myTradingObject.setLowerBreach(rangeLimitObj.stopLossLimit);
            myTradingObject.setUpperBreach(rangeLimitObj.takeProfitLimit);            
            myTradingObject.setLastKnownSpread(legLastPrice);
            myTradingObject.setLastUpdatedTimeStamp(timeWhenUpdated);

            Jedis jedis = jedisPool.getResource();
            jedis.hset(openPositionsQueueKeyName, Integer.toString(slotNumber),myTradingObject.getCompleteTradingObjectString());
            jedisPool.returnResource(jedis);                        
        }        
    }        
    
    void updateActionTakenStatus(MyRangeActionObj rangeLimit, String newStatus) {

        if (rangeLimit.stopLossLimitBreached) {
            rangeLimit.stopLossBreachActionStatus = newStatus;
        } else if (rangeLimit.takeProfitLimitBreached) {
            rangeLimit.takeProfitBreachActionStatus = newStatus;
        } else {
            rangeLimit.stopLossBreachActionStatus = newStatus;
            rangeLimit.takeProfitBreachActionStatus = newStatus; 
        }
    }   // End of updateActionTakenStatus
        
}
