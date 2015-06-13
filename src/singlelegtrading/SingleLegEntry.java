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

public class SingleLegEntry extends Thread {
   private String threadName = "TakingNewPositionThread";
   private String legDetails;
   private boolean debugFlag;
   private JedisPool jedisPool;     
   private IBInteraction ibInteractionClient; 
   private TimeZone exchangeTimeZone;
   private MyUtils myUtils;
   private String strategyName;
   private String openPositionsQueueKeyName;
   private String futExpiry;
   private String legName;
   private int legLotSize;
   private String legPosition; // "BUY" for long. "SELL" for short.
   private String orderTypeToUse; // market, relativewithzeroaslimitwithamountoffset, relativewithmidpointaslimitwithamountoffset, relativewithzeroaslimitwithpercentoffset, relativewithmidpointaslimitwithpercentoffset 

   private int INITIALSTOPLOSSAMOUNT = 20000;
   private int slotNumber = 3;
   
   private int IBTICKARRAYINDEXOFFSET = 50;
   
   SingleLegEntry(String name, String legInfo, JedisPool redisConnectionPool, IBInteraction ibInterClient, String strategyReference, String openPosQueueName, TimeZone exTZ, String confOrderType, int assignedSlotNumber, int stopLossAmount, boolean debugIndicator){
        threadName = name;
        legDetails = legInfo;
        debugFlag = debugIndicator;
        jedisPool = redisConnectionPool;

        ibInteractionClient = ibInterClient;
        exchangeTimeZone = exTZ;
        
        myUtils = new MyUtils();
        
        if (exchangeTimeZone.equals(TimeZone.getTimeZone("Asia/Calcutta"))) {
            // for FUT type of action, fut expiry needs to be defined
            futExpiry = myUtils.getKeyValueFromRedis(jedisPool, "INRFUTCURRENTEXPIRY", false);
        } else if (exchangeTimeZone.equals(TimeZone.getTimeZone("America/New_York"))) {
            // for STK type of action, mark futexpiry to 000000
            futExpiry = "000000";
        }
        
        strategyName = strategyReference;
        openPositionsQueueKeyName = openPosQueueName;
        orderTypeToUse = confOrderType;
        slotNumber = assignedSlotNumber;
        INITIALSTOPLOSSAMOUNT = stopLossAmount;
        TimeZone.setDefault(exchangeTimeZone);
        
        String entrySignal[] = legDetails.split(",");
        if (Integer.parseInt(entrySignal[TradingObject.SIDE_SIZE_INDEX]) > 0) {
            String pairStructure[] = entrySignal[TradingObject.STRUCTURE_INDEX].split("_");
            legName = pairStructure[0];
            legPosition = "BUY";
            legLotSize = Math.abs(Integer.parseInt(pairStructure[1])*Integer.parseInt(entrySignal[TradingObject.SIDE_SIZE_INDEX]));
        } else if (Integer.parseInt(entrySignal[TradingObject.SIDE_SIZE_INDEX]) < 0) {
            String pairStructure[] = entrySignal[TradingObject.STRUCTURE_INDEX].split("_");
            legName = pairStructure[0];
            legPosition = "SELL";            
            legLotSize = Math.abs(Integer.parseInt(pairStructure[1])*Integer.parseInt(entrySignal[TradingObject.SIDE_SIZE_INDEX]));
        } else {
           // TBD - Write error handling code here
        }

   }

    int getOrderStatusArrayIndex(int orderId) {   
    
        boolean found = false;
        int returnIndex = 0;
        while ((returnIndex < ibInteractionClient.myOrderStatusDetails.length) && (!found)) {
            if (ibInteractionClient.myOrderStatusDetails[returnIndex].orderIdentification == orderId) {
                found = true;
            } else {
                returnIndex++;                
            }
        }

        if (!found) {
            returnIndex = -1;
        }

        return(returnIndex);
    }

    boolean entryOrderCompletelyFilled(int legOrderStatusIndex, int maxWaitTime) {

        //ibInteractionClient.ibClient.reqOpenOrders();
        int timeOut = 10;
        myUtils.waitForNSeconds(timeOut);
        while ( (ibInteractionClient.myOrderStatusDetails[legOrderStatusIndex].remainingQuantity != 0) &&
                (timeOut < maxWaitTime)) {
            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Waiting for Order to be filled for  Order ids " +  ibInteractionClient.myOrderStatusDetails[legOrderStatusIndex].orderIdentification + " for " + timeOut + " seconds");
            } 
            timeOut += 10;                
            // Check if following needs to be commented
            ibInteractionClient.ibClient.reqOpenOrders();
            myUtils.waitForNSeconds(10);
        }
        if ((ibInteractionClient.myOrderStatusDetails[legOrderStatusIndex].remainingQuantity == 0)) {
            return(true);
        } else {
            return(false);
        }
        
    }

    void updateOpenPositionsQueue(String queueKeyName, String updateDetails, String orderStatus, double pairSpread, int entryOrderId, int slotNumber, String bidAskPriceDetails ) {

        TradingObject myTradeObject = new TradingObject(updateDetails);

        if (orderStatus.equalsIgnoreCase("entryorderinitiated")) {
            myTradeObject.setEntryTimeStamp(String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",Calendar.getInstance(exchangeTimeZone)));
            myTradeObject.initiateAndValidate();
        }
        
        if (Integer.parseInt(myTradeObject.getSideAndSize()) > 0) {
            myTradeObject.setLowerBreach( Math.abs(Integer.parseInt(myTradeObject.getSideAndSize())) * (Math.round(Float.parseFloat(myTradeObject.getEntrySpread())) - INITIALSTOPLOSSAMOUNT) );
            myTradeObject.setUpperBreach( Math.abs(Integer.parseInt(myTradeObject.getSideAndSize())) * (Math.round(Math.abs(Float.parseFloat(myTradeObject.getZScore()) * Float.parseFloat(myTradeObject.getEntryStdDev()))) + (int) Math.round(Float.parseFloat(myTradeObject.getEntrySpread())) ) );                    
        } else if (Integer.parseInt(myTradeObject.getSideAndSize()) < 0) {
            myTradeObject.setLowerBreach( Math.abs(Integer.parseInt(myTradeObject.getSideAndSize())) * Math.round(Float.parseFloat(myTradeObject.getEntrySpread())) + INITIALSTOPLOSSAMOUNT);
            myTradeObject.setUpperBreach( Math.abs(Integer.parseInt(myTradeObject.getSideAndSize())) * (-1 * Math.round(Math.abs(Float.parseFloat(myTradeObject.getZScore()) * Float.parseFloat(myTradeObject.getEntryStdDev()))) + (int) Math.round(Float.parseFloat(myTradeObject.getEntrySpread()))));                                
        }        
      
        if (bidAskPriceDetails.length() > 0) {
            myTradeObject.setEntryBidAskFillDetails(bidAskPriceDetails);
        }
        
        myTradeObject.setOrderState(orderStatus);

        if ( (pairSpread > 0) || (pairSpread < 0) ) {
            // For single leg must have abs function. for multi leg it should not be there.
            myTradeObject.setEntrySpread(Math.abs(Integer.parseInt(myTradeObject.getSideAndSize())) * pairSpread);
        }
        
        myTradeObject.setExpiry(futExpiry);
        myTradeObject.setEntryOrderIDs(entryOrderId);
        myTradeObject.setLastKnownSpread(0);
        myTradeObject.setLastUpdatedTimeStamp("-1");        

        // Update the open positions key with entry Signal
        Jedis jedis = jedisPool.getResource();
        jedis.hset(queueKeyName, Integer.toString(slotNumber), myTradeObject.getCompleteTradingObjectString());       
        jedisPool.returnResource(jedis);
        
    }

    int myPlaceConfiguredOrder(String symbolName, int quantity, String mktAction) {

        int returnOrderId = 0;
        // Possible order types are as follows
        // market, relativewithzeroaslimitwithamountoffset, relativewithmidpointaslimitwithamountoffset, relativewithzeroaslimitwithpercentoffset, relativewithmidpointaslimitwithpercentoffset 
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Placing following Order - Symbol :" +  symbolName + " quantity " + quantity + " expiry " + futExpiry + " mktAction " + mktAction + " orderType " + orderTypeToUse );
                
        if (orderTypeToUse.equalsIgnoreCase("market")) {
            if (futExpiry.equalsIgnoreCase("000000")) {
                // Place order for STK type
                returnOrderId = ibInteractionClient.placeStkOrderAtMarket(symbolName, quantity, mktAction, strategyName, true);                
            } else {
                // Place Order for FUT type
                returnOrderId = ibInteractionClient.placeFutOrderAtMarket(symbolName, quantity, futExpiry, mktAction, strategyName, true);                
            }
        } else if (orderTypeToUse.equalsIgnoreCase("relativewithzeroaslimitwithamountoffset")) {
            double limitPrice = 0.0; // For relative order, Limit price is suggested to be left as zero
            double offsetAmount = 0.0; // zero means it will take default value based on exchange / timezone
            if (futExpiry.equalsIgnoreCase("000000")) {
                // Place order for STK type
                returnOrderId = ibInteractionClient.placeStkOrderAtRelative(symbolName, quantity, mktAction, strategyName, limitPrice, offsetAmount, true);            
            } else {
                // Place Order for FUT type
                returnOrderId = ibInteractionClient.placeFutOrderAtRelative(symbolName, quantity, futExpiry, mktAction, strategyName, limitPrice, offsetAmount, true);            
            }
        }
        
        return(returnOrderId);
    }
    
    void enterLegPosition() {

        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Blocked Slot Number for this trade : " + slotNumber);
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Placing following Leg Order Element - Symbol :" +  legName + " quantity " + legLotSize + " expiry " + futExpiry  + " orderType " + orderTypeToUse );

        double legFilledPrice = 0.0;

        // Place Order and get the order ID
        if (futExpiry.equalsIgnoreCase("000000")) {
            // for STK type
            ibInteractionClient.getBidAskPriceForStk(slotNumber + IBTICKARRAYINDEXOFFSET, legName);
        } else {
            // for FUT type
            ibInteractionClient.getBidAskPriceForFut(slotNumber + IBTICKARRAYINDEXOFFSET, legName, futExpiry);
        }        
        int legOrderId = this.myPlaceConfiguredOrder(legName, legLotSize, legPosition);

        System.out.println("Placed Orders - with orderID as : " +  legOrderId + " for " + legPosition);
        
        String bidAskDetails = legName + "_" + ibInteractionClient.myBidAskPriceDetails[slotNumber].symbolBidPrice + "_" + ibInteractionClient.myBidAskPriceDetails[slotNumber].symbolAskPrice;
        // update the Open position queue with order inititated status message
        updateOpenPositionsQueue(openPositionsQueueKeyName, legDetails, "entryorderinitiated", legFilledPrice, legOrderId, slotNumber, bidAskDetails );                            
        
        if (legOrderId > 0) {
            // Get index of Order Array of ibinteraction client to get back order status
            int indexOrderSymbol = getOrderStatusArrayIndex(legOrderId);
            if (indexOrderSymbol < 0) {
                // Try after 20 second to get indexes
                myUtils.waitForNSeconds(20);
                indexOrderSymbol = getOrderStatusArrayIndex(legOrderId);
            }
            
            // Wait for orders to be completely filled            
            if ( (indexOrderSymbol >= 0) && (entryOrderCompletelyFilled(indexOrderSymbol, 750)) ) {
                bidAskDetails = legName + "_" + ibInteractionClient.myBidAskPriceDetails[slotNumber].symbolBidPrice + "_" + ibInteractionClient.myBidAskPriceDetails[slotNumber].symbolAskPrice ;
                bidAskDetails = bidAskDetails + "__" + legOrderId + "_" + legName + "_" + ibInteractionClient.myOrderStatusDetails[indexOrderSymbol].filledPrice ;
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Entry Order leg filled for  Order id " +  legOrderId + " order side " + legPosition +" at avg filled price " + ibInteractionClient.myOrderStatusDetails[indexOrderSymbol].filledPrice);
                legFilledPrice = ibInteractionClient.myOrderStatusDetails[indexOrderSymbol].filledPrice * legLotSize;
                //if (legPosition.equalsIgnoreCase("BUY")) {
                //    legFilledPrice = 1 * legFilledPrice;
                //} else if (legPosition.equalsIgnoreCase("SELL")) {
                //    legFilledPrice = -1 * legFilledPrice;                    
                //}
                // update Redis queue with entered order
                updateOpenPositionsQueue(openPositionsQueueKeyName, legDetails, "entryorderfilled", legFilledPrice, legOrderId, slotNumber, bidAskDetails);                            
            } else if (indexOrderSymbol >= 0) {
                ibInteractionClient.requestExecutionDetailsHistorical(legOrderId,1);
                myUtils.waitForNSeconds(30);
                bidAskDetails = legName + "_" + ibInteractionClient.myBidAskPriceDetails[slotNumber].symbolBidPrice + "_" + ibInteractionClient.myBidAskPriceDetails[slotNumber].symbolAskPrice ;
                bidAskDetails = bidAskDetails + "__" + legOrderId + "_" + legName + "_" + ibInteractionClient.myOrderStatusDetails[indexOrderSymbol].filledPrice ;
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Entry Order leg filled for  Order id " +  legOrderId + " order side " + legPosition +" at avg filled price " + ibInteractionClient.myOrderStatusDetails[indexOrderSymbol].filledPrice);
                legFilledPrice = ibInteractionClient.myOrderStatusDetails[indexOrderSymbol].filledPrice * legLotSize;
                //if (legPosition.equalsIgnoreCase("BUY")) {
                //    legFilledPrice = 1 * legFilledPrice;
                //} else if (legPosition.equalsIgnoreCase("SELL")) {
                //    legFilledPrice = -1 * legFilledPrice;                    
                //}
                // update Redis queue with entered order
                updateOpenPositionsQueue(openPositionsQueueKeyName, legDetails, "entryorderinitiated", legFilledPrice, legOrderId, slotNumber, bidAskDetails);                            
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Please Check Order Status manually as entry Order initiated but did not receive Confirmation for Orders filling for Order id " +  legOrderId );
            } else {
                myUtils.waitForNSeconds(30);
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Please Check Order Status manually as entry Order initiated but Orders Array Index could not be found for Order id " +  legOrderId );   
            }
        }
                
    }   
    
    @Override 
    public void run() {

        this.setName(threadName);        
        // Place market Order with IB. Place the order in same sequence as order id is generated. if orderid sent is less than any of previous order id then duplicate order id message would be received
        if (!ibInteractionClient.waitForConnection(60)) {
            // Debug Message
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "IB Connection not available. Can not Enter Position as " +  legName + ". Exiting thread Now.");
        } else {
            enterLegPosition();
            // Debug Message
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(exchangeTimeZone)) + "Tried Entering Position as " +  legName + ". Exiting this thread Now.");            
        }                
    }    
    
}
