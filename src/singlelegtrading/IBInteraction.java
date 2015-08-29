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

import com.ib.client.*;
import java.util.Calendar;
import java.util.TimeZone;
import redis.clients.jedis.*;

/**
 * @author Manish Kumar Singh
 */

public class IBInteraction implements EWrapper {
  
    // variables / constants declarations
    
    // variable to open socket connection 
    public EClientSocket ibClient = new EClientSocket(this);   
   
    private String myIPAddress;
    private int myPortNum;
    private int myClientId;
    
    private JedisPool jedisPool;
    private String orderIDField = "INRIBPAPERORDERID";

    // variable for uitlities class
    private MyUtils myUtils = new MyUtils();
    
    private Object lockOrderPlacement = new Object();

    // Define class to store last updated prices, and time of it
    public class MyTickObjClass {
        double symbolLastPrice, symbolClosePrice, symbolBidPrice, symbolAskPrice;
        long lastPriceUpdateTime,closePriceUpdateTime,bidPriceUpdateTime,askPriceUpdateTime;
        boolean subscriptionStatus;
    }
    public MyTickObjClass[] myTickDetails;

    // Define class to store last updated prices, and time of it
    public class MyBidAskPriceObjClass {
        double symbolBidPrice, symbolAskPrice;
        long bidPriceUpdateTime,askPriceUpdateTime;
    }
    public MyBidAskPriceObjClass[] myBidAskPriceDetails;
    
    // Define class to get Order id
    public class MyOrderStatusObjClass {
        double filledPrice;
        int remainingQuantity, orderIdentification, filledQuantity;
        long updateTime;        
    }
    public MyOrderStatusObjClass[] myOrderStatusDetails;     
    
    private int debugLevel = 0;
    
    private int initialValidOrderID = -1;
    
    private MyExchangeClass myExchangeObj;
    private double defaultOffsetForRelativeOrder = 0.05;
    
    private int IBTICKARRAYINDEXOFFSET = 50;
    
    // Constructor to inititalize variables
    IBInteraction(JedisPool jedisConnectionPool, String orderIDIncrField, String ibAPIIPAddress, int ibAPIPortNumber, int ibAPIClientId, MyExchangeClass exchangeObj) {
        jedisPool = jedisConnectionPool;
        myIPAddress = ibAPIIPAddress;
        myPortNum = ibAPIPortNumber;
        myClientId = ibAPIClientId;
        orderIDField = orderIDIncrField;
        myExchangeObj = exchangeObj;      

        TimeZone.setDefault(myExchangeObj.getExchangeTimeZone());
 
        if (myExchangeObj.getExchangeTimeZone().equals(TimeZone.getTimeZone("Asia/Calcutta"))) {
            defaultOffsetForRelativeOrder = 0.05;
        } else if (myExchangeObj.getExchangeTimeZone().equals(TimeZone.getTimeZone("America/New_York"))) {
            defaultOffsetForRelativeOrder = 0.01;            
        }
        
        myTickDetails = new MyTickObjClass[29];                
        for (int index = 0; index < myTickDetails.length; index++ ) {         
            myTickDetails[index] = new MyTickObjClass();
            myTickDetails[index].symbolLastPrice = 0.0;
            myTickDetails[index].symbolClosePrice = 0.0; 
            myTickDetails[index].symbolBidPrice = 0.0;
            myTickDetails[index].symbolAskPrice = 0.0;                         
            myTickDetails[index].lastPriceUpdateTime = -1;
            myTickDetails[index].closePriceUpdateTime = -1;
            myTickDetails[index].bidPriceUpdateTime = -1;
            myTickDetails[index].askPriceUpdateTime = -1;  
            myTickDetails[index].subscriptionStatus = false;                
        }

        myBidAskPriceDetails = new MyBidAskPriceObjClass[49];                
        for (int index = 0; index < myBidAskPriceDetails.length; index++ ) {         
            myBidAskPriceDetails[index] = new MyBidAskPriceObjClass();
            myBidAskPriceDetails[index].symbolBidPrice = 0.0;
            myBidAskPriceDetails[index].symbolAskPrice = 0.0;            
            myBidAskPriceDetails[index].bidPriceUpdateTime = -1;
            myBidAskPriceDetails[index].askPriceUpdateTime = -1;                
        }
        
        myOrderStatusDetails = new MyOrderStatusObjClass[49];                
        for (int index = 0; index < myOrderStatusDetails.length; index++ ) {         
            myOrderStatusDetails[index] = new MyOrderStatusObjClass();
            myOrderStatusDetails[index].orderIdentification = 0;
            myOrderStatusDetails[index].filledPrice = 0.0;
            myOrderStatusDetails[index].filledQuantity = 0;            
            myOrderStatusDetails[index].remainingQuantity = -1;            
            myOrderStatusDetails[index].updateTime = -1;              
        }         
        
    }
    
    // Custom functions
    public boolean connectToIB(int timeout) {
        
        ibClient.eConnect(myIPAddress, myPortNum, myClientId);

        //ibClient.reqIds(1); // This may be required in case valide order IDs are not being returned/generated
        int waitTime = 0;
        myUtils.waitForNSeconds(2);             
        while (initialValidOrderID < 0) {
            if (timeout == 0) {
                waitTime++;
                myUtils.waitForNSeconds(1);                
            } else if (waitTime <= timeout) {
                waitTime++;
                myUtils.waitForNSeconds(1);                                
            }
        }        

        if (initialValidOrderID > 0) { 
            // Set the orderID in IB Client as next valid Order ID and keep incrementing it for all subsequent order
            myUtils.setNextOrderID(jedisPool, orderIDField, initialValidOrderID, true);
            return(true);
        } else {
            return(false);            
        }        
    } //end of connectToIB
    
    public void disconnectFromIB(){
        ibClient.eDisconnect();
    } // End of disconnectFromIB

    public boolean waitForConnection(int timeout){

        int waitTime = 0;
        while (!ibClient.isConnected()) {
            if (timeout == 0) {
                waitTime++;
                myUtils.waitForNSeconds(1);                
            } else if (waitTime <= timeout) {
                waitTime++;
                myUtils.waitForNSeconds(1);                                
            }
        }
        if (ibClient.isConnected()) {
            return(true);
        } else {
            return(false);
        }

    } // End of waitForConnection

    void getBidAskPriceForStk(int requestID, String symbol) {

        Contract myContract = new Contract();   
        myContract.m_symbol = symbol;
        myContract.m_secType = "STK";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();   

        myBidAskPriceDetails[requestID - IBTICKARRAYINDEXOFFSET].symbolBidPrice = 0.0;
        myBidAskPriceDetails[requestID - IBTICKARRAYINDEXOFFSET].symbolAskPrice = 0.0;            
        
        ibClient.reqMktData(requestID, myContract, "", true);            
        
    } // End of getBidAskPriceForStk    
    
    void stopGettingBidAskPriceForStk(int requestID) {        

        ibClient.cancelMktData(requestID);            

    } // End of stopGettingBidAskPriceForStk()    
    
    void requestStkMktDataSubscription(int requestID, String symbol) {

        Contract myContract = new Contract();   
        myContract.m_symbol = symbol;
        myContract.m_secType = "STK";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();   

        myTickDetails[requestID].symbolLastPrice = 0.0;
        myTickDetails[requestID].symbolClosePrice = 0.0; 
        myTickDetails[requestID].symbolBidPrice = 0.0;
        myTickDetails[requestID].symbolAskPrice = 0.0;                         
        myTickDetails[requestID].lastPriceUpdateTime = -1;
        myTickDetails[requestID].closePriceUpdateTime = -1;
        myTickDetails[requestID].bidPriceUpdateTime = -1;
        myTickDetails[requestID].askPriceUpdateTime = -1;  
        myTickDetails[requestID].subscriptionStatus = true;         
        ibClient.reqMktData(requestID, myContract, "", false);            
        
    } // End of requestStkMktDataSubscription    

    void getBidAskPriceForFut(int requestID, String symbol, String expiry) {

        Contract myContract = new Contract();   
        myContract.m_symbol = symbol;
        myContract.m_secType = "FUT";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();   
        myContract.m_expiry = expiry;         

        myBidAskPriceDetails[requestID - IBTICKARRAYINDEXOFFSET].symbolBidPrice = 0.0;
        myBidAskPriceDetails[requestID - IBTICKARRAYINDEXOFFSET].symbolAskPrice = 0.0;            
        
        ibClient.reqMktData(requestID, myContract, "", true);            
        
    } // End of getBidAskPriceForFut    
    
    void stopGettingBidAskPriceForFut(int requestID) {        

        ibClient.cancelMktData(requestID);            

    } // End of stopGettingBidAskPriceForFut()     
    
    void requestFutMktDataSubscription(int requestID, String symbol, String expiry) {

        Contract myContract = new Contract();   
        myContract.m_symbol = symbol;
        myContract.m_secType = "FUT";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();   
        myContract.m_expiry = expiry;         

        myTickDetails[requestID].symbolLastPrice = 0.0;
        myTickDetails[requestID].symbolClosePrice = 0.0; 
        myTickDetails[requestID].symbolBidPrice = 0.0;
        myTickDetails[requestID].symbolAskPrice = 0.0;                         
        myTickDetails[requestID].lastPriceUpdateTime = -1;
        myTickDetails[requestID].closePriceUpdateTime = -1;
        myTickDetails[requestID].bidPriceUpdateTime = -1;
        myTickDetails[requestID].askPriceUpdateTime = -1;  
        myTickDetails[requestID].subscriptionStatus = true;        
        ibClient.reqMktData(requestID, myContract, "", false);            
        
    } // End of requestFutMktDataSubscription    

    void cancelMktDataSubscription(int requestID) {        

        myTickDetails[requestID].subscriptionStatus = false;
        ibClient.cancelMktData(requestID);            

    } // End of onCancelMktData() 

    void requestExecutionDetailsHistorical(int requestId, int numPrevDays) {
        
        Calendar startingTimeStamp = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());
        startingTimeStamp.add(Calendar.DATE, -1*numPrevDays); 

        String startTime = String.format("%1$tY%1$tm%1$td-00:00:00",startingTimeStamp); // format is - yyyymmdd-hh:mm:ss

        ExecutionFilter myFilter = new ExecutionFilter();
        myFilter.m_clientId = myClientId;
        myFilter.m_exchange = myExchangeObj.getExchangeName(); 
        myFilter.m_time = startTime;

        if (requestId > 0) {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "requesting execution details for time after " + startTime);
        }            
        
        ibClient.reqExecutions(requestId, myFilter);
        
    } // end of requestExecutionDetailsHistorical

    public int placeStkOrderAtRelative(String symbol, int qty, String mktAction, String referenceComments, double limitPrice, double offsetAmount, boolean debugFlag) {
                
        int ibOrderId;
        Contract myContract = new Contract();
        Order myOrder = new Order();
        
        myContract.m_symbol = symbol;
        myContract.m_secType = "STK";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();   
        
        myOrder.m_action = mktAction;
        myOrder.m_totalQuantity = qty;
        myOrder.m_orderType = "REL"; // At Relative to Market Price
        myOrder.m_lmtPrice = limitPrice;
        myOrder.m_auxPrice = defaultOffsetForRelativeOrder;        
        if (offsetAmount > 0) {
            myOrder.m_auxPrice = offsetAmount;            
        }
        myOrder.m_tif = "DAY"; // GTC - Good Till Cancel Order, DAY - Good Till Day
        myOrder.m_orderRef = referenceComments; // This is what gets displayed on TWS screen
        myOrder.m_transmit = true; // STP order i.e. transmit immediately
        synchronized(lockOrderPlacement) {
            ibOrderId = myUtils.getNextOrderID(jedisPool, orderIDField, debugFlag);
            getNextOrderStatusArrayIndex(ibOrderId);
            ibClient.placeOrder(ibOrderId, myContract, myOrder);
            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Placed Relative Order for " + symbol +" for "+ mktAction + " type STK " + " order ID " + ibOrderId + " limit " + limitPrice + " offsetAmt " + offsetAmount);
            }            
        }
        
        return(ibOrderId);        
    } // placeStkOrderAtRelative
    
    public int placeStkOrderAtMarket(String symbol, int qty, String mktAction, String referenceComments, boolean debugFlag) {
                
        int ibOrderId;
        Contract myContract = new Contract();
        Order myOrder = new Order();
        
        myContract.m_symbol = symbol;
        myContract.m_secType = "STK";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();   
        
        myOrder.m_action = mktAction;
        myOrder.m_totalQuantity = qty;
        myOrder.m_orderType = "MKT"; // At Market Price
        //myOrder.m_allOrNone = true; // ALL or None are not supported in NSE
        myOrder.m_tif = "DAY"; // GTC - Good Till Cancel Order, DAY - Good Till Day
        myOrder.m_orderRef = referenceComments; // This is waht gets displayed on TWS screen
        myOrder.m_transmit = true; // STP order i.e. transmit immediately
        synchronized(lockOrderPlacement) {
            ibOrderId = myUtils.getNextOrderID(jedisPool, orderIDField, debugFlag);
            ibClient.placeOrder(ibOrderId, myContract, myOrder);

            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Placed Market Order for " + symbol +" for "+ mktAction + " type STK " + " order ID " + ibOrderId);
            }            
        }
        
        return(ibOrderId);        
    } // placeStkOrderAtMarket

    public int placeFutOrderAtRelative(String symbol, int qty, String expiry, String mktAction, String referenceComments, double limitPrice, double offsetAmount, boolean debugFlag) {
                
        int ibOrderId;
        Contract myContract = new Contract();
        Order myOrder = new Order();
        
        myContract.m_symbol = symbol;
        myContract.m_secType = "FUT";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();   
        myContract.m_expiry = expiry;
        
        myOrder.m_action = mktAction;
        myOrder.m_totalQuantity = qty;
        myOrder.m_orderType = "REL"; // At Market Price
        myOrder.m_lmtPrice = limitPrice;
        myOrder.m_auxPrice = defaultOffsetForRelativeOrder;        
        if (offsetAmount > 0) {
            myOrder.m_auxPrice = offsetAmount;            
        }
        myOrder.m_tif = "DAY"; // GTC - Good Till Cancel Order, DAY - Good Till Day
        myOrder.m_orderRef = referenceComments; // This is what gets displayed on TWS screen
        myOrder.m_transmit = true; // STP order i.e. transmit immediately
        synchronized(lockOrderPlacement) {
            ibOrderId = myUtils.getNextOrderID(jedisPool, orderIDField, debugFlag);
            getNextOrderStatusArrayIndex(ibOrderId);
            ibClient.placeOrder(ibOrderId, myContract, myOrder);
            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Placed Relative Order for " + symbol +" for "+ mktAction + " type FUT " + " expiry "+ expiry + " order ID " + ibOrderId + " limit " + limitPrice + " offsetAmt " + offsetAmount);
            }            
        }
        
        return(ibOrderId);        
    } // placeFutOrderAtRelative
    
    public int placeFutOrderAtMarket(String symbol, int qty, String expiry, String mktAction, String referenceComments, boolean debugFlag) {
                
        int ibOrderId;
        Contract myContract = new Contract();
        Order myOrder = new Order();
        
        myContract.m_symbol = symbol;
        myContract.m_secType = "FUT";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();   
        myContract.m_expiry = expiry;
        
        myOrder.m_action = mktAction;
        myOrder.m_totalQuantity = qty;
        myOrder.m_orderType = "MKT"; // At Market Price
        //myOrder.m_allOrNone = true; // ALL or None are not supported in NSE
        myOrder.m_tif = "DAY"; // GTC - Good Till Cancel Order, DAY - Good Till Day
        myOrder.m_orderRef = referenceComments; // This is waht gets displayed on TWS screen
        myOrder.m_transmit = true; // STP order i.e. transmit immediately
        synchronized(lockOrderPlacement) {
            ibOrderId = myUtils.getNextOrderID(jedisPool, orderIDField, debugFlag);
            getNextOrderStatusArrayIndex(ibOrderId);
            ibClient.placeOrder(ibOrderId, myContract, myOrder);
            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Placed Market Order for " + symbol +" for "+ mktAction + " type FUT " + " expiry "+ expiry + " order ID " + ibOrderId);
            }            
        }
        
        return(ibOrderId);        
    } // placeFutOrderAtMarket

    private boolean getNextOrderStatusArrayIndex(int orderId) {   
    
        boolean found = false;
        int returnIndex = 0;
        while ((returnIndex < myOrderStatusDetails.length) && (!found)) {
            if (myOrderStatusDetails[returnIndex].orderIdentification == 0) {
                found = true;
                myOrderStatusDetails[returnIndex].orderIdentification = orderId;
                myOrderStatusDetails[returnIndex].filledPrice = 0.0;
                myOrderStatusDetails[returnIndex].filledQuantity = 0;            
                myOrderStatusDetails[returnIndex].remainingQuantity = -1;            
                myOrderStatusDetails[returnIndex].updateTime = -1;              
            } else {
                returnIndex++;                
            }
        }

        return(found);
    }    
    
    // overridden functions to receive data from IB interface / TWS
    @Override
    public void historicalData(int reqId, String date, double open, double high, double low,
            double close, int volume, int count, double WAP, boolean hasGaps) {   
        
    } // End of historcialData(...)
    
    @Override
    public void tickPrice(int tickerId, int field, double price, int canAutoExecute) {
        
        if (tickerId > IBTICKARRAYINDEXOFFSET) { 
            if (field == 1) {
                myBidAskPriceDetails[tickerId - IBTICKARRAYINDEXOFFSET].symbolBidPrice = price;                                    
                System.out.println("bidPrice " + price +" tickerId "+ tickerId + " Time " + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) );
            }
            if (field == 2) {
                myBidAskPriceDetails[tickerId - IBTICKARRAYINDEXOFFSET].symbolAskPrice = price;                                    
                System.out.println("askPrice " + price +" tickerId "+ tickerId + " Time " + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) );
            }
        } else {
            for (int index = 0; index < myTickDetails.length; index++ ) {         
                if (myTickDetails[index].subscriptionStatus) {
                    double closePrice = 0.0;
                    if (field == 9) {
                        if (tickerId == index) {
                            closePrice = price;
                        }
                        if (closePrice > 0) {
                            myTickDetails[index].closePriceUpdateTime = System.currentTimeMillis();            
                            myTickDetails[index].symbolClosePrice = closePrice;                                    
                        }
                    }                    
                    double lastPrice = 0.0;
                    if (field == 4) {
                        myTickDetails[index].lastPriceUpdateTime = System.currentTimeMillis();            
                        if (tickerId == index) {
                            lastPrice = price;
                        }
                        if (lastPrice > 0) {
                            myTickDetails[index].lastPriceUpdateTime = System.currentTimeMillis();            
                            myTickDetails[index].symbolLastPrice = lastPrice;                                    
                        }                    
                    }
                    double bidPrice = 0.0;
                    if (field == 1) {
                        myTickDetails[index].bidPriceUpdateTime = System.currentTimeMillis();            
                        if (tickerId == index) {
                            bidPrice = price;
                        }
                        if (bidPrice > 0) {
                            myTickDetails[index].bidPriceUpdateTime = System.currentTimeMillis();            
                            myTickDetails[index].symbolBidPrice = bidPrice;                                    
                        }                    
                    }       
                    double askPrice = 0.0;
                    if (field == 2) {
                        myTickDetails[index].askPriceUpdateTime = System.currentTimeMillis();            
                        if (tickerId == index) {
                            askPrice = price;
                        }
                        if (askPrice > 0) {
                            myTickDetails[index].askPriceUpdateTime = System.currentTimeMillis();            
                            myTickDetails[index].symbolAskPrice = askPrice;                                    
                        }                    
                    }                      
                }            
            }                                        
        }        
    } // End of tickPrice(...)

    @Override
    public void tickSize(int tickerId, int field, int size) {
             
    } // End of tickSize(...)

    
    @Override
    public void error(Exception e) {
        System.err.println(e.getMessage());
    }

    @Override
    public void error(String str) {
        System.err.println(str);
    }

    @Override
    public void error(int id, int errorCode, String errorMsg) {
        System.err.println(errorMsg);
    }

    @Override
    public void connectionClosed() {
    }

    public void tickOptionComputation(int tickerId, int field, double impliedVol,
            double delta, double modelPrice, double pvDividend) {
    }

    @Override
    public void tickGeneric(int tickerId, int tickType, double value) {
    }

    @Override
    public void tickString(int tickerId, int tickType, String value) {
    }

    @Override
    public void tickEFP(int tickerId, int tickType, double basisPoints,
            String formattedBasisPoints, double impliedFuture, int holdDays,
            String futureExpiry, double dividendImpact, double dividendsToExpiry) {
    }

    @Override
    public void orderStatus(int orderId, String status, int filled, int remaining,
            double avgFillPrice, int permId, int parentId, double lastFillPrice,
            int clientId, String whyHeld) {

        for (int index = 0; index < myOrderStatusDetails.length; index++ ) { 
            if ((myOrderStatusDetails[index].orderIdentification > 0) &&
            (orderId == myOrderStatusDetails[index].orderIdentification)) {
                myOrderStatusDetails[index].filledQuantity = filled;
                myOrderStatusDetails[index].remainingQuantity = remaining;
                myOrderStatusDetails[index].filledPrice= avgFillPrice;
                myOrderStatusDetails[index].updateTime = System.currentTimeMillis();                    
                if (debugLevel > 4) {
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ",Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "OrderId " + orderId +" status "+ status + " filled qty "+ filled + " remaining qty " + remaining + " average fill price " + avgFillPrice + " last filled price " + lastFillPrice);            
                }
            }                         
        }             
    }

    @Override
    public void openOrder(int orderId, Contract contract, Order order, OrderState orderState) {
    }

    @Override
    public void updateAccountValue(String key, String value, String currency, String accountName) {
    }

    @Override
    public void updatePortfolio(Contract contract, int position, double marketPrice, double marketValue,
            double averageCost, double unrealizedPNL, double realizedPNL, String accountName) {
    }

    @Override
    public void updateAccountTime(String timeStamp) {
    }

    @Override
    public void nextValidId(int orderId) {
        initialValidOrderID = orderId;
    }

    //public void contractDetails(ContractDetails contractDetails) {} gives reqid now, see below
    public void bondContractDetails(ContractDetails contractDetails) {
    }

    @Override
    public void execDetails(int orderId, Contract contract, Execution execution) {

        //System.out.println("reqId :" + orderId +" symbol :"+ contract.m_symbol + " expiry :" + contract.m_expiry + " execTime :" + execution.m_time + " avgPrice :" + execution.m_avgPrice + " execOrderId :" + execution.m_orderId + " price :" + execution.m_price + " qty :" + execution.m_cumQty + " numShares :" + execution.m_shares + " orderRef :" + execution.m_orderRef );                        

        for (int index = 0; index < myOrderStatusDetails.length; index++ ) { 
            if ((myOrderStatusDetails[index].orderIdentification > 0) &&
                (myOrderStatusDetails[index].remainingQuantity != 0 ) &&
                (execution.m_orderId == myOrderStatusDetails[index].orderIdentification)) {
                    myOrderStatusDetails[index].filledQuantity = execution.m_cumQty;
                    myOrderStatusDetails[index].remainingQuantity = 0;
                    myOrderStatusDetails[index].filledPrice= execution.m_price;                    
            }                         
        }                     
        
    }

    @Override
    public void updateMktDepth(int tickerId, int position, int operation, int side, double price, int size) {
    }

    @Override
    public void updateMktDepthL2(int tickerId, int position, String marketMaker, int operation,
            int side, double price, int size) {
    }

    @Override
    public void updateNewsBulletin(int msgId, int msgType, String message, String origExchange) {
    }

    @Override
    public void managedAccounts(String accountsList) {
    }

    @Override
    public void receiveFA(int faDataType, String xml) {
    }

    @Override
    public void scannerParameters(String xml) {
    }

    @Override
    public void scannerData(int reqId, int rank, ContractDetails contractDetails, String distance,
            String benchmark, String projection, String legsStr) {
    }

    @Override
    public void scannerDataEnd(int reqId) {
    }

    @Override
    public void realtimeBar(int reqId, long time, double open, double high, double low, double close, long volume, double wap, int count) {
    }

    @Override
    public void currentTime(long time) {
    }

    @Override
    public void tickSnapshotEnd(int reqId) {
    }

    @Override
    public void deltaNeutralValidation(int reqId, UnderComp underComp) {
    }

    @Override
    public void fundamentalData(int reqId, String data) {
    }

    @Override
    public void execDetailsEnd(int reqId) {
    }

    @Override
    public void contractDetailsEnd(int reqId) {
    }

    @Override
    public void bondContractDetails(int reqId, ContractDetails contractDetails) {
    }

    @Override
    public void contractDetails(int reqId, ContractDetails contractDetails) {
    } //the new version

    @Override
    public void accountDownloadEnd(String accountName) {
    }

    @Override
    public void openOrderEnd() {
    }

    @Override public void tickOptionComputation(int tickerId, int field, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice) {
    }


    @Override public void marketDataType(int reqId, int marketDataType) {
    }

    @Override public void commissionReport(CommissionReport commissionReport) {
    }

    @Override public void position(String account, Contract contract, int pos, double avgCost) {
    }

    @Override public void positionEnd() {
    }

    @Override public void accountSummary(int reqId, String account, String tag, String value, String currency) {
    }

    @Override public void accountSummaryEnd(int reqId) {
    }        
}
