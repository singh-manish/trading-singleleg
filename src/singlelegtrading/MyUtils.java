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

import java.text.*;
import java.util.*;
import java.util.logging.*;
import redis.clients.jedis.*;

/**
 * @author Manish Kumar Singh
 */

class MyExchangeClass {
    private TimeZone exchangeTimeZone;
    private int exchangeStartTimeHHMM, exchangeStartTimeHHMMSS, exchangeCloseTimeHHMM,exchangeCloseTimeHHMMSS;
    private int programStartTimeHHMM, programStartTimeHHMMSS, programCloseTimeHHMM,programCloseTimeHHMMSS;
    private String exchangeName, exchangeCurrency;

    public MyExchangeClass (TimeZone exTZ) {
        this.exchangeTimeZone = exTZ;
        if (exTZ.equals(TimeZone.getTimeZone("Asia/Calcutta"))) {
            this.exchangeStartTimeHHMM = 915;
            this.exchangeStartTimeHHMMSS = 91500;
            this.exchangeCloseTimeHHMM = 1530;
            this.exchangeCloseTimeHHMMSS = 153000;
            this.programStartTimeHHMM = 900;
            this.programStartTimeHHMMSS = 90000;
            this.programCloseTimeHHMM = 1535;
            this.programCloseTimeHHMMSS = 153500;
            this.exchangeName = "NSE";
            this.exchangeCurrency = "INR";
        } else if (exTZ.equals(TimeZone.getTimeZone("America/New_York"))) {         
            this.exchangeStartTimeHHMM = 930;
            this.exchangeStartTimeHHMMSS = 93000;
            this.exchangeCloseTimeHHMM = 1600;
            this.exchangeCloseTimeHHMMSS = 160000;
            this.programStartTimeHHMM = 915;
            this.programStartTimeHHMMSS = 91500;
            this.programCloseTimeHHMM = 1605;
            this.programCloseTimeHHMMSS = 160500;
            this.exchangeName = "SMART";
            this.exchangeCurrency = "USD";
        }
    }

    public MyExchangeClass (String exchangeCurrency) {
        if (exchangeCurrency.equalsIgnoreCase("inr")) {
            this.exchangeTimeZone = TimeZone.getTimeZone("Asia/Calcutta");            
            this.exchangeStartTimeHHMM = 915;
            this.exchangeStartTimeHHMMSS = 91500;
            this.exchangeCloseTimeHHMM = 1530;
            this.exchangeCloseTimeHHMMSS = 153000;
            this.programStartTimeHHMM = 900;
            this.programStartTimeHHMMSS = 90000;
            this.programCloseTimeHHMM = 1535;
            this.programCloseTimeHHMMSS = 153500;
            this.exchangeName = "NSE";
            this.exchangeCurrency = "INR";
        } else if (exchangeCurrency.equalsIgnoreCase("usd")) {
            this.exchangeTimeZone = TimeZone.getTimeZone("America/New_York");          
            this.exchangeStartTimeHHMM = 930;
            this.exchangeStartTimeHHMMSS = 93000;
            this.exchangeCloseTimeHHMM = 1600;
            this.exchangeCloseTimeHHMMSS = 160000;
            this.programStartTimeHHMM = 915;
            this.programStartTimeHHMMSS = 91500;
            this.programCloseTimeHHMM = 1605;
            this.programCloseTimeHHMMSS = 160500;
            this.exchangeName = "SMART";
            this.exchangeCurrency = "USD";
        }
    }    

    public TimeZone getExchangeTimeZone() { return this.exchangeTimeZone; }
    public int getExchangeStartTimeHHMM() { return this.exchangeStartTimeHHMM; }
    public String getStringExchangeStartTimeHHMM() { return String.format("%04d", this.exchangeStartTimeHHMM); }    
    public int getExchangeStartTimeHHMMSS() { return this.exchangeStartTimeHHMMSS; } 
    public String getStringExchangeStartTimeHHMMSS() { return String.format("%06d", this.exchangeStartTimeHHMMSS); }      
    public int getExchangeCloseTimeHHMM() { return this.exchangeCloseTimeHHMM; }
    public String getStringExchangeCloseTimeHHMM() { return String.format("%04d", this.exchangeCloseTimeHHMM); }    
    public int getExchangeCloseTimeHHMMSS() { return this.exchangeCloseTimeHHMMSS; } 
    public String getStringExchangeCloseTimeHHMMSS() { return String.format("%06d", this.exchangeCloseTimeHHMMSS); }     
    public int getProgramStartTimeHHMM() { return this.programStartTimeHHMM; }
    public String getStringProgramStartTimeHHMM() { return String.format("%04d", this.programStartTimeHHMM); }    
    public int getProgramStartTimeHHMMSS() { return this.programStartTimeHHMMSS; } 
    public String getStringProgramStartTimeHHMMSS() { return String.format("%06d", this.programStartTimeHHMMSS); }     
    public int getProgramCloseTimeHHMM() { return this.programCloseTimeHHMM; }
    public String getStringProgramCloseTimeHHMM() { return String.format("%04d", this.programCloseTimeHHMM); }    
    public int getProgramCloseTimeHHMMSS() { return this.programCloseTimeHHMMSS; }  
    public String getStringProgramCloseTimeHHMMSS() { return String.format("%04d", this.programCloseTimeHHMMSS); }    
    public String getExchangeName() { return this.exchangeName; } 
    public String getExchangeCurrency() { return this.exchangeCurrency; }    
     
}

public class MyUtils {
    
    public void waitForNSeconds(int numSeconds) {

        try {
            Thread.sleep(numSeconds*1000);
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        
    } //End of method    

    public void waitForNMiliSeconds(int numMiliSeconds) {

        try {
            Thread.sleep(numMiliSeconds);
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        
    } //End of method 

    public void waitForStartTime(int startTimeHHMM, TimeZone timeZone, String customMessage, boolean debugFlag) {

        TimeZone.setDefault(timeZone);
        Calendar timeNow = Calendar.getInstance(timeZone);        
        // Provision to wait time of wait is over
        while (Integer.parseInt(String.format("%1$tH%1$tM",timeNow)) < startTimeHHMM) {
            if (debugFlag) {
                System.out.println("Waiting for " + customMessage + " At :" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow));                            
            }
            // Wait for 20 seconds before checking again
            waitForNSeconds(20);
            timeNow = Calendar.getInstance(timeZone);     
        }
        if (debugFlag) {
            System.out.println("Wait for " + customMessage +" over at:" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow));                            
        }
    } // End of method

    public boolean fallsOnExchangeHoliday(String customMessage, String holidayList, Calendar timeToCheck, boolean debugFlag) {

        boolean exchangeHolidayStatus = false;        
        String exchangeHolidayList[] = holidayList.split(",");
        // Provision to check if current Date falls on any of the Exchange Holidays
        for (int index = 0; index < exchangeHolidayList.length; index++) {
            if (String.format("%1$tY%1$tm%1$td",timeToCheck).equalsIgnoreCase(exchangeHolidayList[index]) ||
                (timeToCheck.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) || 
                (timeToCheck.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) ) {
                if (debugFlag) {
                    System.out.println(customMessage + " Date today :" + String.format("%1$tY%1$tm%1$td",timeToCheck));                            
                }
                exchangeHolidayStatus = true;
            }
        }

        return(exchangeHolidayStatus);
    } // End of method
            
    public void waitForMarketsToOpen(int marketOpeningTimeHHMM, TimeZone timeZone, boolean debugFlag) {

        TimeZone.setDefault(timeZone);
        Calendar timeNow = Calendar.getInstance(timeZone);        
        // Provision to wait till market opens for NSE
        while (Integer.parseInt(String.format("%1$tH%1$tM",timeNow)) < marketOpeningTimeHHMM) {
            if (debugFlag) {
                System.out.println("Markets Not Open Yet at:" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow));                            
            }
            // Wait for 20 seconds before checking again
            waitForNSeconds(20);
            timeNow = Calendar.getInstance(timeZone);     
        }
        if (debugFlag) {
            System.out.println("Markets Are Open now at:" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow));                            
        }
    } // End of method

    public boolean marketIsOpen(int marketClosingTimeHHMM, TimeZone timeZone, boolean debugFlag) {

        boolean returnValue = true;
        TimeZone.setDefault(timeZone);
        Calendar timeNow = Calendar.getInstance(timeZone);        
        // return false if time has reached outside market hours for NSE
        if (Integer.parseInt(String.format("%1$tH%1$tM",timeNow)) >= marketClosingTimeHHMM) {
            returnValue = false;
            if (debugFlag) {
                System.out.println("Reached End of Day at :" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",timeNow));                            
            }
        }
        
        //returnValue = true; // For Testing Purposes
        return(returnValue);

    } // End of method
 
    public String popKeyValueFromQueueRedis(JedisPool jedisPool, String queueName, int timeOut, boolean debugFlag) {

        List<String> localList = null;
        boolean caughtException = false;
        Jedis jedis = jedisPool.getResource();

        try {
             localList = jedis.brpop(timeOut, queueName);
         } catch(Exception ex) {
             caughtException = true;
             // do nothing
             System.out.println("Exception Caught. Message : " + ex.getMessage());             
             if (debugFlag) {
                 System.out.println("Caught Exception while popping from queue " + queueName);
             } 
         }          
        jedisPool.returnResource(jedis);

        if ( (localList != null) && (!caughtException) ){
            if (debugFlag) {
                System.out.println("Received Message for  " + queueName + " KEY: " + localList.get(0) + " VALUE: " + localList.get(1));
            }
            return(localList.get(1));            
        } else {
            return(null);
        }
    }    
   
    public String getHashMapValueFromRedis(JedisPool jedisPool, String hashKeyName, String fieldName, boolean debugFlag) {

        String retValue = null;        
        int noOfAttempts = 1;
        boolean exceptionCaught = true;
        Jedis jedis = jedisPool.getResource();        

        while ( (noOfAttempts <= 9) && (exceptionCaught) ) {
            noOfAttempts++;
            exceptionCaught = false;
            String localString = null;
            try {
                localString = jedis.hget(hashKeyName, fieldName);
            } catch(Exception ex) {
                // print exception caught
                exceptionCaught = true;
                System.out.println("Exception Caught. Message : " + ex.getMessage());                
            }  
            if (localString != null && localString.length() > 0 ) {
                retValue = localString;
            } 
            if (exceptionCaught) {
               if (debugFlag) {
                   System.out.println("Caught Exception while reading " + fieldName + " from Redis hashKey " + hashKeyName + " on attempt number " + noOfAttempts);
               } 
               // wait for few milli second before retrying or exiting
               waitForNMiliSeconds(100);
            } else {
               if (debugFlag) {
                    //System.out.println("Read Value from Redis fielname " + fieldName + " from Redis hashKey " + hashKeyName + " as " + localString);
               }                  
            }                         
         }

        jedisPool.returnResource(jedis);
        return(retValue);
    } //End of method  

    public void defragmentOpenPositionsQueue(JedisPool jedisPool, String redisConfigurationKey, boolean debugFlag) {

        String openPositionsQueueKeyName = getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "OPENPOSITIONSQUEUE", debugFlag);
        int maxNumPositions = Integer.parseInt(getHashMapValueFromRedis(jedisPool,redisConfigurationKey, "MAXNUMPAIRPOSITIONS",debugFlag));
        if (debugFlag) {
            System.out.println(" Maximum Positions " + maxNumPositions);                                 
        }
        
        boolean[] currentPosStatus = new boolean[10 * maxNumPositions];
        
        // Get slot numbers which has open positions
        for (int index = 0; index < 10*maxNumPositions; index++) {
            currentPosStatus[index] = false;
            if (checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, Integer.toString(index),false)) {
                if (debugFlag) {
                    System.out.println(" Got Open Positions for Slot Number : " + index + " position details " + getHashMapValueFromRedis(jedisPool, openPositionsQueueKeyName, Integer.toString(index), debugFlag));
                }                                 
                currentPosStatus[index] = true;
            }
        }

        // Traverse the array in reverse and whenever find a position, start filling it from first empty slot
        for (int index = (10*maxNumPositions - 1); index > maxNumPositions ; index--) {
            // found a slot which has position
            if (currentPosStatus[index]) {
                // Get one empty slot
                int slotNumber = 0;
                boolean found = false;
                while ((slotNumber <= 5*maxNumPositions) && (!found)) {
                    slotNumber++;
                    if (!currentPosStatus[slotNumber]) {
                        found = true;
                    }
                }

                if (slotNumber < index) {
                    // got empty slot as SlotNumber
                    if (debugFlag) {
                        System.out.println(" Moving position from Slot : " + index + " to slot " + slotNumber);
                    }                                 
                    // Copy the position from index to slotNumber                
                    String positionValue = getHashMapValueFromRedis(jedisPool, openPositionsQueueKeyName, Integer.toString(index), debugFlag);
                    Jedis jedis = jedisPool.getResource();
                    jedis.hset(openPositionsQueueKeyName, Integer.toString(slotNumber), positionValue);       
                    jedis.hdel(openPositionsQueueKeyName, Integer.toString(index));       
                    jedisPool.returnResource(jedis);
                    // make slotNumber occupied
                    currentPosStatus[slotNumber] = true;
                    // make index empty
                    currentPosStatus[index] = false;                                    
                }
            }                
        }        
    } // End of method

    public void moveCurrentClosedPositions2ArchiveQueue(JedisPool jedisPool, String redisConfigurationKey, boolean debugFlag) {

        String closedPositionsQueueKeyName = getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "CLOSEDPOSITIONSQUEUE", debugFlag);
        String archivePositionsQueueKeyName = getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "PASTCLOSEDPOSITIONSQUEUE", debugFlag);

        // Find the field value of hash to update exited position
        int archivedQueueIndex = 1;
        while (checkIfExistsHashMapField(jedisPool, archivePositionsQueueKeyName, Integer.toString(archivedQueueIndex), debugFlag)) {
            archivedQueueIndex++;
        } 
        
        int indexNumber = 1;
        while (checkIfExistsHashMapField(jedisPool, closedPositionsQueueKeyName, Integer.toString(indexNumber), debugFlag)) {        
            // Move from Closed Positions Queue to Archived Positions Queue
            // read the trade details from closed positions queue
            String tradeDetails = getHashMapValueFromRedis(jedisPool, closedPositionsQueueKeyName, Integer.toString(indexNumber), debugFlag);
            // Update the Archived Queue at archive Index
            Jedis jedis = jedisPool.getResource();                
            jedis.hset(archivePositionsQueueKeyName, Integer.toString(archivedQueueIndex), tradeDetails);
            // Delete from Closed Positions Queue
            jedis.hdel(closedPositionsQueueKeyName, Integer.toString(indexNumber));
            jedisPool.returnResource(jedis);
            // Increment the Archived and Closed position Indexes
            archivedQueueIndex++;
            indexNumber++;
        }        
        
    }
        
    public int getNextOrderID(JedisPool jedisPool,String keyName, boolean debugFlag) {

        int retOrderID = -1;
        int noOfAttempts = 1;
        boolean exceptionCaught = true;
        long incrementedVal = -1;
        Jedis jedis = jedisPool.getResource();

        while ( (noOfAttempts <= 5) && (exceptionCaught) ) {
            noOfAttempts++;
            exceptionCaught = false;
            try {
                incrementedVal = jedis.incr(keyName);
             } catch(Exception ex) {
                 // print exception caught
                 exceptionCaught = true;
                 System.out.println("Exception Caught. Message : " + ex.getMessage());                 
             }          
        }
        if (exceptionCaught) {
            if (debugFlag) {
                System.out.println("Caught Exception while incrementing OrderID " + keyName + " on attempt number " + noOfAttempts);
            } 
            // wait for few milliseconds before retrying or exiting
            waitForNMiliSeconds(100);
        } else {
           retOrderID = (int) incrementedVal;                  
        }        

        jedisPool.returnResource(jedis);
        return(retOrderID);
    }   
    
    public boolean setNextOrderID(JedisPool jedisPool,String keyName, int orderIdValue, boolean debugFlag) {

        boolean successStatus = false;
        int noOfAttempts = 1;
        boolean exceptionCaught = true;
        Jedis jedis = jedisPool.getResource();

        while ( (noOfAttempts <= 5) && (exceptionCaught) ) {
            noOfAttempts++;
            exceptionCaught = false;
            try {
                jedis.set(keyName, Integer.toString(orderIdValue));
                if (debugFlag) { 
                    System.out.println("Setting order ID filed for Key : " + keyName + " orderId : " + orderIdValue);
                }                  
             } catch(Exception ex) {
                 // print exception caught
                 exceptionCaught = true;
                 System.out.println("Exception Caught. Message : " + ex.getMessage());                 
             }          
        }
        if (exceptionCaught) {
            if (debugFlag) {
                System.out.println("Caught Exception while Setting OrderID " + keyName + " on attempt number " + noOfAttempts);
            } 
            // wait for few milliseconds before retrying or exiting
            waitForNMiliSeconds(100);
        } else {
           successStatus = true;                  
        }        

        jedisPool.returnResource(jedis);
        return(successStatus);
    }    
            
    public String getKeyValueFromRedis(JedisPool jedisPool, String KeyName, boolean debugFlag) {

        String retValue = null;        
        int noOfAttempts = 1;
        boolean exceptionCaught = true;
        Jedis jedis = jedisPool.getResource();
        
        while ( (noOfAttempts <= 9) && (exceptionCaught) ) {
            noOfAttempts++;
            exceptionCaught = false;
            String localString = null;
            try {
                 localString = jedis.get(KeyName);
             } catch(Exception ex) {
                 // do nothing
                 exceptionCaught = true;
                 System.out.println("Exception Caught. Message : " + ex.getMessage());
             }  
             if (localString != null && localString.length() > 0 ) {
                 retValue = localString;
             } 
             if (exceptionCaught) {
                 if (debugFlag) {
                     System.out.println("Caught Exception while reading from Redis Key " + KeyName + " on attempt number " + noOfAttempts);
                 } 
                 // wait for one second before retrying or exiting
                 waitForNMiliSeconds(100);
             } else {
                if (debugFlag) {
                   //System.out.println("Read Value from Redis Key " + KeyName + " as " + localString);
                }                  
             }
        }

        jedisPool.returnResource(jedis);
        return(retValue);
    } //End of method            
 
    public boolean checkIfExistsHashMapField(JedisPool jedisPool, String hashKeyName, String fieldName, boolean debugFlag) {

        boolean retValue = false;        
        int noOfAttempts = 0;
        boolean exceptionCaught = true;
        Jedis jedis = jedisPool.getResource();
        
        while ( (noOfAttempts <= 9) && (exceptionCaught) ) {
            noOfAttempts++;
            exceptionCaught = false;
            try {
                retValue = jedis.hexists(hashKeyName, fieldName);
            } catch(Exception ex) {
                // do nothing
                exceptionCaught = true;
                System.out.println("Exception Caught. Message : " + ex.getMessage());                
            }  
            if (exceptionCaught) {
               if (debugFlag) {
                   System.out.println("Caught Exception checking ifExists " + fieldName + " from Redis hashKey " + hashKeyName + " on attempt number " + noOfAttempts);
               } 
               // wait for few milli second before retrying or exiting
               waitForNMiliSeconds(100);
            } else {
               if (debugFlag) {
                    //System.out.println("Checked successfullay ifExists Redis fielname " + fieldName + " from Redis hashKey " + hashKeyName + " as " + retValue);
               }                  
            }                         
         }

        jedisPool.returnResource(jedis);
        return(retValue);
    } //End of method  

    int incrementHHMMByNMinute(String timeStampHHMM, int incrementMinutes) {
        
        int returnValue = 0;

        int hourHH = Integer.parseInt(timeStampHHMM.substring(0 ,2));
        int minuteMM = Integer.parseInt(timeStampHHMM.substring(2 ,4));
        
        minuteMM += incrementMinutes;
        if (minuteMM >= 60) {
            hourHH += 1;
            minuteMM -= 60;
        }
        
        returnValue = hourHH * 100 + minuteMM;
        
        return(returnValue);
    }   
    
    int calcElapsedBars(JedisPool jedisPool, String startTime, String currentTime, TimeZone timeZone, String exchangeHolidayListKeyName, boolean debugFlag) {
        
        int elapsedBars = 0;

        if (debugFlag) {
             //System.out.println("startTime :" + startTime); 
             //System.out.println("currentTime :" + currentTime);               
        }         

        TimeZone.setDefault(timeZone);        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        Calendar startTS = Calendar.getInstance(timeZone);
        try {
            startTS.setTime(sdf.parse(startTime));
        } catch (ParseException ex) {
            elapsedBars = -1;
            Logger.getLogger(MyUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Calendar currentTS = Calendar.getInstance(timeZone);
        try {
            currentTS.setTime(sdf.parse(currentTime));
        } catch (ParseException ex) {
            elapsedBars = -1;
            Logger.getLogger(MyUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        if (elapsedBars == 0) {
            if ( startTS.after(currentTS)  ) {
                elapsedBars = -1;
            } else if (startTS.equals(currentTS)) {
                elapsedBars = 0;
            } else {
                int elapsedMinutes = 0;
                while (startTS.before(currentTS)) {
                    elapsedMinutes++;
                    startTS = incrementTimeStampByOneTradingMinute(startTS, getKeyValueFromRedis(jedisPool, exchangeHolidayListKeyName, false), timeZone );
                }
                elapsedBars = elapsedMinutes / 10; 
                if (debugFlag) {
                     System.out.println("Elapsed Minutes :" + elapsedMinutes + " Elapsed Bars :" + elapsedBars);
                }                      
            }
            
        }
        
        if (debugFlag) {
             //System.out.println("Elapsed Bars " + elapsedBars);
             //System.out.println("Incremented Time Stamp " + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",startTS)); 
             //System.out.println("Current Time Stamp " + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS",currentTS));               
        }          
        
        return (elapsedBars);
    }    
    
    Calendar incrementTimeStampByOneTradingMinute(Calendar timeStamp, String holidayList, TimeZone exchangeTimeZone) {
        
        Calendar retTimeStamp = timeStamp;

        // Increment time stamp by one minute
        retTimeStamp.add(Calendar.MINUTE, 1);

        int exchangeOpeningHour = 9;
        int exchangeOpeningMinute = 15;
        int exchangeClosingTimeHHMM = 1529;
        
        if (exchangeTimeZone.equals(TimeZone.getTimeZone("Asia/Calcutta"))) {
            exchangeOpeningHour = 9;
            exchangeOpeningMinute = 15;
            exchangeClosingTimeHHMM = 1530;            
        } else if (exchangeTimeZone.equals(TimeZone.getTimeZone("America/New_York"))) {
            exchangeOpeningHour = 9;
            exchangeOpeningMinute = 30;
            exchangeClosingTimeHHMM = 1600;                        
        }
        
        // Bring the minute to trading range
        if (Integer.parseInt(String.format("%1$tH%1$tM",retTimeStamp)) >= exchangeClosingTimeHHMM) {
            retTimeStamp.add(Calendar.DATE, 1);
            retTimeStamp.set(Calendar.HOUR_OF_DAY, exchangeOpeningHour );
            retTimeStamp.set(Calendar.MINUTE, exchangeOpeningMinute);            
        }
        
        while (fallsOnExchangeHoliday("", holidayList, retTimeStamp, false)) {
            retTimeStamp.add(Calendar.DATE, 1);             
        }
    
        return(retTimeStamp);
    }

    void unblockOpenPositionSlot(JedisPool jedisPool, String queueKeyName, int slotNumber) {   
   
        // Empty open positions slot
        Jedis jedis = jedisPool.getResource();
        jedis.hdel(queueKeyName, Integer.toString(slotNumber));       
        jedisPool.returnResource(jedis);

    }    

    boolean checkIfStaleMessage(String entryTimeStamp, String currentTimeStamp, int differenceInMinutes) {
        boolean returnValue = false;

        // Compare YYYYMMDD
        if (!entryTimeStamp.substring(0,8).matches(currentTimeStamp.substring(0,8))) {
             //if YYYYMMDD are not same then order is stale
            returnValue = true;
        } else {
            // YYYYMMDD is same. Increment HHMM by 5 minutes and check if it more than current.
            try {
                int incrementedTimeHHMM = incrementHHMMByNMinute(entryTimeStamp.substring(8,12),differenceInMinutes);
                int currentTimeHHMM = Integer.parseInt(currentTimeStamp.substring(8,12));
                
                if (incrementedTimeHHMM < currentTimeHHMM) {
                    // it is more than given minutes stale.
                    returnValue = true;
                }                
            } catch (Exception ex) {
                System.out.println("Exception caused while converting timestamps to HHMM from string. Defaulting to assume it is not a stale Order " + ex.getMessage());
                returnValue = true;                
            }
        }
        
        return(returnValue);
    }
    
}
