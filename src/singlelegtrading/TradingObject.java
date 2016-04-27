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

import java.text.DecimalFormat;

/**
 * @author Manish Kumar Singh
 */
public class TradingObject {

    // Index Values for Object
    public static final int ENTRY_TIMESTAMP_INDEX = 0;
    public static final int NAME_INDEX = 1;
    public static final int SIDE_SIZE_INDEX = 2;
    public static final int STRUCTURE_INDEX = 3;
    public static final int ENTRY_ZSCORE_INDEX = 4;
    public static final int ENTRY_MEAN_INDEX = 5;
    public static final int ENTRY_HALFLIFE_INDEX = 6;
    public static final int ENTRY_STDDEV_INDEX = 7;
    public static final int ENTRY_BID_ASK_FILL_INDEX = 8;
    public static final int ENTRY_REGRESSION_SLOPE_INDEX = 9;
    public static final int ORDER_STATE_INDEX = 10;
    public static final int ENTRY_SPREAD_INDEX = 11;
    public static final int EXPIRY_INDEX = 12;
    public static final int ENTRY_ORDERIDS_INDEX = 13;
    public static final int LOWER_BREACH_INDEX = 14;
    public static final int UPPER_BREACH_INDEX = 15;
    public static final int LAST_KNOWN_SPREAD_INDEX = 16;
    public static final int LAST_UPDATED_TIMESTAMP_INDEX = 17;
    public static final int EXIT_SPREAD_INDEX = 18;
    public static final int EXIT_TIMESTAMP_INDEX = 19;
    public static final int EXIT_ORDERIDS_INDEX = 20;
    public static final int EXIT_BID_ASK_FILL_INDEX = 21;
    public static final int MFE_MAE_INDEX = 22;

    public static final int MAX_NUM_ELEMENTS = 23;

    private String[] tradeObjectStructure;

    // Index Values for contractStructure
    // contractStructure is <underlyingSymbol>_<lotSize>_<contractType>_<optionType>_<optionStrike>
    // e.g. SBIN_300_STK
    // e.g. NIFTY50_75_FUT
    // e.g. NIFTY50_75_OPT_PUT_7500.0 OR NIFTY50_75_OPT_CALL_8500.0    
    public static final int CONTRACT_STRUCT_UNDERLYING_INDEX = 0;
    public static final int CONTRACT_STRUCT_LOTSIZE_INDEX = 1;
    public static final int CONTRACT_STRUCT_TYPE_INDEX = 2;
    public static final int CONTRACT_STRUCT_OPTIONRIGHT_INDEX = 3;
    public static final int CONTRACT_STRUCT_OPTIONSTRIKE_INDEX = 4;    

    TradingObject(String incomingTradeObject) {

        tradeObjectStructure = new String[MAX_NUM_ELEMENTS];

        if (incomingTradeObject.length() > 0) {
            String[] tempTradeObjectStructure = incomingTradeObject.split(",");
            for (int index = 0; index < tempTradeObjectStructure.length; index++) {
                if (index < MAX_NUM_ELEMENTS) {
                    tradeObjectStructure[index] = tempTradeObjectStructure[index];                    
                }
            }
        }
    }

    // assemble and return Structure
    public String getCompleteTradingObjectString() {
        String returnString = "";

        for (int index = 0; index < (tradeObjectStructure.length - 1); index++) {
            returnString = returnString + tradeObjectStructure[index] + ",";
        }

        returnString = returnString + tradeObjectStructure[tradeObjectStructure.length - 1];

        return (returnString);
    }

    public void initiateAndValidate() {

        if ((tradeObjectStructure[ENTRY_ZSCORE_INDEX] != null) && (tradeObjectStructure[ENTRY_ZSCORE_INDEX].length() > 0)) {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[ENTRY_ZSCORE_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[ENTRY_ZSCORE_INDEX]));
        }

        if ((tradeObjectStructure[ENTRY_MEAN_INDEX] != null)
                && (tradeObjectStructure[ENTRY_MEAN_INDEX].length() > 0)) {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[ENTRY_MEAN_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[ENTRY_MEAN_INDEX]));
        }

        if ((tradeObjectStructure[ENTRY_HALFLIFE_INDEX] != null)
                && (tradeObjectStructure[ENTRY_HALFLIFE_INDEX].length() > 0)) {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[ENTRY_HALFLIFE_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[ENTRY_HALFLIFE_INDEX]));
        }

        if ((tradeObjectStructure[ENTRY_STDDEV_INDEX] != null)
                && (tradeObjectStructure[ENTRY_STDDEV_INDEX].length() > 0)) {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[ENTRY_STDDEV_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[ENTRY_STDDEV_INDEX]));
        }

        if ((tradeObjectStructure[ENTRY_SPREAD_INDEX] == null)
                || (tradeObjectStructure[ENTRY_SPREAD_INDEX].length() <= 0)) {
            tradeObjectStructure[ENTRY_SPREAD_INDEX] = String.format("%.2f", 0.0);
        } else {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[ENTRY_SPREAD_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[ENTRY_SPREAD_INDEX]));
        }

        if ((tradeObjectStructure[LAST_KNOWN_SPREAD_INDEX] == null)
                || (tradeObjectStructure[LAST_KNOWN_SPREAD_INDEX].length() <= 0)) {
            tradeObjectStructure[LAST_KNOWN_SPREAD_INDEX] = String.format("%.2f", 0.0);
        } else {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[LAST_KNOWN_SPREAD_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[LAST_KNOWN_SPREAD_INDEX]));
        }

        if ((tradeObjectStructure[LAST_UPDATED_TIMESTAMP_INDEX] == null)
                || (tradeObjectStructure[LAST_UPDATED_TIMESTAMP_INDEX].length() <= 0)) {
            tradeObjectStructure[LAST_UPDATED_TIMESTAMP_INDEX] = String.format("%d", -1);
        }

    }

    public void setEntryTimeStamp(String newTimeStamp) {
        tradeObjectStructure[ENTRY_TIMESTAMP_INDEX] = newTimeStamp;
    }

    public void setSideAndSize(int side, int size) {
        String sideAndSize = "1";
        sideAndSize = Integer.toString(size * side);
        tradeObjectStructure[SIDE_SIZE_INDEX] = sideAndSize;
    }

    public void setEntryBidAskFillDetails(String newBidAskFillDetails) {
        tradeObjectStructure[ENTRY_BID_ASK_FILL_INDEX] = newBidAskFillDetails;
    }

    public void setOrderState(String newState) {
        tradeObjectStructure[ORDER_STATE_INDEX] = newState;
    }

    public void setEntrySpread(double newSpread) {
        DecimalFormat myDf = new DecimalFormat("0.00");
        tradeObjectStructure[ENTRY_SPREAD_INDEX] = myDf.format(Double.valueOf(newSpread));
    }

    public void setExpiry(int expiry) {
        tradeObjectStructure[EXPIRY_INDEX] = Integer.toString(expiry);
    }

    public void setExpiry(String expiry) {
        tradeObjectStructure[EXPIRY_INDEX] = expiry;
    }

    public void setEntryOrderIDs(int entryOrderID) {
        tradeObjectStructure[ENTRY_ORDERIDS_INDEX] = Integer.toString(entryOrderID);
    }

    public void setEntryOrderIDs(String newOrderIDs) {
        tradeObjectStructure[ENTRY_ORDERIDS_INDEX] = newOrderIDs;
    }

    public void setLowerBreach(int newBreach) {
        tradeObjectStructure[LOWER_BREACH_INDEX] = Integer.toString(newBreach);
    }

    public void setLowerBreach(double newBreach) {
        DecimalFormat myDf = new DecimalFormat("0.00");
        tradeObjectStructure[LOWER_BREACH_INDEX] = myDf.format(Double.valueOf(newBreach));
    }

    public void setUpperBreach(int newBreach) {
        tradeObjectStructure[UPPER_BREACH_INDEX] = Integer.toString(newBreach);
    }

    public void setUpperBreach(double newBreach) {
        DecimalFormat myDf = new DecimalFormat("0.00");
        tradeObjectStructure[UPPER_BREACH_INDEX] = myDf.format(Double.valueOf(newBreach));
    }

    public void setEntryStdDev(double newEntryStdDev) {
        tradeObjectStructure[ENTRY_STDDEV_INDEX] = Double.toString(newEntryStdDev);
    }

    public void setLastKnownSpread(double newSpread) {
        tradeObjectStructure[LAST_KNOWN_SPREAD_INDEX] = Double.toString(newSpread);
    }

    public void setLastUpdatedTimeStamp(String newTimeStamp) {
        tradeObjectStructure[LAST_UPDATED_TIMESTAMP_INDEX] = newTimeStamp;
    }

    public void setExitSpread(double newSpread) {
        DecimalFormat myDf = new DecimalFormat("0.00");
        tradeObjectStructure[EXIT_SPREAD_INDEX] = myDf.format(Double.valueOf(newSpread));
    }

    public void setExitTimeStamp(String newTimeStamp) {
        tradeObjectStructure[EXIT_TIMESTAMP_INDEX] = newTimeStamp;
    }

    public void setExitOrderIDs(int exitOrderID) {
        tradeObjectStructure[EXIT_ORDERIDS_INDEX] = Integer.toString(exitOrderID);
    }

    public void setExitOrderIDs(String newOrderIDs) {
        tradeObjectStructure[EXIT_ORDERIDS_INDEX] = newOrderIDs;
    }

    public void setExitBidAskFillDetails(String newBidAskFillDetails) {
        tradeObjectStructure[EXIT_BID_ASK_FILL_INDEX] = newBidAskFillDetails;
    }

    public String getEntryTimeStamp() {
        return (tradeObjectStructure[ENTRY_TIMESTAMP_INDEX]);
    }

    public String getTradingObjectName() {
        return (tradeObjectStructure[NAME_INDEX]);
    }

    public int getSideAndSize() {
        
        int sideAndSize = 0; // Default value
     
        try {
            sideAndSize = Integer.parseInt(tradeObjectStructure[SIDE_SIZE_INDEX]);
        } catch (NumberFormatException | NullPointerException ex) {
            sideAndSize = 0;
        }            
        return (sideAndSize);
    }        

    public String getContractStructure() {
        // contractStructure is <underlyingSymbol>_<lotSize>_<contractType>_<optionType>_<optionStrike>
        // e.g. SBIN_300_STK
        // e.g. NIFTY50_75_FUT
        // e.g. NIFTY50_75_OPT_PUT_7500.0 OR NIFTY50_75_OPT_CALL_8500.0
        //CONTRACT_STRUCT_UNDERLYING_INDEX = 0;
        //CONTRACT_STRUCT_LOTSIZE_INDEX = 1;
        //CONTRACT_STRUCT_TYPE_INDEX = 2;
        //CONTRACT_STRUCT_OPTIONRIGHT_INDEX = 3;
        //CONTRACT_STRUCT_OPTIONSTRIKE_INDEX = 4;           
        
        return (tradeObjectStructure[STRUCTURE_INDEX]);
    }   

    public String getContractUnderlyingName() {
        
        String underlyingName = "UNDERLYING"; // Default value
        String[] contractStructure = this.getContractStructure().split("_");
        if (contractStructure.length >= CONTRACT_STRUCT_UNDERLYING_INDEX) {
            underlyingName = contractStructure[CONTRACT_STRUCT_UNDERLYING_INDEX];
        }
        return (underlyingName);
    }    
    
    public int getContractLotSize() {
        
        int lotSize = 0; // Default value
        String[] contractStructure = this.getContractStructure().split("_");
        if (contractStructure.length >= CONTRACT_STRUCT_LOTSIZE_INDEX) {          
            try {
                lotSize = Integer.parseInt(contractStructure[CONTRACT_STRUCT_LOTSIZE_INDEX]);
            } catch (NumberFormatException | NullPointerException ex) {
                lotSize = 0;
            }            
        }
        return (lotSize);
    }
    
    public String getContractType() {
        
        String contractType = "FUT"; // Default value
        String[] contractStructure = this.getContractStructure().split("_");
        if (contractStructure.length >= CONTRACT_STRUCT_TYPE_INDEX) {
            contractType = contractStructure[CONTRACT_STRUCT_TYPE_INDEX];
        }
        return (contractType);
    }     

    public String getContractOptionRightType() {
        
        String optionRightType = "CALL"; // Default value
        String[] contractStructure = this.getContractStructure().split("_");
        if (contractStructure.length >= CONTRACT_STRUCT_OPTIONRIGHT_INDEX) {
            optionRightType = contractStructure[CONTRACT_STRUCT_OPTIONRIGHT_INDEX];
        }
        return (optionRightType);
    }    

    public double getContractOptionStrike() {
        
        double optionStrike = 0.0; // Default value
        String[] contractStructure = this.getContractStructure().split("_");
        if (contractStructure.length >= CONTRACT_STRUCT_OPTIONSTRIKE_INDEX) {          
            try {
                optionStrike = Double.parseDouble(contractStructure[CONTRACT_STRUCT_OPTIONSTRIKE_INDEX]);
            } catch (NumberFormatException | NullPointerException ex) {
                optionStrike = 0.0;
            }          
        }
        return (optionStrike);
    }
    
    public String getZScore() {
        return (tradeObjectStructure[ENTRY_ZSCORE_INDEX]);
    }

    public String getEntryMean() {
        return (tradeObjectStructure[ENTRY_MEAN_INDEX]);
    }

    public String getHalfLife() {
        return (tradeObjectStructure[ENTRY_HALFLIFE_INDEX]);
    }

    public String getEntryStdDev() {
        return (tradeObjectStructure[ENTRY_STDDEV_INDEX]);
    }

    public String getEntryBidAskFillDetails() {
        return (tradeObjectStructure[ENTRY_BID_ASK_FILL_INDEX]);
    }

    public String getOrderState() {
        return (tradeObjectStructure[ORDER_STATE_INDEX]);
    }

    public String getEntrySpread() {
        return (tradeObjectStructure[ENTRY_SPREAD_INDEX]);
    }

    public String getExpiry() {
        return (tradeObjectStructure[EXPIRY_INDEX]);
    }

    public String getEntryOrderIDs() {
        return (tradeObjectStructure[ENTRY_ORDERIDS_INDEX]);
    }

    public String getLowerBreach() {
        return (tradeObjectStructure[LOWER_BREACH_INDEX]);
    }

    public String getUpperBreach() {
        return (tradeObjectStructure[UPPER_BREACH_INDEX]);
    }

    public String getLastKnownSpread() {
        return (tradeObjectStructure[LAST_KNOWN_SPREAD_INDEX]);
    }

    public String getLastUpdatedTimeStamp() {
        return (tradeObjectStructure[LAST_UPDATED_TIMESTAMP_INDEX]);
    }

    public String getExitSpread() {
        return (tradeObjectStructure[EXIT_SPREAD_INDEX]);
    }

    public String getExitTimeStamp() {
        return (tradeObjectStructure[EXIT_TIMESTAMP_INDEX]);
    }

    public String getExitOrderIDs() {
        return (tradeObjectStructure[EXIT_ORDERIDS_INDEX]);
    }

    public String getExitBidAskFillDetails() {
        return (tradeObjectStructure[EXIT_BID_ASK_FILL_INDEX]);
    }

}
