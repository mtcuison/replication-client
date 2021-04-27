package org.rmj.lr.dcp;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.appdriver.agentfx.CommonUtils;
import org.rmj.appdriver.agentfx.FileUtil;
import org.rmj.replication.utility.LogWrapper;
import org.rmj.replication.utility.WebClient;

/**
 *
 * @author mac
 */
public class DCPUtil {
    private static final String DRIVE = "D:/dcp/";
    
    public static boolean Export2File(GRiderX foApp, String fsTransNox, LogWrapper foLog){
        //check if application driver was set
        if (foApp == null){
            foLog.severe("Application driver is not set.");            
            return false;
        }
        
        //check if transaction number was valid
        String lsSQL = "SELECT" +
                            "  a.sTransNox" +
                            ", a.dTransact" +
                            ", a.sReferNox" +
                            ", b.sCompnyNm xCollName" +
                            ", c.sRouteNme" +
                            ", a.dReferDte" +
                            ", a.cTranStat" +
                            ", a.cDCPTypex" +
                            ", a.nEntryNox" +
                            ", d.sBranchNm" +
                            ", a.sCollctID" +
                        " FROM LR_DCP_Collection_Master a" +
                            " LEFT JOIN Client_Master b" +
                                " ON a.sCollctID = b.sClientID" +
                            " LEFT JOIN Route_Area c" +
                                " ON a.sCollctID = c.sCollctID" +
                            " LEFT JOIN Branch d" +
                                " ON LEFT(a.sTransNox, 4) = d.sBranchCd" +
                        " WHERE a.sTransNox = " + SQLUtil.toSQL(fsTransNox);
        ResultSet loRS = foApp.executeQuery(lsSQL);
        
        if (MiscUtil.RecordCount(loRS) <= 0){
            foLog.severe("No record found based on the given criteria.");            
            return false;
        }
        
        JSONObject loJSON = new JSONObject();
        
        //convert resultset to json array
        JSONArray loArray = CommonUtils.RS2JSON(loRS);
        
        if (loArray.isEmpty()){
            foLog.severe("Error exporting database record.");            
            return false;
        }
        
        //convert json array  to json object
        JSONObject loMaster = (JSONObject) loArray.get(0);
        loJSON.put("master", loMaster);
        
        //get transaction detail records
        lsSQL = "SELECT" +
                    "  a.nEntryNox" +
                    ", a.sAcctNmbr" +
                    ", c.sCompnyNm xFullName" +
                    ", a.cIsDCPxxx" +
                    ", IFNULL(c.sHouseNox, '') sHouseNox" +
                    ", c.sAddressx" +
                    ", d.sBrgyName" +
                    ", e.sTownName" +
                    ", b.nAmtDuexx" +
                    ", IFNULL(f.cApntUnit, '') cApntUnit" +
                    ", b.dDueDatex" +
                    ", IFNULL(g.nLongitud, 0) nLongitud" +
                    ", IFNULL(g.nLatitude, 0) nLatitude" +
                    ", b.sClientID" +
                    ", h.sSerialID" +
                    ", h.sEngineNo sSerialNo" +
                    ", c.sMobileNo" + 
                    ", b.nMonAmort" +
                    ", b.nABalance" +
                    ", b.nDelayAvg" +
                    ", b.nLastPaym" +
                    ", b.dLastPaym" +
                " FROM LR_DCP_Collection_Detail a" +
                    " LEFT JOIN MC_AR_Master b" +
                        " ON a.sAcctNmbr = b.sAcctNmbr" +
                    " LEFT JOIN Client_Master c" +
                        " ON b.sClientID = c.sClientID" +
                    " LEFT JOIN Barangay d" +
                        " ON c.sBrgyIDxx = d.sBrgyIDxx" +
                    " LEFT JOIN TownCity e" +
                        " ON c.sTownIDxx = e.sTownIDxx" +
                    " LEFT JOIN LR_Collection_Unit f" +
                        " ON a.sAcctNmbr = f.sAcctNmbr" +
                            " AND (f.dAppointx >= '$transact'" +
                                " OR (f.dDueUntil >= '$transact' AND f.dAppointx IS NULL))" +
                    " LEFT JOIN Client_Coordinates g" +
                        " ON b.sClientID = g.sClientID" +
                    " LEFT JOIN MC_Serial h" +
                        " ON b.sSerialID = h.sSerialID" +
                " WHERE a.sTransNox = " + SQLUtil.toSQL(fsTransNox);  
        loRS = foApp.executeQuery(lsSQL);
        
        if (MiscUtil.RecordCount(loRS) <= 0){
            foLog.severe("No record found based on the given criteria.");            
            return false;
        }        
        
        //convert resultset to json array   
        loArray = CommonUtils.RS2JSON(loRS);
        
        if (loArray.isEmpty()){
            foLog.severe("Error exporting database record.");                       
            return false;
        }
        
        loJSON.put("detail", loArray);
        
        File afile = new File(DRIVE);
        if (!afile.exists()) {
            if (!afile.mkdir()){
                foLog.severe("Unable to create output folder.");                       
                return false;
            }
        }
        
        //export json object to file
        String lsFile = DRIVE + fsTransNox + "-out.txt";
        FileUtil.fileWrite(lsFile, loJSON.toJSONString());
        
        return FileUtil.exists(lsFile);
    }
    
    public static boolean Export2FileAndroid(GRiderX foApp, String fsTransNox, LogWrapper foLog){
        //check if application driver was set
        if (foApp == null){
            foLog.severe("Application driver is not set.");            
            return false;
        }
        
        //check if transaction number was valid
        String lsSQL = "SELECT * FROM LR_DCP_Collection_Master WHERE sTransNox = " + SQLUtil.toSQL(fsTransNox);
        ResultSet loRS = foApp.executeQuery(lsSQL);
        
        if (MiscUtil.RecordCount(loRS) <= 0){
            foLog.severe("No record found based on the given criteria.");            
            return false;
        }
        
        JSONObject loJSON = new JSONObject();
        
        //get transaction detail records
        lsSQL = "SELECT * FROM LR_DCP_Collection_Detail_Android WHERE sTransNox = " + SQLUtil.toSQL(fsTransNox);
        loRS = foApp.executeQuery(lsSQL);
        
        if (MiscUtil.RecordCount(loRS) <= 0){
            foLog.severe("No record found based on the given criteria.");            
            return false;
        }        
        
        //convert resultset to json array   
        JSONArray loArray = CommonUtils.RS2JSON(loRS);
        
        if (loArray.isEmpty()){
            foLog.severe("Error exporting database record.");                       
            return false;
        }
        
        loJSON.put("android", loArray);
        
        File afile = new File(DRIVE);
        if (!afile.exists()) {
            if (!afile.mkdir()){
                foLog.severe("Unable to create output folder.");                       
                return false;
            }
        }
        
        //export json object to file
        String lsFile = DRIVE + fsTransNox + "-mob.txt";
        FileUtil.fileWrite(lsFile, loJSON.toJSONString());
        
        return FileUtil.exists(lsFile);
    }
    
    public static boolean ImportFromFile(GRiderX foApp, String fsFileName, LogWrapper foLog){
        //check if application driver was set
        if (foApp == null){
            foLog.severe("Application driver is not set.");            
            return false;
        }
        
        if (!FileUtil.exists(fsFileName)){
            foLog.severe("File to import does not exist.");            
            return false;
        }
        
        String lsValue = FileUtil.fileRead(fsFileName);
        
        if (lsValue.isEmpty()){
            foLog.severe("File is empty.");            
            return false;
        }
                
        try {
            JSONParser loParser = new JSONParser();
            JSONObject loJSON = (JSONObject) loParser.parse(lsValue);
            JSONArray loArray = (JSONArray) loJSON.get("android");
            
            foApp.beginTrans();
            for (int lnCtr = 0; lnCtr <= loArray.size()-1; lnCtr++){
                loJSON = (JSONObject) loArray.get(lnCtr);
                
                lsValue = "INSERT INTO LR_DCP_Collection_Detail_Android SET" + 
                            "  sTransNox = " + SQLUtil.toSQL((String) loJSON.get("sTransNox")) +
                            ", nEntryNox = " + Integer.parseInt((String) loJSON.get("nEntryNox")) +
                            ", sAcctNmbr = " + SQLUtil.toSQL((String) loJSON.get("sAcctNmbr")) +
                            ", sRemCodex = " + SQLUtil.toSQL((String) loJSON.get("sRemCodex")) +
                            ", sJsonData = '" + (String) loJSON.get("sJsonData") + "'" +
                            ", dReceived = " + SQLUtil.toSQL((String) loJSON.get("dReceived")) +
                            ", sUserIDxx = " + SQLUtil.toSQL((String) loJSON.get("sUserIDxx")) +
                            ", sDeviceID = " + SQLUtil.toSQL((String) loJSON.get("sDeviceID")) +
                            ", dModified = " + SQLUtil.toSQL((String) loJSON.get("dModified")) +
                            ", dTimeStmp = " + SQLUtil.toSQL((String) loJSON.get("dTimeStmp"));
                
                if (foApp.executeUpdate(lsValue) <= 0){
                    foApp.rollbackTrans();
                    foLog.severe(foApp.getErrMsg() + "; " + foApp.getMessage());
                    return false;
                }
            }
            foApp.commitTrans();
        } catch (ParseException ex) {
            foLog.severe(ex.getMessage());
            return false;
        }
        
        return true;
    }
    
    public static boolean ImportFromServer(GRiderX foApp, String fsTransNox, LogWrapper foLog){
        Calendar calendar = Calendar.getInstance();
        //Create the header section needed by the API
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Accept", "application/json");
        headers.put("Content-Type", "application/json");
        headers.put("g-api-id", foApp.getProductID());
        headers.put("g-api-imei", MiscUtil.getPCName());
        headers.put("g-api-key", SQLUtil.dateFormat(calendar.getTime(), "yyyyMMddHHmmss"));
        headers.put("g-api-hash", org.apache.commons.codec.digest.DigestUtils.md5Hex((String)headers.get("g-api-imei") + (String)headers.get("g-api-key")));
        headers.put("g-api-client", foApp.getClientID());
        headers.put("g-api-user", foApp.getUserID());
        headers.put("g-api-log", "");
        headers.put("g-api-token", "");
        
        JSONObject param = new JSONObject();
        param.put("sTransNox", fsTransNox);
              
        try {
            String lsAPI = "https://restgk.guanzongroup.com.ph/integsys/dcp/dcp_android_download.php";
            String response = WebClient.httpPostJSon(lsAPI, param.toJSONString(), (HashMap<String, String>) headers);
            
            if(response == null){
                JSONObject err_detl = new JSONObject();
                err_detl.put("message", System.getProperty("store.error.info"));
                JSONObject err_mstr = new JSONObject();
                err_mstr.put("result", "ERROR");
                err_mstr.put("error", err_detl);
                foLog.severe(err_mstr.toJSONString());
                return false;
            }
            JSONParser loParser = new JSONParser();
            param = (JSONObject) loParser.parse(response);
            
            if (!"success".equals((String) param.get("result"))){
                foLog.severe(param.toJSONString());
                return false;
            }
            
            JSONObject loJSON = (JSONObject) loParser.parse(response);
            JSONArray loArray = (JSONArray) loJSON.get("detail");
            loJSON = new JSONObject();
            loJSON.put("android", loArray);
            
            File afile = new File(DRIVE);
            if (!afile.exists()) {
                if (!afile.mkdir()){
                    foLog.severe("Unable to create output folder.");                       
                    return false;
                }
            }
            
            String lsFile = DRIVE + fsTransNox + "-mob.txt";
            FileUtil.fileWrite(lsFile, loJSON.toJSONString());
            
            return FileUtil.exists(lsFile);
        } catch (IOException | ParseException ex) {
            foLog.severe(ex.getMessage());
            return false;
        }
    }
    
    public static boolean ImportFromServerX(GRiderX foApp, String fsTransNox, LogWrapper foLog){
        Calendar calendar = Calendar.getInstance();
        //Create the header section needed by the API
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Accept", "application/json");
        headers.put("Content-Type", "application/json");
        headers.put("g-api-id", foApp.getProductID());
        headers.put("g-api-imei", MiscUtil.getPCName());
        headers.put("g-api-key", SQLUtil.dateFormat(calendar.getTime(), "yyyyMMddHHmmss"));
        headers.put("g-api-hash", org.apache.commons.codec.digest.DigestUtils.md5Hex((String)headers.get("g-api-imei") + (String)headers.get("g-api-key")));
        headers.put("g-api-client", foApp.getClientID());
        headers.put("g-api-user", foApp.getUserID());
        headers.put("g-api-log", "");
        headers.put("g-api-token", "");
        
        JSONObject param = new JSONObject();
        param.put("sTransNox", fsTransNox);
              
        try {
            String lsAPI = "https://restgk.guanzongroup.com.ph/integsys/dcp/dcp_android_download.php";
            String response = WebClient.httpPostJSon(lsAPI, param.toJSONString(), (HashMap<String, String>) headers);
            
            if(response == null){
                JSONObject err_detl = new JSONObject();
                err_detl.put("message", System.getProperty("store.error.info"));
                JSONObject err_mstr = new JSONObject();
                err_mstr.put("result", "ERROR");
                err_mstr.put("error", err_detl);
                foLog.severe(err_mstr.toJSONString());
                return false;
            }
            JSONParser loParser = new JSONParser();
            param = (JSONObject) loParser.parse(response);
            
            if (!"success".equals((String) param.get("result"))){
                foLog.severe(param.toJSONString());
                return false;
            }
            
            JSONObject loJSON = (JSONObject) loParser.parse(response);
            JSONArray loArray = (JSONArray) loJSON.get("detail");
            
            foApp.beginTrans();
            for (int lnCtr = 0; lnCtr <= loArray.size()-1; lnCtr++){
                loJSON = (JSONObject) loArray.get(lnCtr);
                
                response = "INSERT INTO LR_DCP_Collection_Detail_Android SET" + 
                            "  sTransNox = " + SQLUtil.toSQL((String) loJSON.get("sTransNox")) +
                            ", nEntryNox = " + Integer.parseInt((String) loJSON.get("nEntryNox")) +
                            ", sAcctNmbr = " + SQLUtil.toSQL((String) loJSON.get("sAcctNmbr")) +
                            ", sRemCodex = " + SQLUtil.toSQL((String) loJSON.get("sRemCodex")) +
                            ", sJsonData = '" + (String) loJSON.get("sJsonData") + "'" +
                            ", dReceived = " + SQLUtil.toSQL((String) loJSON.get("dReceived")) +
                            ", sUserIDxx = " + SQLUtil.toSQL((String) loJSON.get("sUserIDxx")) +
                            ", sDeviceID = " + SQLUtil.toSQL((String) loJSON.get("sDeviceID")) +
                            ", dModified = " + SQLUtil.toSQL((String) loJSON.get("dModified")) +
                            ", dTimeStmp = " + SQLUtil.toSQL((String) loJSON.get("dTimeStmp"));
                
                if (foApp.executeUpdate(response) <= 0){
                    foApp.rollbackTrans();
                    foLog.severe(foApp.getErrMsg() + "; " + foApp.getMessage());
                    return false;
                }
            }
            foApp.commitTrans();
        } catch (IOException | ParseException ex) {
            foLog.severe(ex.getMessage());
            return false;
        }
        
        return true;
    }
    
    public static JSONObject getDelay(GRiderX foApp, String fsAcctNmbr){
        JSONObject loJSON = new JSONObject();
        JSONObject loErr = new JSONObject();
        
        if (fsAcctNmbr.isEmpty()){
            loJSON.put("result", "error");
            loErr.put("code", "100");
            loErr.put("message", "No ACCOUNT was specified.");
            loJSON.put("error", loErr);
            return loJSON;
        }
        
        String lsSQL = MiscUtil.addCondition(getSQL_Account(), "a.sAcctNmbr = " + SQLUtil.toSQL(fsAcctNmbr));
        ResultSet loRS = foApp.executeQuery(lsSQL);
        
        try {
            if (!loRS.next()){
                loJSON.put("result", "error");
                loErr.put("code", "100");
                loErr.put("message", "No record found.");
                loJSON.put("error", loErr);
                return loJSON;
            }
            
            if (loRS.getInt("nAcctTerm") == 0){                   
                loJSON.put("result", "success");
                loJSON.put("delayavg", loRS.getDate("dDueDatex").after(foApp.getServerDate()) ? 0 : 1);
                loJSON.put("amtduexx", loRS.getDouble("nAmtDuexx"));
                return loJSON;
            }
            
            int lnAcctTerm;
            if (CommonUtils.dateDiff(loRS.getDate("dFirstPay"), foApp.getServerDate()) > 1)
                lnAcctTerm = 0;
            else
                lnAcctTerm = (int) CommonUtils.monthDiff(SQLUtil.dateFormat(loRS.getDate("dFirstPay"), SQLUtil.FORMAT_SHORT_DATE), SQLUtil.dateFormat(foApp.getServerDate(), SQLUtil.FORMAT_SHORT_DATE)) + 1;
            System.out.println("Account Term: " + lnAcctTerm);
                
            if (lnAcctTerm > loRS.getInt("nAcctTerm"))
                lnAcctTerm = loRS.getInt("nAcctTerm");
            else {
                if (CommonUtils.getDateDay(foApp.getServerDate()) <= CommonUtils.getDateDay(SQLUtil.toDate(loRS.getString("dFirstPay") + " 00:00:00", SQLUtil.FORMAT_TIMESTAMP))){
                    lnAcctTerm = lnAcctTerm - 1;
                } else if (CommonUtils.dateDiff(foApp.getServerDate(), loRS.getDate("dFirstPay")) < 30){
                    lnAcctTerm = lnAcctTerm - 1;
                }
            }
            System.out.println("Account Term: " + lnAcctTerm);
            
            double lnABalance = loRS.getDouble("nGrossPrc") + loRS.getDouble("nDebtTotl") - 
                    (loRS.getDouble("nDownTotl") + loRS.getDouble("nCashTotl") + loRS.getDouble("nPaymTotl") +
                    loRS.getDouble("nRebTotlx") + loRS.getDouble("nCredTotl"));
            System.out.println("Account Balance: " + lnABalance);
            
            
            double lnAmtDuexx = (lnAcctTerm * loRS.getDouble("nMonAmort") + loRS.getDouble("nDownPaym") + 
                                loRS.getDouble("nCashBalx")) - (loRS.getDouble("nGrossPrc") - lnABalance);
            System.out.println("Amount Due: " + lnAmtDuexx);
            
            double lnDelayAvg = 0.00;
            if (lnAmtDuexx > 0.00){
                if (loRS.getDouble("nMonAmort") > 0.00)
                    lnDelayAvg = Math.round(lnAmtDuexx / loRS.getDouble("nMonAmort") * 100.0) /100.0;
                else
                    if (loRS.getDate("nMonAmort").before(foApp.getServerDate())) lnDelayAvg = 1;          
            } else
                lnDelayAvg = Math.round(lnAmtDuexx / loRS.getDouble("nMonAmort") * 100.0) /100.0;
            System.out.println("Delay Average: " + lnDelayAvg);
                
            if (lnAmtDuexx > lnABalance) lnAmtDuexx = lnABalance;
            System.out.println("Amount Due: " + lnAmtDuexx);
            
            loJSON.put("result", "success");
            loJSON.put("acctterm", lnAcctTerm);
            loJSON.put("amtduexx", lnAmtDuexx);
            loJSON.put("abalance", lnABalance);
            loJSON.put("delayavg", lnDelayAvg);
            return loJSON;
        } catch (SQLException ex) {
            loJSON.put("result", "error");
            loErr.put("code", "100");
            loErr.put("message", ex.getMessage());
            loJSON.put("error", loErr);
            return loJSON;
        } 
    }
    
    private static String getSQL_Account(){
        return "SELECT" +
                    "  a.sAcctNmbr" +
                    ", a.sApplicNo" +
                    ", CONCAT(b.sLastName, ', ', b.sFrstName, IF(IFNull(b.sSuffixNm, '') = '', ' ', CONCAT(' ', b.sSuffixNm, ' ')), b.sMiddName) xFullName" +
                    ", CONCAT(b.sAddressx, ', ', c.sTownName, ', ', d.sProvName, ' ', c.sZippCode) xAddressx" +
                    ", a.sRemarksx" +
                    ", CONCAT(g.sBrandNme, ' ', f.sModelNme) as xModelNme" +
                    ", e.sEngineNo" +
                    ", e.sFrameNox" +
                    ", h.sColorNme" +
                    ", CONCAT(b.sLastName, ', ', b.sFrstName, ' ', b.sMiddName) xCCounNme" +
                    ", j.sRouteNme" +
                    ", CONCAT(p.sLastName, ', ', p.sFrstName, ' ', p.sMiddName) xCollectr" +
                    ", CONCAT(q.sLastName, ', ', q.sFrstName, ' ', q.sMiddName) xManagerx" +
                    ", m.sBranchNm xCBranchx" +
                    ", a.dPurchase" +
                    ", a.dFirstPay" +
                    ", a.nAcctTerm" +
                    ", a.dDueDatex" +
                    ", a.nGrossPrc" +
                    ", a.nDownPaym" +
                    ", a.nCashBalx" +
                    ", a.nPNValuex" +
                    ", a.nMonAmort" +
                    ", a.nPenaltyx" +
                    ", a.nRebatesx" +
                    ", a.nLastPaym" +
                    ", a.dLastPaym" +
                    ", a.nPaymTotl" +
                    ", a.nRebTotlx" +
                    ", a.nDebtTotl" +
                    ", a.nCredTotl" +
                    ", a.nAmtDuexx" +
                    ", a.nABalance" +
                    ", a.nDownTotl" +
                    ", a.nCashTotl" +
                    ", a.nDelayAvg" +
                    ", a.cRatingxx" +
                    ", a.cAcctstat" +
                    ", a.sClientID" +
                    ", a.sExAcctNo" +
                    ", a.sSerialID" +
                    ", a.cMotorNew" +
                    ", a.dClosedxx" +
                    ", a.cActivexx" +
                    ", a.nLedgerNo" +
                    ", a.cLoanType" +
                    ", b.sTownIDxx" +
                    ", a.sRouteIDx" +
                    ", a.nPenTotlx" +
                    ", i.sTransNox" +
                    ", a.sModified" +
                    ", a.dModified" +
                    ", CONCAT(n.sLastName, ', ', n.sFrstName, IF(IFNull(n.sSuffixNm, '') = '', ' ', CONCAT(' ', n.sSuffixNm, ' ')), n.sMiddName) xCoCltNm1" +
                    ", CONCAT(o.sLastName, ', ', o.sFrstName, IF(IFNull(o.sSuffixNm, '') = '', ' ', CONCAT(' ', o.sSuffixNm, ' ')), o.sMiddName) xCoCltNm2" +
                    ", a.sCoCltID1" +
                    ", a.sCoCltID2" +
                    ", CONCAT(r.sLastName, ', ', r.sFrstName, ' ', r.sMiddName) zCollectr" +
                    ", CONCAT(s.sLastName, ', ', s.sFrstName, ' ', s.sMiddName) zManagerx" +
                    ", t.nLatitude" +
                    ", t.nLongitud" +
                    ", b.sBrgyIDxx" +
                " FROM MC_AR_Master  a" +
                        " LEFT JOIN MC_Serial e" +
                            " LEFT JOIN MC_Model f" +
                                " LEFT JOIN Brand g ON f.sBrandIDx = g.sBrandIDx" +
                            " ON e.sModelIDx = f.sModelIDx" +
                            " LEFT JOIN Color h ON e.sColorIDx = h.sColorIDx" +
                            " ON a.sSerialID = e.sSerialID" +
                        " LEFT JOIN MC_Credit_Application i ON a.sApplicNo = i.sTransNox" +
                        " LEFT JOIN Client_Master n ON a.sCoCltID1 = n.sClientID" +
                        " LEFT JOIN Client_Master o ON a.sCoCltID2 = o.sClientID" +
                        " LEFT JOIN Client_Coordinates t ON a.sClientID = t.sClientID" +
                        " LEFT JOIN Route_Area j" +
                        " LEFT JOIN Employee_Master001 k" +
                            " LEFT JOIN Client_Master p" +
                                " ON k.sEmployID = p.sClientID" +
                            " ON j.sCollctID = k.sEmployID" +
                        " LEFT JOIN Employee_Master001 l" +
                            " LEFT JOIN Client_Master q" +
                                " ON l.sEmployID = q.sClientID" +
                            " ON j.sManagrID = l.sEmployID" +
                        " LEFT JOIN Branch m ON j.sBranchCd = m.sBranchCd" +
                        " LEFT JOIN Employee_Master r ON j.sCollctID = r.sEmployID" +
                            " LEFT JOIN Employee_Master s" +
                                " ON j.sManagrID = s.sEmployID" +
                            " ON a.sRouteIDx = j.sRouteIDx" +
                    ", Client_Master b" +
                    ", TownCity c" +
                    ", Province d" +
                " WHERE a.sClientID = b.sClientID" +
                    " AND b.sTownIDxx = c.sTownIDxx" +
                    " AND c.sProvIDxx = d.sProvIDxx" +
                    " AND a.cLoanType <> '4'" +
                    " AND a.cAcctStat = '0'";
    }
}
