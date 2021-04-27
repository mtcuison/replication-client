/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.rmj.branch.util;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GDBFChain;
import org.rmj.appdriver.GProperty;
import org.rmj.appdriver.SQLUtil;

/**
 * Trims the xxxReplicationLog and retains the 6 months log.
 * Trims the other records not belonging to the branch if all parameter is specified.
 * @author sayso
 * ++++++++++++++++++++++++++++ 
 * Rules:
 *    connects only to the localhost
 *    set the Branch_Others->sDBHostNm to the current Server Name 
 * Usage:
 *    TrimMySQL [all]  
 */
public class TrimMySQL {
    private static GDBFChain dblocal;
    //make sure that we are connecting to the local database only
    private static String psDBSrvrNm = "localhost";
     
    public static void main(String[] args) {
        boolean blogonly = true;
        
        if(args.length == 1){
            if(args[0].equalsIgnoreCase("all")){
                blogonly = false;
            }
        }
        
        String path;
        if(System.getProperty("os.name").toLowerCase().contains("win")){
            path = "D:/GGC_Java_Systems";
        }
        else{
            path = "/srv/GGC_Java_Systems";
        }

        System.setProperty("sys.default.path.config", path);
        System.setProperty("sys.default.signature", "08220326");
        
        loadConfig("gRider");
        
        dblocal = new GDBFChain();
        dblocal.doDBFChain("", "", System.getProperty("sys.default.dbsrvr"), System.getProperty("sys.default.dbname"), System.getProperty("sys.default.dbuser"), System.getProperty("sys.default.dbpass"), System.getProperty("sys.default.dbport"));

        if(!validHostName()){
            System.exit(0);
        }
        
        ResultSet loBranches = getBranches();
        
        if(loBranches == null){
            System.out.println("Can't load list of branches to trim");
            System.exit(0);
        }
        
        try {
            while(loBranches.next()){
                System.out.println("Trimming logs of " + loBranches.getString("sBranchNm") + " with branch code " + loBranches.getString("sBranchCD"));
                if(!trimBranchLog(loBranches.getString("sBranchCD"))){
                    System.out.println("Error trimming the log of " + loBranches.getString("sBranchNm"));
                    System.exit(0);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.exit(0);
        }

        if(!blogonly){
            //regardless of division truncate this tables...
            System.out.println("Truncating MAIN OFFICE ONLY TABLES...");
            for(String x:truncate_all){
                dblocal.executeUpdate("TRUNCATE TABLE " + x);
            }
            
            //Assume that this utility is for 
            //0->MP Division;1->MC Division
            if(System.getProperty("sys.default.division").equalsIgnoreCase("0")){
                System.out.println("Truncating crowded MC Division only tables");
                for(String x:truncate_mc){
                    dblocal.executeUpdate("TRUNCATE TABLE " + x);
                }
                
                try {
                    loBranches.beforeFirst();
                    while(loBranches.next()){
                        System.out.println("Trimming transaction of " + loBranches.getString("sBranchNm"));
                        for(String x:trim_mp_branch){
                            if(!trimBranchTrans(x, "sBranchCd = " + SQLUtil.toSQL(loBranches.getString("sBranchCD")))){
                                System.out.println("Error trimming " + x);
                                System.exit(0);
                            }
                        }
                        
                        for(String x:trim_mp_trans){
                            if(!trimBranchTrans(x, "sTransNox LIKE " + SQLUtil.toSQL(loBranches.getString("sBranchCD")  + "%"))){
                                System.out.println("Error trimming " + x);
                                System.exit(0);
                            }
                        }
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.exit(0);
                }
            }
            else if(System.getProperty("sys.default.division").equalsIgnoreCase("1")){
                System.out.println("Truncating MP Division tables");
                for(String x:truncate_mp){
                    dblocal.executeUpdate("TRUNCATE TABLE " + x);
                }
                
                try {
                    loBranches.beforeFirst();
                    while(loBranches.next()){
                        System.out.println("Trimming transaction of " + loBranches.getString("sBranchNm") + " with branch code " + loBranches.getString("sBranchCD"));
                        for(String x:trim_mc_branch){
                            if(!trimBranchTrans(x, "sBranchCd = " + SQLUtil.toSQL(loBranches.getString("sBranchCD")))){
                                System.out.println("Error trimming " + x);
                                System.exit(0);
                            }
                        }
                        
                        for(String x:trim_mc_trans){
                            if(!trimBranchTrans(x, "sTransNox LIKE " + SQLUtil.toSQL(loBranches.getString("sBranchCD")  + "%"))){
                                System.out.println("Error trimming " + x);
                                System.exit(0);
                            }
                        }
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.exit(0);
                }
            }
            //TODO: what should we do with our database if division is not an MP nor MC?
            else{
                
            }

        }
    }
    
    private static boolean loadConfig(String fsProductID){
        String lnHexCrypt = "";
        String lsDBSrvrNm = "";
        String lsDBUserNm = "";
        String lsDBPassWD = "";
        String lsDBPortNo = "";
        String lsDBNameXX = "";
        String lsClientID = "";
        
        //System.out.println("loadConfig(String fsProductID)");
        //Get configuration values
        try{
            GProperty loProp = new GProperty("GhostRiderXP");

            if(loProp.getConfig(fsProductID + "-CryptType") != null){
                lnHexCrypt = loProp.getConfig(fsProductID + "-CryptType");
            }
            else{
                lnHexCrypt = "0";
            }

            if(psDBSrvrNm == null){
                lsDBSrvrNm = loProp.getConfig(fsProductID + "-ServerName");
            }
            else{
                lsDBSrvrNm = psDBSrvrNm;
            }

            if(loProp.getConfig(fsProductID + "-UserName") != null){
                lsDBUserNm = loProp.getConfig(fsProductID + "-UserName");
                lsDBPassWD = loProp.getConfig(fsProductID + "-Password");
            }

            lsDBPortNo = loProp.getConfig(fsProductID + "-Port");
            lsDBNameXX = loProp.getConfig(fsProductID + "-Database");
            lsClientID = loProp.getConfig(fsProductID + "-ClientID");

            
        }catch(Exception ex){
           ex.printStackTrace();
           return false;
        }

        //Test validity of Results
        if(lsDBSrvrNm.equals("")) {
            return false;
        }

        if (lsDBPortNo == null || lsDBPortNo.equals("")){
            lsDBPortNo = "3306";
        }
        
        System.setProperty("sys.default.crypt", lnHexCrypt);
        System.setProperty("sys.default.dbname", lsDBNameXX);
        System.setProperty("sys.default.dbuser", Decrypt(lsDBUserNm, System.getProperty("sys.default.signature")));
        System.setProperty("sys.default.dbpass", Decrypt(lsDBPassWD, System.getProperty("sys.default.signature")));
        System.setProperty("sys.default.dbsrvr", lsDBSrvrNm);
        System.setProperty("sys.default.dbport", lsDBPortNo);
        System.setProperty("sys.default.clientid", lsClientID);
        
        return true;
    }
    
   private static String Decrypt(String value, String salt) {
      if(value == null || value.trim().length() == 0 || salt == null || salt.trim().length() == 0)
               return null;

      byte[] hex;
      try {
         if(System.getProperty("sys.default.crypt").equals("1")){
            try {
               hex = Hex.decodeHex(value);
            } catch (DecoderException e1) {
               return null;
            }
         }
         else{
            hex = value.getBytes("ISO-8859-1");
         }
         //System.out.println(new String(hex, "ISO-8859-1"));
         //System.out.println(value);
         //remove this part if returning the new logic...
         GCrypt loCrypt = new GCrypt(salt.getBytes("ISO-8859-1"));
         byte ret[] = loCrypt.decrypt(hex);

         return new String(ret, "ISO-8859-1");
      } catch (UnsupportedEncodingException e) {
         e.printStackTrace();
         return null;
      }
   }
   
   public static boolean validHostName(){
        try {
            String lsSQL;
            
            //extract host name of the mysql server
            lsSQL = "SELECT @@hostname  AS srvrname";
            ResultSet loRS = dblocal.executeQuery(lsSQL);
            loRS.next();
            String host = loRS.getString("srvrname");
            
            lsSQL = "SELECT a.sClientID, b.sBranchCD, b.sDBHostNm, b.cDivision" +
                   " FROM xxxSysClient a" +
                      " LEFT JOIN Branch_Others b ON a.sBranchCD = b.sBranchCD" +
                   " WHERE a.sClientID = " + SQLUtil.toSQL(System.getProperty("sys.default.clientid"));
            loRS = dblocal.executeQuery(lsSQL);
            
            if(!loRS.next()){
                System.out.println(System.getProperty("sys.default.clientid") + " was not found from the list of client!");
                return false;
            }
            
            if(!loRS.getString("sDBHostNm").equalsIgnoreCase(host)){
                System.out.println(System.getProperty("sys.default.clientid") + " is not allowed to TRIM this server!");
                return false;
            }

            System.setProperty("sys.default.branchcd", loRS.getString("sBranchCD"));
            System.setProperty("sys.default.division", loRS.getString("cDivision"));
            
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
   }
   
    public static ResultSet getBranches(){
        String lsSQL;
        
        lsSQL = "SELECT sBranchCD, sBranchNm" + 
               " FROM Branch" + 
               " WHERE sBranchCD <> " + SQLUtil.toSQL(System.getProperty("sys.default.branchcd")) + 
               " ORDER BY sBranchCD"; 
        ResultSet loRS = dblocal.executeQuery(lsSQL);
        
        return loRS;
    }
   
    public static boolean trimBranchLog(String sBranchCD){
        try {
            String lsSQL;
            lsSQL = "SELECT sTransNox, sModified" +
                    " FROM xxxReplicationLog" +
                    " WHERE sTransNox LIKE " + SQLUtil.toSQL(sBranchCD + "%") +
                    " AND dModified <= DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH)" +
                    " LIMIT 1";
            ResultSet loRS = dblocal.executeQuery(lsSQL);

            if(!loRS.next()){
                return true;
            }
            
            String start = loRS.getString("sTransNox");

            dblocal.beginTrans();

            while(start.length() > 0){
                lsSQL = "SELECT sTransNox, sModified" +
                        " FROM xxxReplicationLog" +
                        " WHERE sTransNox LIKE " + SQLUtil.toSQL(sBranchCD + "%") +
                        " AND dModified <= DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH)" +
                        " LIMIT 1000, 1";
                loRS = dblocal.executeQuery(lsSQL);
                if(!loRS.next()){
                    start = "";
                    continue;
                }
                
                String stop = loRS.getString("sTransNox");
                
                lsSQL = "DELETE FROM xxxReplicationLog" + 
                       " WHERE sTransNox BETWEEN " + SQLUtil.toSQL(start) + " AND " + SQLUtil.toSQL(stop);
                
                //TODO: for testing only...
                //dblocal.executeUpdate(lsSQL);

                //Display the command that recently deleted the log
                System.out.println(lsSQL);
                
                start = stop;
            }
            
            dblocal.commitTrans();
            
            return true;
        } catch (SQLException ex) {
            dblocal.rollbackTrans();
            ex.printStackTrace();
            return false;
        }
    }
    
    public static boolean trimBranchTrans(String sTableNme, String sFilter){
        String lsSQL;
        lsSQL = "DELETE FROM " + sTableNme +
                " WHERE " + sFilter;

        dblocal.beginTrans();
        
        //TODO: for testing only...
        //dblocal.executeUpdate(lsSQL);
        System.out.println(lsSQL);
        
        dblocal.commitTrans();
        return true;
    }
    
    private static String truncate_all[] = 
        { "2H_JobOrder_Detail", "2H_JobOrder_Labor"
        , "2H_JobOrder_Master", "HotLine_Outgoing"  
        , "JobOrder_Master", "JobOrder_Detail"
        , "Knox_Request_Master", "Knox_Request_Detail"
        , "RaffleEntries"
        , "Employee_Timesheet_Old", "Employee_Log_Old"
        , "TLM_Client", "Hotline_Reminder_Source"
        , "TLM_Call_Master", "TLM_Call_Detail"
        , "Collection_Text_Alert", "LR_Calls_Appointment"
        , "Text_SMS", "SMS_Incoming"
        , "LR_Calls_Master", "LR_Calls_Detail"
        , "Text_Mktg_Detail", "Text_Mktg_Master"};
    
    private static String truncate_mp[] = 
    { "CP_Inventory_Serial_Ledger", "CP_Inventory_Ledger"
    , "CP_SO_Detail", "CP_SO_Master"
    , "CP_SO_Eload", "CP_SO_Accessories"
    , "CP_Transfer_Detail", "CP_Transfer_Master"
    , "CP_Unit_Classification_Detail_Model", "CP_Unit_Classification_Detail"
    , "CP_PO_Receiving_Serial", "CP_PO_Receiving_Master"
    , "CP_PO_Receiving_Detail", "CP_StockInquiry_Detail"
    , "CP_StockInquiry_Master", "CP_Classification_Detail"
    , "CP_Classification_Master", "CP_Load_Matrix_Hist_Ledger"
    , "CP_Serial_Cost", "CP_Load_Matrix_Ledger"
    , "CP_Hist_Ledger", "CP_Stock_Request_Detail"
    , "CP_Stock_Request_Master"};
    
    private static String truncate_mc[] = 
    { "SP_Classification_Detail", "SP_Classification_Master"
    , "SP_Inventory_Ledger", "SP_Hist_Ledger"        
    , "SP_StockInquiry_Detail", "SP_StockInquiry_Master"        
    , "SP_SO_Detail", "SP_SO_Master"        
    , "LR_DCP_Collection_Detail", "LR_DCP_Collection_Master"        
    , "JobOrderBranch_Detail", "JobOrderBranch_Master"        
    , "MC_SO_Detail", "MC_SO_Master"        
    , "MC_SO_GiveAways", "LR_DCP_Request"        
    , "MC_StockInquiry_Detail", "MC_StockInquiry_Master"        
    , "MC_Reg_Requirement_Detail", "MC_Reg_Requirement_Master"        
    , "MC_Classification_Detail", "MC_Classification_Master"        
    , "SP_PO_Receiving_Detail", "SP_PO_Receiving_Master"        
    , "SP_Stock_Request_Detail", "SP_Stock_Request_Master"        
    , "SP_PO_Detail", "SP_PO_Master"        
    , "MC_Inventory_Ledger", "MC_LTO_Pool"        
    , "LR_Collection_Unit_History", "tmpMC_Serial_Transaction"        
    , "Registration_Expense_Detail", "Registration_Expense_Master"        
    , "MC_Registration", "MC_Registration_Expense"        
    , "SP_Transfer_Detail", "SP_Transfer_Master"        
    , "MC_Transfer_Detail", "MC_Transfer_Master"        
    , "MC_Transfer_Accessories", "MC_Serial_Registration"        
    , "MC_Serial_Service", "MC_Serial_Ledger"        
    , "MC_Insurance", "MC_LR_QuickMatch_Result"        
    , "Access_Model_Ledger"        
    , "MC_PO_Receiving_Detail", "MC_PO_Receiving_Master"        
    , "MC_PO_Receiving_Serial", "SP_Price_History"
    , "MC_Reg_Transfer_Detail", "MC_Reg_Transfer_Master"
    , "SP_Retail_Order_Detail", "SP_Retail_Order_Master"
    , "Validation_Expense_Detail", "Validation_Expense_Master"        
    , "MC_Applicant_Means", "MC_LR_QuickMatch"        
    , "G_Card_Transfer_Detail", "G_Card_Transfer_Master"        
    , "MC_Reg_Release_Detail", "MC_Reg_Release_Master"        
    , "MC_AR_Field_Collection_Detail", "MC_AR_Field_Collection_Master"        
    , "MC_Transfer_Accessories"        
    };        

    private static String trim_mp_branch[] = 
    { "CP_Inventory_Ledger", "CP_Hist_Ledger"
    , "CP_Load_Matrix_Ledger", "CP_Load_Matrix_Hist_Ledger"    
    , "CP_Unit_Classification_Detail", "CP_Unit_Classification_Detail_Model"    
    , "CP_Classification_Detail", "CP_Classification_Master"        
    , "Descrepancy", "xxxBinlog_Monitor"
    , "DTR_Summary", "Employee_Log"    
    , "Employee_Log_Old"        
    };
    
    private static String trim_mp_trans[] = 
    { "CP_SO_Detail", "CP_SO_Master"
    , "CP_SO_Eload", "CP_SO_Accessories"
    , "CP_Stock_Request_Detail", "CP_Stock_Request_Master"        
    , "xxxSysUserLogs"        
    };

    private static String trim_mc_branch[] = 
    { "SP_Inventory_Ledger", "SP_Hist_Ledger"
    , "SP_Classification_Detail", "SP_Classification_Master"        
    , "MC_Classification_Detail", "MC_Classification_Master"    
    , "MC_Inventory_Ledger", "Descrepancy"
    , "xxxBinlog_Monitor", "DTR_Summary"
    , "Employee_Log", "Employee_Log_Old"        
    };        
    
    private static String trim_mc_trans[] = 
    { "SP_StockInquiry_Detail", "SP_StockInquiry_Master"
    , "SP_SO_Detail", "SP_SO_Master"        
    , "SalesInvoice", "MC_SO_GiveAways"    
    , "LR_DCP_Collection_Detail", "LR_DCP_Collection_Master"        
    , "JobOrderBranch_Detail", "JobOrderBranch_Master"        
    , "MC_Reg_Requirement_Detail", "MC_Reg_Requirement_Master"        
    , "SP_Retail_Order_Detail", "SP_Retail_Order_Master"
    , "Registration_Expense_Detail", "Registration_Expense_Master"        
    , "Validation_Expense_Detail", "Validation_Expense_Master"        
    , "Employee_Log_Manual_Detail", "Employee_Log_Manual_Master"        
    , "MC_AR_Field_Collection_Detail", "MC_AR_Field_Collection_Master"        
    , "MC_Reg_Release_Detail", "MC_Reg_Release_Detail"        
    , "MC_Transfer_Accessories", "xxxSysUserLog"        
    };        
}
