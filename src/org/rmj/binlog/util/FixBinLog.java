/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.binlog.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.MessagingException;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GDBFChain;
import org.rmj.appdriver.GProperty;
import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.mail.MessageInfo;
import org.rmj.lib.mail.SendMail;

/**
 *
 * FixBinLog --path="" --server="" --mode=show|fix --force=true
 * @author sayso
 */
public class FixBinLog {
    private static GDBFChain slave;
    private static GDBFChain master;
    private static SendMail pomail;
    private static boolean dbFix = true;
    private static boolean dbSkip = false;
    private static String psDBSrvrNm = null;
    public static void main(String[] args) throws SQLException{
        String path;
        
        if(args.length == 1){
            psDBSrvrNm = args[0];
        }
        else if(args.length == 2){
            psDBSrvrNm = args[0];
            dbSkip = args[1].equalsIgnoreCase("true") ? true : false;
        }
        else{
            psDBSrvrNm = "192.168.10.238";
            dbSkip = true;
        }
        
        if(System.getProperty("os.name").toLowerCase().contains("win")){
            path = "D:/GGC_Java_Systems";
        }
        else{
            path = "/srv/GGC_Java_Systems";
        }
        
        System.setProperty("sys.default.path.config", path);
        System.setProperty("sys.default.signature", "08220326");
        
        loadConfig("gRider");
        
        slave = new GDBFChain();
        //slave.doDBFChain("", "", System.getProperty("sys.default.dbsrvr"), System.getProperty("sys.default.dbname"), System.getProperty("sys.default.dbuser"), System.getProperty("sys.default.dbpass"), System.getProperty("sys.default.dbport"));
        slave.doDBFChain("", "", System.getProperty("sys.default.dbsrvr"), System.getProperty("sys.default.dbname"), "root", System.getProperty("sys.default.dbpass"), System.getProperty("sys.default.dbport"));
        
        ResultSet loRS = slave.executeQuery("SHOW SLAVE STATUS");

        if(!loRS.next()){
            System.out.println(slave.getMessage());
            System.exit(0);
        }

        showStatus(loRS);

        if(!dbFix || (loRS.getInt("Last_Errno") == 0 && loRS.getInt("Last_IO_Errno") == 0)){
            System.out.println("No Error");
            System.exit(0);
        }

        int ctr = 0;
        // && ctr <= 40
        while(dbFix){
            int err_no = loRS.getInt("Last_Errno") == 0 ? loRS.getInt("Last_IO_Errno") : loRS.getInt("Last_Errno");
            System.out.println("Error No: " + err_no);
            
            switch(err_no){
                //No error
                case 0:
                    dbFix = false;
                    break;
                //Relay LOG READ failure: Could NOT parse relay LOG event entry 
                case 1594:
                    fixError1594(loRS);
                    break;
                //The SLAVE I/O thread stops because SET @master_heartbeat_period ON MASTER failed.    
                case 1593:
                    fixError1593();
                    break;
                //Error 'You cannot 'ALTER' a log table if logging is enabled' on query. Default database: 'mysql'    
                case 1580:
                    fixError1580();
                    break;
                //Can't find record in '<table>'    
                case 1032:
                //Can't write; duplicate key in table '%s'      
                case 1022:
                //Duplicate entry '%s' for key %d    
                case 1062:
                    if(dbSkip){
                        fixErrorSkip();
                    }
                    else{
                        System.out.println("Error Ctr: " + String.valueOf(ctr)) ; 
                        System.out.println("Log Pos: " + loRS.getString("Exec_Master_Log_Pos")) ; 
                        System.out.println("Fixing: " + loRS.getString("Last_Error"));
                        fixErrorNextRec(loRS);
                    }
                    break;
                default:
                    reportError(loRS);
                    dbFix = false;
            }
            
            loRS = slave.executeQuery("SHOW SLAVE STATUS");
            loRS.next();
            ctr++;
        }
        
        showStatus(loRS);
    }
    
    private static boolean fixError1594(ResultSet rsdata){
        String auto_query;
        try {
            auto_query = "STOP SLAVE";
            auto_query = auto_query + "»" +
                    "CHANGE MASTER TO" +
                    " MASTER_HOST = " + SQLUtil.toSQL(rsdata.getString("Master_Host")) +
                    ", MASTER_USER = 'ggcrepladmin'" +
                    ", MASTER_PASSWORD = 'Tciadsoggc'" +
                    ", MASTER_LOG_FILE = " + SQLUtil.toSQL(rsdata.getString("Relay_Master_Log_File")) +
                    ", MASTER_LOG_POS = " + rsdata.getLong("Exec_Master_Log_Pos");                        
            auto_query = auto_query + "»" + "START SLAVE";
            auto_query = auto_query + "»" + "DO SLEEP(2)";

            return executeSolution(auto_query);
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    private static boolean fixError1593(){
        String auto_query;
        auto_query = "STOP SLAVE";
        auto_query = auto_query + "»" + "START SLAVE";
        auto_query = auto_query + "»" + "DO SLEEP(2)";

        return executeSolution(auto_query);
    }
    
    private static boolean fixError1580(){
        String auto_query;
        auto_query = "STOP SLAVE";
        auto_query = auto_query + "»" + "SET GLOBAL SLOW_QUERY_LOG = 'OFF'";
        auto_query = auto_query + "»" + "START SLAVE";
        auto_query = auto_query + "»" + "DO SLEEP(5)";
        auto_query = auto_query + "»" + "STOP SLAVE";
        auto_query = auto_query + "»" + "SET GLOBAL SLOW_QUERY_LOG = 'ON'";
        auto_query = auto_query + "»" + "START SLAVE";
        auto_query = auto_query + "»" + "DO SLEEP(2)";
        return executeSolution(auto_query);
    }
    
    private static boolean fixErrorSkip(){
        System.out.println("fixErrorSkip()");
        String auto_query;
        auto_query = "STOP SLAVE";
        auto_query = auto_query + "»" + "SET GLOBAL sql_slave_skip_counter = 1";
        auto_query = auto_query + "»" + "START SLAVE";
        auto_query = auto_query + "»" + "DO SLEEP(2)";
        return executeSolution(auto_query);
    }

    private static boolean fixErrorNextRec(ResultSet rsdata){
        String auto_query;
        try {
            auto_query = "STOP SLAVE";

            master = new GDBFChain();
            master.doDBFChain("", "", rsdata.getString("Master_Host"), System.getProperty("sys.default.dbname"), System.getProperty("sys.default.dbuser"), System.getProperty("sys.default.dbpass"), System.getProperty("sys.default.dbport"));
            
            String sql = "SHOW BINLOG EVENTS IN " + SQLUtil.toSQL(rsdata.getString("Relay_Master_Log_File"));
            sql = sql + " FROM " + rsdata.getString("Exec_Master_Log_Pos") + " LIMIT 500";

            ResultSet mstrData = master.executeQuery(sql);
            
            if(!mstrData.next()){
                return false;
            }
            
            //position to the next record
            mstrData.next();
            //if not begin statement data is on the next record
            if(!mstrData.getString("Info").equalsIgnoreCase("begin")){
                mstrData.next();
            }
            
            auto_query = auto_query + "»" + "CHANGE MASTER TO";
            auto_query = auto_query + "  MASTER_LOG_FILE = " + SQLUtil.toSQL(mstrData.getString("Log_Name"));  
            auto_query = auto_query + ", MASTER_LOG_POS = " + mstrData.getString("Pos");  
            
            auto_query = auto_query + "»" + "START SLAVE";
            auto_query = auto_query + "»" + "DO SLEEP(2)";

            return executeSolution(auto_query);
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }
    
    private static boolean executeSolution(String auto_query){
       
       String lasMessage[] = auto_query.split("»");
       for(int ctr=0;ctr<lasMessage.length;ctr++){
          slave.executeUpdate(lasMessage[ctr]);
          if(slave.getMessage().length() > 0){
             System.out.println(slave.getMessage()  + "\nUnable to execute the following: " + auto_query); 
             return false;
          } //if(gchain.getMessage().length() > 0){
       }// for(int ctr=0;ctr<lasMessage.length;ctr++){
       
       return true;  
    }

    private static void reportError(ResultSet loRS){
        pomail = new SendMail(System.getProperty("sys.default.path.config"), "GMail");
        if(pomail.connect(true)){
            try {
                MessageInfo msginfo = new MessageInfo();
                
                //msginfo.addTo(java.net.IDN.toUnicode(new String(encode(email))));
                msginfo.addTo("Support Group <support@guanzongroup.com.ph>");
                msginfo.setSubject("Replication Error for " + System.getProperty("sys.default.clientid") + "!");
                msginfo.setBody(getStatus(loRS));
                msginfo.setFrom("No Reply <no-reply@guanzongroup.com.ph>");
                pomail.sendMessage(msginfo);
            } catch (MessagingException ex) {
                ex.printStackTrace();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
    
    private static boolean showStatus(ResultSet loRS) throws SQLException{
        System.out.println("SLAVE STATUS:");
        System.out.println("=============================================");
        System.out.print(getStatus(loRS));
        System.out.println("=============================================");
        return true;
    }
    
    private static String getStatus(ResultSet loRS){
        try {
            StringBuilder message = new StringBuilder();
            String line = System.getProperty("line.separator");
            message.append("Slave_IO_State        : ").append(loRS.getString("Slave_IO_State")).append(line);
            message.append("Master_Host           : ").append(loRS.getString("Master_Host")).append(line);
            message.append("Master_Log_File       : ").append(loRS.getString("Master_Log_File")).append(line);
            message.append("Read_Master_Log_Pos   : ").append(loRS.getString("Read_Master_Log_Pos")).append(line);
            message.append("Relay_Log_File        : ").append(loRS.getString("Relay_Log_File")).append(line);
            message.append("Relay_Log_Pos         : ").append(loRS.getString("Relay_Log_Pos")).append(line);
            message.append("Relay_Master_Log_File : ").append(loRS.getString("Relay_Master_Log_File")).append(line);
            message.append("Exec_Master_Log_Pos   : ").append(loRS.getString("Exec_Master_Log_Pos")).append(line);
            message.append("Slave_IO_Running      : ").append(loRS.getString("Slave_IO_Running")).append(line);
            message.append("Slave_SQL_Running     : ").append(loRS.getString("Slave_SQL_Running")).append(line);
            message.append("Last_ErrNo            : ").append(loRS.getString("Last_ErrNo")).append(line);
            message.append("Last_Error            : ").append(loRS.getString("Last_Error")).append(line);

            return message.toString();
        } catch (SQLException ex) {
            ex.printStackTrace();
            return "";
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
}
