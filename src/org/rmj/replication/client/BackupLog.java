/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.client;

import com.google.gson.stream.JsonWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import org.rmj.lib.net.LogWrapper;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.appdriver.agent.MsgBox;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.net.MiscReplUtil;

/**
 * @author kalyptus
 */
public class BackupLog{
   private static LogWrapper logwrapr = new LogWrapper("Client.BackupLog", "temp/BackupLog.log");
   private static GRiderX instance = null;

   private static String file_from = null;
   private static String file_thru = null;
   private static String log_from = null;
   private static String log_thru = null;
   
   public static void main(String[] args) {
      BackupLogServer sds = null;  
      ResultSet rs = null;
        
//      System.out.println(System.getProperty("user.home"));
//      System.exit(1);
      
      try {
         Socket clientSocket = new Socket("localhost", BackupLogServer.PORT);
         System.out.println("*** Already running!");
         System.exit(1);
      }
      catch (Exception e) {
         sds = new BackupLogServer();
         sds.start();
      }
      
      instance = new GRiderX("gRider");
      if(!instance.getErrMsg().isEmpty()){
         MsgBox.showOk(instance.getMessage() + instance.getErrMsg());
         sds.interrupt();
         System.exit(1);
      }
      instance.setOnline(false);
      
      if(!getStartLog()){
         sds.interrupt();
         System.exit(1);
      }

      if(!getStopLog()){
         sds.interrupt();
         System.exit(1);
      }
      
      rs = extract_record();
      
      if(rs == null){
         sds.interrupt();
         System.exit(1);
      }
      
      try {
         if(rs.next()){
            String filename = instance.getBranchCode() + MiscReplUtil.format(Calendar.getInstance().getTime(), "yyyyMMddHHmmss");
            
            System.out.println("writing : " + filename);
            if(!write_file(rs, filename)){
               sds.interrupt();
               System.exit(1);
            }
            
            System.out.println("tarring : " + filename);
            if(!tar_json(filename)){
               sds.interrupt();
               System.exit(1);
            }
            
            System.out.println("deleting : " + filename);
            if(!delete(instance.getApplPath() + "/backup/log/unzipped/" + filename + ".json")){
               sds.interrupt();
               System.exit(1);
            }
            
            String lsSQL;
            lsSQL = "UPDATE xxxOutgoingLog" + 
                   " SET cTranStat = '4'" + 
                   " WHERE sFileName BETWEEN " + SQLUtil.toSQL(file_from) + " AND " + SQLUtil.toSQL(file_thru) + 
                     " AND cTranStat = '2'";
            instance.getConnection().createStatement().executeUpdate(lsSQL);
            
         }
      } catch (SQLException ex) {
         logwrapr.severe("main: SQLException error detected.", ex);
      }  
      
        sds.interrupt();
        System.exit(0);
   }

   private static boolean getStartLog(){
        boolean bErr = false;
        
        try {
            String lsSQL = "SELECT sFileName, sLogFromx, sLogThrux, dExported" +
                          " FROM xxxOutgoingLog" + 
                          " WHERE sFileName LIKE " + SQLUtil.toSQL(instance.getBranchCode() + "%") + 
                            " AND cTranStat = '4'" + 
                          " ORDER BY sFileName DESC LIMIT 1";  
            ResultSet rs = instance.getConnection().createStatement().executeQuery(lsSQL);
            System.out.println(lsSQL);
            if(rs.next()){
               file_from = rs.getString("sFileName");
               log_from = rs.getString("sLogThrux");
            }
            else
            {
//               //if just a start then set the first transaction for the year as the start...
//               lsSQL = "SELECT sFileName, sLogFromx, sLogThrux, dExported" +
//                      " FROM xxxOutgoingLog" + 
//                      " WHERE sFileName LIKE " + SQLUtil.toSQL(instance.getBranchCode() + MiscReplUtil.format(Calendar.getInstance().getTime(), "yyyy") + "%") + 
//                        " AND cTranStat = '2'" + 
//                      " ORDER BY sFileName ASC LIMIT 1";  
//               rs = instance.getConnection().createStatement().executeQuery(lsSQL);
//               if(rs.next()){
//                  file_from = rs.getString("sFileName");
//                  log_from = rs.getString("sLogThrux");
//               }
//               else{
                  file_from = instance.getBranchCode();
                  log_from = instance.getBranchCode();
//               }
            }
        } 
        catch (SQLException ex) {
            logwrapr.severe("getStartLog: SQLException error detected.", ex);
            bErr = true;
        }
        return !bErr;
   }

   private static boolean getStopLog(){
        boolean bErr = false;
        
        try {
            String lsSQL = "SELECT sFileName, sLogFromx, sLogThrux, dExported" +
                          " FROM xxxOutgoingLog" + 
                          " WHERE sFileName LIKE " + SQLUtil.toSQL(instance.getBranchCode() + "%") + 
                            " AND cTranStat = '2'" + 
                            " AND dExported < CURRENT_DATE()" + 
                          " ORDER BY sFileName DESC LIMIT 1";  
            ResultSet rs = instance.getConnection().createStatement().executeQuery(lsSQL);
            System.out.println(lsSQL);
            if(rs.next()){
               file_thru = rs.getString("sFileName");
               log_thru = rs.getString("sLogThrux");
            }
            else{
               file_thru = file_from;
               log_thru = log_from;
            }
        } 
        catch (SQLException ex) {
            logwrapr.severe("getStopLog: SQLException error detected.", ex);
            bErr = true;
        }
        return !bErr;
   }
   
    private static ResultSet extract_record(){
        ResultSet rs = null;
        
        try {
            String lsSQL = "SELECT *" +
                          " FROM xxxReplicationLog" + 
                          " WHERE sTransNox LIKE " + SQLUtil.toSQL(instance.getBranchCode() + "%") + 
                            " AND sTransNox BETWEEN " + SQLUtil.toSQL(log_from) + " AND " + SQLUtil.toSQL(log_thru);
            System.out.println(lsSQL);
            rs = instance.getConnection().createStatement().executeQuery(lsSQL);
            System.out.println("extract_record done...");
        } 
        catch (SQLException ex) {
            logwrapr.severe("extract_record: IOException error detected.", ex);
            rs = null;
        }
        
        return rs;
    }
   
    private static boolean write_file(ResultSet rs, String filename){
        boolean bErr = false;
        try {
           
            String destpath = instance.getApplPath() + "/backup/log/unzipped/";
            File destpth = new File(destpath);
            if (!destpth.exists()) {
                System.out.println("I am here");
                destpth.mkdirs();
            } 
           
            OutputStream out = new FileOutputStream(instance.getApplPath() + "/backup/log/unzipped/" + filename + ".json");
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));

            LinkedList<Map<String, String>> link = MiscReplUtil.RStoLinkList(rs);
            
            writer.beginArray();
            
            //SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat sf = new SimpleDateFormat("MMM d, yyyy K:m:s");
            
            for(Map map : link){
               writer.beginObject();
               writer.name("sTransNox").value((String)map.get("sTransNox"));
               writer.name("sBranchCd").value((String)map.get("sBranchCd"));
               writer.name("sStatemnt").value((String)map.get("sStatemnt"));
               writer.name("sTableNme").value((String)map.get("sTableNme"));
               writer.name("sDestinat").value((String)map.get("sDestinat"));
               writer.name("sModified").value((String)map.get("sModified"));
               writer.name("dEntryDte").value(sf.format((Date)map.get("dEntryDte")));
               writer.name("dModified").value(sf.format((Date)map.get("dModified")));
               writer.endObject();
            }
            
            writer.endArray();
            writer.close();
            
        } catch(IOException ex){
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }    

    private static boolean tar_json(String filename){
        boolean bErr = false;
        
        try {
            MiscReplUtil.tar(instance.getApplPath() + "/backup/log/unzipped/" + filename + ".json", instance.getApplPath() + "/backup/log/zipped/");
        } catch (IOException ex) {
            logwrapr.severe("tar_json: IOException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }    
    
   private static boolean delete(String fullpath){
      try{
          File file = new File(fullpath);

          if(file.delete()){
            return true;
          }else{
            return false;
          }
    	}catch(Exception ex){
            logwrapr.severe("delete: IOException error detected.", ex);
            return false;
    	}       
    }
}

//#Extract last BACKUP
//SELECT sFileName, sLogFromx, sLogThrux, dExported 
//FROM xxxOutgoingLog 
//WHERE sFileName LIKE 'M076%' 
//  AND cTranStat = '4' 
//ORDER BY sFileName DESC
//LIMIT 1;
//
//#Extract last ITEM TO BACKUP
//SELECT sFileName, sLogFromx, sLogThrux, dExported 
//FROM xxxOutgoingLog 
//WHERE sFileName LIKE 'M076%' 
//  AND cTranStat = '2' 
//  AND dExported < CURRENT_DATE()
//ORDER BY sFileName DESC
//LIMIT 1;
//
//#Update CURRENTLY BACKUP logs...
//UPDATE xxxOutgoingLog 
//SET cTranStat = '4' 
//WHERE sFileName BETWEEN 'M07620170102090005' AND 'M07620170612171505'

