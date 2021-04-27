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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.MiscReplUtil;
import org.rmj.appdriver.agent.GRiderX;

/**
 *
 * @author kalyptus
 */
public class BackUp{
   private LogWrapper logwrapr = new LogWrapper("Client.BackupLog", "temp/BackupLog.log");
   private GRiderX instance = null;

   private String file_from = null;
   private String file_thru = null;
   private String log_from = null;
   private String log_thru = null;   
   
   public BackUp(GRiderX foGRider){
      this.instance = foGRider;
      if(foGRider != null){
         file_from = null;
         file_thru = null;
         log_from = null;
         log_thru = null;
         logwrapr = new LogWrapper("Client.BackUp", "temp/BackUp.log");
      }
   }   

   public boolean doBackup(String sBranchCD){
      ResultSet rs = null;
      
      if(!getStartLog(sBranchCD)){
         return false;
      }      
   
      if(!getStopLog(sBranchCD)){
         return false;
      }      
      
       rs = extract_record(sBranchCD);
      
      if(rs == null){
         return false;
      }
     
      try {
         if(rs.next()){
            String filename = sBranchCD + MiscReplUtil.format(Calendar.getInstance().getTime(), "yyyyMMddHHmmss");
            
            System.out.println("writing : " + filename);
            if(!write_file(rs, filename)){
               return false;
            }
            
            System.out.println("tarring : " + filename);
            if(!tar_json(filename)){
               return false;
            }
            
            System.out.println("deleting : " + filename);
            if(!delete(instance.getApplPath() + "/backup/log/unzipped/" + filename + ".json")){
               return false;
            }
            
            String lsSQL;
            lsSQL = "UPDATE xxxOutgoingLog" + 
                   " SET cTranStat = '4'" + 
                   " WHERE sFileName BETWEEN " + SQLUtil.toSQL(file_from) + " AND " + SQLUtil.toSQL(file_thru) + 
                     " AND cTranStat = '2'";
            instance.getConnection().createStatement().executeUpdate(lsSQL);
            
            //MiscReplUtil.fileWrite(instance.getApplPath() + "/backup.me", MiscReplUtil.format(instance.getServerDate(), "yyyyMMddHHmmss"));
         }
      } catch (SQLException ex) {
         logwrapr.severe("doBackup: SQLException error detected.", ex);
         return false;
      }  

      return true;
   }
   
   private boolean getStartLog(String sBranchCD){
        boolean bErr = false;
        
        try {
            String lsSQL = "SELECT sFileName, sLogFromx, sLogThrux, dExported" +
                          " FROM xxxOutgoingLog" + 
                          " WHERE sFileName LIKE " + SQLUtil.toSQL(sBranchCD + "%") + 
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
               file_from = sBranchCD;
               log_from = sBranchCD;
            }
        } 
        catch (SQLException ex) {
            logwrapr.severe("getStartLog: SQLException error detected.", ex);
            bErr = true;
        }
        return !bErr;
   }

   private boolean getStopLog(String sBranchCD){
        boolean bErr = false;
        
        try {
            String lsSQL = "SELECT sFileName, sLogFromx, sLogThrux, dExported" +
                          " FROM xxxOutgoingLog" + 
                          " WHERE sFileName LIKE " + SQLUtil.toSQL(sBranchCD + "%") + 
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
   
    private ResultSet extract_record(String sBranchCD){
        ResultSet rs = null;
        
        try {
            String lsSQL = "SELECT *" +
                          " FROM xxxReplicationLog" + 
                          " WHERE sTransNox LIKE " + SQLUtil.toSQL(sBranchCD + "%") + 
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
   
    private boolean write_file(ResultSet rs, String filename){
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

            //LinkedList<Map<String, String>> link = MiscReplUtil.RStoLinkList(rs);
            
            writer.beginArray();
            
            //SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat sf = new SimpleDateFormat("MMM d, yyyy K:m:s");
            
            while(rs.next()){
               writer.beginObject();
               writer.name("sTransNox").value(rs.getString("sTransNox"));
               writer.name("sBranchCd").value(rs.getString("sBranchCd"));
               writer.name("sStatemnt").value(rs.getString("sStatemnt"));
               writer.name("sTableNme").value(rs.getString("sTableNme"));
               writer.name("sDestinat").value(rs.getString("sDestinat"));
               writer.name("sModified").value(rs.getString("sModified"));
               writer.name("dEntryDte").value(sf.format(rs.getDate("dEntryDte")));
               writer.name("dModified").value(sf.format(rs.getDate("dModified")));
               writer.endObject();
            }
            
//            for(Map map : link){
//               writer.beginObject();
//               writer.name("sTransNox").value((String)map.get("sTransNox"));
//               writer.name("sBranchCd").value((String)map.get("sBranchCd"));
//               writer.name("sStatemnt").value((String)map.get("sStatemnt"));
//               writer.name("sTableNme").value((String)map.get("sTableNme"));
//               writer.name("sDestinat").value((String)map.get("sDestinat"));
//               writer.name("sModified").value((String)map.get("sModified"));
//               writer.name("dEntryDte").value(sf.format((Date)map.get("dEntryDte")));
//               writer.name("dModified").value(sf.format((Date)map.get("dModified")));
//               writer.endObject();
//            }
            
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

    private boolean tar_json(String filename){
        boolean bErr = false;
        
        try {
            MiscReplUtil.tar(instance.getApplPath() + "/backup/log/unzipped/" + filename + ".json", instance.getApplPath() + "/backup/log/zipped/");
        } catch (IOException ex) {
            logwrapr.severe("tar_json: IOException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }    
    
   private boolean delete(String fullpath){
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
