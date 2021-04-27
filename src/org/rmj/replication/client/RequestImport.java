/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.client;

import com.google.gson.stream.JsonReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Collator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GProperty;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.WebClient;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.appdriver.agent.MsgBox;

/**
 *
 * @author marlon
 */
public class RequestImport {
    private static String GUANZON_SITE = null;     
    private static String SIGNATURE = "08220326";
    private static LogWrapper logwrapr = new LogWrapper("Client.RequestImport", "temp/RequestImport.log");
    private static GRiderX instance = null;    
    private static String exceptnx="";
    
    private static String mailsrvr="";
    private static String domainxx="";
    private static String mailpass="";
    private static String mailuser="";
    private static String mailtoxx="";
    
    public static void main(String[] args) {
        instance = new GRiderX("gRider");

        if(!instance.getErrMsg().isEmpty()){
            MsgBox.showOk(instance.getMessage() + instance.getErrMsg());
            return;
        }        
        
        instance.setOnline(false);
        
        loadConfig();
        
        instance.beginTrans();
        
        if(import_file(args[0])){
            log_import(args[0]);
            System.out.println("Import successfull!");
        }
        
        instance.commitTrans();
        
    }
    
   public static boolean import_file(String filename){
      boolean bErr = false;
      int ctr=0;
      try {     
         JsonReader reader = new JsonReader(new InputStreamReader(new FileInputStream(filename + ".jbr"), "UTF-8"));
         
         //signal initiation of reading of the json file
         reader.beginArray();
         while(reader.hasNext()){
            ctr++;
            //signal initiation of reading of the json object
            reader.beginObject();
            JSONObject json = new JSONObject();
            while(reader.hasNext()){
               String name = reader.nextName();
               if(name.equalsIgnoreCase("sTransNox"))
                  json.put("sTransNox", reader.nextString());
               else if(name.equalsIgnoreCase("sBranchCd"))
                  json.put("sBranchCd", reader.nextString());
               else if(name.equalsIgnoreCase("sStatemnt"))
                  json.put("sStatemnt", reader.nextString());
               else if(name.equalsIgnoreCase("sTableNme"))
                  json.put("sTableNme", reader.nextString());
               else if(name.equalsIgnoreCase("sDestinat"))
                  json.put("sDestinat", reader.nextString());
               else if(name.equalsIgnoreCase("sModified"))
                  json.put("sModified", reader.nextString());     
               else if(name.equalsIgnoreCase("dEntryDte"))
                  json.put("dEntryDte", reader.nextString());     
               else if(name.equalsIgnoreCase("dModified"))
                  json.put("dModified", reader.nextString());     
               else
                  reader.skipValue();
            }
            //signal end of reading of the json object
            reader.endObject();
            
            try{   
                if(!isDuplicate((String) json.get("sTransNox"), (String) json.get("sModified"))){
                    instance.getConnection().createStatement().executeUpdate((String) json.get("sStatemnt"));

                    StringBuilder str = new StringBuilder();

                    str.append("INSERT INTO xxxReplicationLog SET");
                    str.append("  sTransNox = " + SQLUtil.toSQL((String) json.get("sTransNox")));
                    str.append(", sBranchCd = " + SQLUtil.toSQL((String) json.get("sBranchCd")));
                    str.append(", sStatemnt = " + SQLUtil.toSQL((String) json.get("sStatemnt")));
                    str.append(", sTableNme = " + SQLUtil.toSQL((String) json.get("sTableNme")));
                    str.append(", sDestinat = " + SQLUtil.toSQL((String) json.get("sDestinat")));
                    str.append(", sModified = " + SQLUtil.toSQL((String) json.get("sModified")));
                    str.append(", dEntryDte = " + SQLUtil.toSQL(SQLUtil.toDate((String) json.get("dEntryDte"), "MMM d, yyyy K:m:s")));
                    str.append(", dModified = " + SQLUtil.toSQL(instance.getServerDate(instance.getConnection())));
                    
                    instance.getConnection().createStatement().executeUpdate(str.toString());
                }

                if(ctr % 10000 == 0){
                   ctr = 0;
                   instance.commitTrans();
                   instance.beginTrans();
                }

            } catch (SQLException ex) {

                logwrapr.severe("SQLException error detected.", ex);
                logwrapr.info((String) json.get("sStatemnt"));
                //if not duplicate continue:1022x & 1062x, Out range:1690x, 1264x
                //Data too long: 1406x
                System.out.println("Error Code: " + ex.getErrorCode());
                //Please remove 1064x later...
                if(!exceptnx.contains(String.valueOf(ex.getErrorCode()))){
                    if(!sendError(ex, json, filename).equalsIgnoreCase("ignore")){
                        System.out.println("Sent!");
                        bErr = true;
                        break;
                    }                            
                }            
            }
         }
         //signal end of reading the json file
         reader.endArray();
         
      } catch (UnsupportedEncodingException ex) {
            logwrapr.severe("UnsupportedEncodingException error detected.", ex);
            bErr = true;
       } catch (FileNotFoundException ex) {
            logwrapr.severe("FileNotFoundException error detected.", ex);
            bErr = true;
       } catch (IOException ex) {
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
       }
      
      return !bErr;
   }
    
//    private static boolean import_file(String filename){
//        boolean bErr = false;
//        int ctr=0;
//                
//        
//        JSONParser parser = new JSONParser();
//        
//        try {     
//
//        JsonReader reader = new JsonReader(new InputStreamReader(new FileInputStream(filename + ".jbr"), "UTF-8"));
//        reader.beginArray();
//        
//        while (reader.hasNext()) {
//            ctr++; 
//            if(ctr % 10000 == 0){
//               instance.commitTrans();
//               instance.beginTrans();
//            }           
//        }
//        
//        
//        JSONArray a = (JSONArray) parser.parse(new InputStreamReader(new FileInputStream(filename + ".jbr"), "UTF-8"));
//            for (Object o : a){
//               ctr++; 
//               JSONObject json = (JSONObject) o;
//                try{   
//                    if(!isDuplicate((String) json.get("sTransNox"), (String) json.get("sModified"))){
//                        instance.getConnection().createStatement().executeUpdate((String) json.get("sStatemnt"));
//
//                        StringBuilder str = new StringBuilder();
//                        
//                        //System.out.println(SQLUtil.toDate((String) json.get("dEntryDte"), "MMM d, yyyy K:m:s"));
//                        
//                        str.append("INSERT INTO xxxReplicationLog SET");
//                        str.append("  sTransNox = " + SQLUtil.toSQL((String) json.get("sTransNox")));
//                        str.append(", sBranchCd = " + SQLUtil.toSQL((String) json.get("sBranchCd")));
//                        str.append(", sStatemnt = " + SQLUtil.toSQL((String) json.get("sStatemnt")));
//                        str.append(", sTableNme = " + SQLUtil.toSQL((String) json.get("sTableNme")));
//                        str.append(", sDestinat = " + SQLUtil.toSQL((String) json.get("sDestinat")));
//                        str.append(", sModified = " + SQLUtil.toSQL((String) json.get("sModified")));
//                        str.append(", dEntryDte = " + SQLUtil.toSQL((String) json.get("dEntryDte")));
//                        str.append(", dModified = " + SQLUtil.toSQL(instance.getServerDate(instance.getConnection())));
//
////                        str.append(", dEntryDte = " + SQLUtil.toSQL(SQLUtil.toDate((String) json.get("dEntryDte"), "MMM d, yyyy K:m:s")));
//                        
//                        //System.out.println(str.toString());
//                        instance.getConnection().createStatement().executeUpdate(str.toString());
//                    }
//                    
//                    if(ctr % 10000 == 0){
//                       instance.commitTrans();
//                       instance.beginTrans();
//                    }
//                    
//                } catch (SQLException ex) {
//                    
//                    logwrapr.severe("SQLException error detected.", ex);
//                    logwrapr.info((String) json.get("sStatemnt"));
//                    //if not duplicate continue:1022x & 1062x, Out range:1690x, 1264x
//                    //Data too long: 1406x
//                    System.out.println("Error Code: " + ex.getErrorCode());
//                    //Please remove 1064x later...
//                    if(!exceptnx.contains(String.valueOf(ex.getErrorCode()))){
//                        if(!sendError(ex, json, filename).equalsIgnoreCase("ignore")){
//                            System.out.println("Sent!");
//                            bErr = true;
//                            break;
//                        }                            
//                    }
////                    if(!(ex.getErrorCode() == 1022x 
////                      || ex.getErrorCode() == 1064x 
////                      || ex.getErrorCode() == 1062x 
////                      || ex.getErrorCode() == 1690x 
////                      || ex.getErrorCode() == 1264x 
////                      || ex.getErrorCode() == 1406x)){
////                        bErr = true;
////                        break;
////                    }
//                }
//            }            
//        } catch (FileNotFoundException ex) {
//            logwrapr.severe("FileNotFoundException error detected.", ex);
//            bErr = true;
//        } catch (IOException ex) {
//            logwrapr.severe("IOException error detected.", ex);
//            bErr = true;
//        } catch (ParseException ex) {
//            logwrapr.severe("ParseException error detected.", ex);
//            bErr = true;
//        }
//        
//        return !bErr;
//    }
    
    private static boolean import_file_old(String filename){
        boolean bErr = false;
        int ctr=0;
        JSONParser parser = new JSONParser();
        
        try {     

            JSONArray a = (JSONArray) parser.parse(new InputStreamReader(new FileInputStream(filename + ".jbr"), "UTF-8"));
            for (Object o : a){
               ctr++; 
               JSONObject json = (JSONObject) o;
                try{   
                    if(!isDuplicate((String) json.get("sTransNox"), (String) json.get("sModified"))){
                        instance.getConnection().createStatement().executeUpdate((String) json.get("sStatemnt"));

                        StringBuilder str = new StringBuilder();
                        
                        //System.out.println(SQLUtil.toDate((String) json.get("dEntryDte"), "MMM d, yyyy K:m:s"));
                        
                        str.append("INSERT INTO xxxReplicationLog SET");
                        str.append("  sTransNox = " + SQLUtil.toSQL((String) json.get("sTransNox")));
                        str.append(", sBranchCd = " + SQLUtil.toSQL((String) json.get("sBranchCd")));
                        str.append(", sStatemnt = " + SQLUtil.toSQL((String) json.get("sStatemnt")));
                        str.append(", sTableNme = " + SQLUtil.toSQL((String) json.get("sTableNme")));
                        str.append(", sDestinat = " + SQLUtil.toSQL((String) json.get("sDestinat")));
                        str.append(", sModified = " + SQLUtil.toSQL((String) json.get("sModified")));
                        str.append(", dEntryDte = " + SQLUtil.toSQL((String) json.get("dEntryDte")));
                        str.append(", dModified = " + SQLUtil.toSQL(instance.getServerDate(instance.getConnection())));

//                        str.append(", dEntryDte = " + SQLUtil.toSQL(SQLUtil.toDate((String) json.get("dEntryDte"), "MMM d, yyyy K:m:s")));
                        
                        //System.out.println(str.toString());
                        instance.getConnection().createStatement().executeUpdate(str.toString());
                    }
                    
                    if(ctr % 10000 == 0){
                       ctr = 0;
                       instance.commitTrans();
                       instance.beginTrans();
                    }
                    
                } catch (SQLException ex) {
                    
                    logwrapr.severe("SQLException error detected.", ex);
                    logwrapr.info((String) json.get("sStatemnt"));
                    //if not duplicate continue:1022x & 1062x, Out range:1690x, 1264x
                    //Data too long: 1406x
                    System.out.println("Error Code: " + ex.getErrorCode());
                    //Please remove 1064x later...
                    if(!exceptnx.contains(String.valueOf(ex.getErrorCode()))){
                        if(!sendError(ex, json, filename).equalsIgnoreCase("ignore")){
                            System.out.println("Sent!");
                            bErr = true;
                            break;
                        }                            
                    }
//                    if(!(ex.getErrorCode() == 1022x 
//                      || ex.getErrorCode() == 1064x 
//                      || ex.getErrorCode() == 1062x 
//                      || ex.getErrorCode() == 1690x 
//                      || ex.getErrorCode() == 1264x 
//                      || ex.getErrorCode() == 1406x)){
//                        bErr = true;
//                        break;
//                    }
                }
            }            
        } catch (FileNotFoundException ex) {
            logwrapr.severe("FileNotFoundException error detected.", ex);
            bErr = true;
        } catch (IOException ex) {
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        } catch (ParseException ex) {
            logwrapr.severe("ParseException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }

    
    
    private static boolean isDuplicate(String trailno, String by) {
        ResultSet rs = null;
        boolean duplicate = false;
        try {
            String lsSQL = "SELECT *" +
                          " FROM xxxReplicationLog" + 
                          " WHERE sTransNox = " + SQLUtil.toSQL(trailno) + 
                            " AND sModified = " + SQLUtil.toSQL(by);

            rs = instance.getConnection().createStatement().executeQuery(lsSQL);              
            
            duplicate = rs.next();
        } 
        catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
        }
        finally{
            MiscUtil.close(rs);
        }

        return duplicate;
    }    

    private static void loadConfig(){
        GCrypt loEnc = new GCrypt(SIGNATURE);
        GProperty loProp = new GProperty("ReplicaXP");

        exceptnx = loProp.getConfig("ignoreme");
        
        mailuser = loProp.getConfig("watchdog");
        mailpass = loEnc.decrypt(loProp.getConfig("watchpss")); 
        mailtoxx = loProp.getConfig("mailtoxx");
        mailsrvr = loProp.getConfig("mailsrvr");
        domainxx = loProp.getConfig("domainxx");

        String sWebHost[] = loProp.getConfig("websrvcs").split(";");
        for(int x=0;x < sWebHost.length;x++){
           if(WebClient.isHttpURLConOk(sWebHost[x])){
               GUANZON_SITE = sWebHost[x];
               x = sWebHost.length;
           }    
        }          
    }
    
    private static boolean log_import(String filename){
        boolean bErr = false;
        JSONParser parser = new JSONParser();        
        String lsSQL;
        ResultSet loRS;
        String sExportNo;
        try {
            
            JSONArray a = (JSONArray) parser.parse(new InputStreamReader(new FileInputStream(filename + ".jbc"), "UTF-8"));

            for (Object o : a){
                JSONObject json = (JSONObject) o;
                
                lsSQL = "SELECT" +
                        "  IFNull(sExportNo, '') sExportNo" +
                " FROM xxxSysClient" +
                " WHERE sClientID = " + SQLUtil.toSQL((String) json.get("sClientID"));
                
                loRS = instance.getConnection().createStatement().executeQuery(lsSQL);
                
                if(loRS.next()){
                    sExportNo = (String) json.get("sExportTr");
                    
                    if(sExportNo.compareToIgnoreCase(loRS.getString("sExportNo")) > 0){
                        lsSQL = "UPDATE xxxSysClient" + 
                               " SET sExportNo = " + SQLUtil.toSQL(sExportNo) + 
                               " WHERE sClientID = " +  SQLUtil.toSQL((String) json.get("sClientID"));
                        instance.getConnection().createStatement().executeUpdate(lsSQL);
                    }
                }
            }
        } catch (IOException ex) {
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        } catch (ParseException ex) {
            logwrapr.severe("ParseException error detected.", ex);
            bErr = true;
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }
    
    private static String sendError(SQLException ex, JSONObject json, String filename ){
        JSONObject json_obj = new JSONObject();
        
        json_obj.put("sBranchCD", instance.getBranchCode());
        json_obj.put("sFileName", filename);
        json_obj.put("sRefernox", (String)json.get("sTransNox"));                
        json_obj.put("nErrorNox", ex.getErrorCode());
        json_obj.put("sDescript", ex.getMessage());
        
        return report_error(json_obj.toJSONString());
        
    }

    public static String report_error(String json){
        String success = "";
        JSONParser oParser = new JSONParser();
        
        try {
            System.out.println("info:" + json);
            JSONObject json_obj = (JSONObject) oParser.parse(WebClient.httpPostJSon(GUANZON_SITE + "/upload/report_error.php", json));

            System.out.println("Status: " + (String) json_obj.get("sStatusxx"));
            System.out.println("Response: " + (String) json_obj.get("sResponse"));
            System.out.println("Remarks: " + (String) json_obj.get("sRemarksx"));

            if(((String) json_obj.get("sResponse")).length() > 0){
                if(((String) json_obj.get("sResponse")).equalsIgnoreCase("ignore"))
                    success = "ignore";
                else
                    instance.getConnection().createStatement().executeUpdate((String) json_obj.get("sResponse"));
            }
            
            //success = WebClient.httpPostJSon(GUANZON_SITE + "/upload/report_error.php", json);
            //System.out.println("success: " + success);
        } catch (ParseException ex) {
            logwrapr.severe("ParseException error detected.", ex);
        } catch (MalformedURLException ex) {
            logwrapr.severe("MalformedURLException error detected.", ex);
        } catch (IOException ex) {
            logwrapr.severe("IOException error detected.", ex);
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
        }
        return success;
    }


            
    
}
