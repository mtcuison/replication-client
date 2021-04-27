//Use in importing downloaded data to the local server
package org.rmj.replication.client;

import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.Socket;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GProperty;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.MiscReplUtil;
import org.rmj.lib.net.SFTP_DU;
import org.rmj.lib.net.WebClient;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.appdriver.agent.MsgBox;

//kalyptus - 2016.07.05 03:59pm
//Note: Replace import_file with that of the RequestImport to enable this utility
//      to import extremely large json files.

public class Import {
    private static String SIGNATURE = "08220326";
    private static String GUANZON_SITE = null; 
    private static LogWrapper logwrapr = new LogWrapper("Client.Import", "temp/Import.log");
    private static GRiderX instance = null;       
    private static String mailsrvr="";
    private static String domainxx="";
    private static String mailpass="";
    private static String mailuser="";
    private static String mailtoxx="";
    private static String exceptnx="";
    private  Integer id;
    private static String readtype="";
    
    public static void main(String[] args) {
        ImportServer sds = null;  
        try {
            Socket clientSocket = new Socket("localhost", ImportServer.PORT);
            System.out.println("*** Already running!");
            System.exit(1);
        }
        catch (Exception e) {
            sds = new ImportServer();
            sds.start();
        }
        String path;
        if(System.getProperty("os.name").toLowerCase().contains("win")){
            path = "C:/GGC_Java_Systems";
        }
        else{
            path = "/srv/GGC_Java_Systems";
        }
        
        System.setProperty("sys.default.path.config", path);
        
        instance = new GRiderX("gRider");
        
        if(!instance.getErrMsg().isEmpty()){
            MsgBox.showOk(instance.getMessage() + instance.getErrMsg());
            sds.interrupt();
            System.exit(1);
        }

        instance.setOnline(false);
        
        loadConfig();
        
        Boolean bErr;
        ResultSet rs = extract_record();
        if(rs != null){
            try {
                //import all files that are imported from the appserver by the Upload object
               while(rs.next()){
                   //untar file 
                   if(!untar_json(rs.getString("sFileName"))) break;
                   
                   instance.beginTrans();
                   
                   //import file content to local database...
                   System.out.println(rs.getString("sFileName"));
                   if(!import_file(rs.getString("sFileName"))) break;
                    
                   //update local database that file was already imported...
                   if(!post_success(rs.getString("sFileName"))) break;
                   
                   instance.commitTrans();
               }
            } catch (SQLException ex) {
                logwrapr.severe("main: SQLException error detected.", ex);
            }
        }

        sds.interrupt();
        System.exit(0);
    }
    
    private static boolean post_success(String filename){
        boolean bErr = false;
        
        try {
            String lsSQL = "UPDATE xxxIncomingLog" +
                          " SET dImported = " + SQLUtil.toSQL(instance.getServerDate()) +  
                             ", cTranStat = '2'" +
                          " WHERE sFileName = " + SQLUtil.toSQL(filename);   

            instance.getConnection().createStatement().executeUpdate(lsSQL);  
            
            File file = new File(instance.getApplPath() + "/repl/download/unzipped/" + filename + ".json");
            if(file.exists()) {
                file.delete();
            }               
        } 
        catch (SQLException ex) {
            logwrapr.severe("post_success: SQLException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }
    
    private static ResultSet extract_record(){
        ResultSet rs = null;
        try {
            String lsSQL = "SELECT sFileName" +
                          " FROM xxxIncomingLog" + 
                          " WHERE cTranStat = '1'" + 
                          " ORDER BY dReceived ASC";
            rs = instance.getConnection().createStatement().executeQuery(lsSQL);              
            
        } 
        catch (SQLException ex) {
            logwrapr.severe("extract_record: IOException error detected.", ex);
        }
        
        return rs;
    }    

   public static boolean import_file(String filename){
       if(readtype == null || readtype.isEmpty() || readtype.equalsIgnoreCase("0")){
           return import_file2(filename);
       }
       else{
           return import_file1(filename);
       }
   }  
    
   private static boolean import_file1(String filename){
      boolean bErr = false;
      int ctr=0;
      int lastcount=0;
      
        //kalyptus - 2017.09.05 05:04pm
        //use a property object to save location of the counter...      
        GProperty prop= new GProperty();
        File file = new File("temp/" + filename + ".properties");
        if (file.exists()) {
            prop = new GProperty("temp/" + filename);
            lastcount=Integer.parseInt(prop.getConfig("lastcount"));
        }
        else{
            prop.setConfig("lastcount", String.valueOf(0));
            prop.save("temp/" + filename);
        }
      
      file = null;
      
      try {     
         JsonReader reader = new JsonReader(new InputStreamReader(new FileInputStream(instance.getApplPath() + "/repl/download/unzipped/" + filename + ".json"), "UTF-8"));
         
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
            
            if(lastcount < ctr){
               try{   
                   if(!isDuplicate((String) json.get("sTransNox"), (String) json.get("sModified"))){
                       StringBuilder str = new StringBuilder();

                       str.append("INSERT INTO xxxReplicationLog SET");
                       str.append("  sTransNox = " + SQLUtil.toSQL((String) json.get("sTransNox")));
                       str.append(", sBranchCd = " + SQLUtil.toSQL((String) json.get("sBranchCd")));
                       str.append(", sStatemnt = " + SQLUtil.toSQL((String) json.get("sStatemnt")));
                       str.append(", sTableNme = " + SQLUtil.toSQL((String) json.get("sTableNme")));
                       str.append(", sDestinat = " + SQLUtil.toSQL((String) json.get("sDestinat")));
                       str.append(", sModified = " + SQLUtil.toSQL((String) json.get("sModified")));
                       str.append(", dEntryDte = " + SQLUtil.toSQL(SQLUtil.toDate((String) json.get("dEntryDte"), "yyyy-MM-dd HH:mm:ss")));
                       str.append(", dModified = " + SQLUtil.toSQL(instance.getServerDate(instance.getConnection())));
                       instance.getConnection().createStatement().executeUpdate(str.toString());
                       instance.getConnection().createStatement().executeUpdate((String) json.get("sStatemnt"));

                       str=null;
                   }

                   if(ctr % 1000 == 0){
                      instance.commitTrans();
                      instance.beginTrans();
                      lastcount=ctr;
                      prop.setConfig("lastcount", String.valueOf(lastcount));
                      prop.save("temp/" + filename);
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
            
            json=null;
            
         }
         //signal end of reading the json file
         reader.endArray();
         reader=null;
         
         file = new File("temp/" + filename + ".properties");
         file.delete();
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

    private static boolean import_file2(String filename){
      boolean bErr = false;
      int ctr=0;
      try {     
         JsonReader reader = new JsonReader(new InputStreamReader(new FileInputStream(instance.getApplPath() + "/repl/download/unzipped/" + filename + ".json"), "UTF-8"));
         
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
                    //str.append(", dEntryDte = " + SQLUtil.toSQL(SQLUtil.toDate((String) json.get("dEntryDte"), "MMM d, yyyy K:m:s")));
                    str.append(", dEntryDte = " + SQLUtil.toSQL(SQLUtil.toDate((String) json.get("dEntryDte"), "yyyy-MM-dd HH:mm:ss")));
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
   
//    private static boolean import_file2(String filename){
//        boolean bErr = false;
//        
//        JSONParser parser = new JSONParser();
//        
//        try {     
//
////            JSONArray a = (JSONArray) parser.parse(new FileReader(instance.getApplPath() + "/repl/download/unzipped/" + filename + ".json"));
//            JSONArray a = (JSONArray) parser.parse(new InputStreamReader(new FileInputStream(instance.getApplPath() + "/repl/download/unzipped/" + filename + ".json"), "UTF-8"));
//            
//            //Create statement
//            Statement stmt = instance.getConnection().createStatement();
//            StringBuilder str = new StringBuilder();
//                
//            for (Object o : a){
//                JSONObject json = (JSONObject) o;
//                try{   
//                    if(!isDuplicate((String) json.get("sTransNox"), (String) json.get("sModified"))){
//                        //instance.getConnection().createStatement().executeUpdate((String) json.get("sStatemnt"));
//                        stmt.executeUpdate((String) json.get("sStatemnt"));
//
//                        str = new StringBuilder();
//                        //System.out.println(SQLUtil.toDate((String) json.get("dEntryDte"), "MMM d, yyyy K:m:s"));
//                        
//                        str.append("INSERT INTO xxxReplicationLog SET");
//                        str.append("  sTransNox = " + SQLUtil.toSQL((String) json.get("sTransNox")));
//                        str.append(", sBranchCd = " + SQLUtil.toSQL((String) json.get("sBranchCd")));
//                        str.append(", sStatemnt = " + SQLUtil.toSQL((String) json.get("sStatemnt")));
//                        str.append(", sTableNme = " + SQLUtil.toSQL((String) json.get("sTableNme")));
//                        str.append(", sDestinat = " + SQLUtil.toSQL((String) json.get("sDestinat")));
//                        str.append(", sModified = " + SQLUtil.toSQL((String) json.get("sModified")));
//                        //str.append(", dEntryDte = " + SQLUtil.toSQL(SQLUtil.toDate((String) json.get("dEntryDte"), "MMM d, yyyy K:m:s")));
//                        str.append(", dEntryDte = " + SQLUtil.toSQL(SQLUtil.toDate((String) json.get("dEntryDte"), "yyyy-MM-dd HH:mm:ss")));
//                        str.append(", dModified = " + SQLUtil.toSQL(instance.getServerDate(instance.getConnection())));
//                        
//                        //System.out.println(str.toString());
//                        //instance.getConnection().createStatement().executeUpdate(str.toString());
//                        stmt.executeUpdate(str.toString());
//                        
//                        str = null;
//                    }
//                    
//                    json = null;
//                } catch (SQLException ex) {
//                    
//                    logwrapr.severe("import_file: SQLException error detected.", ex);
//                    logwrapr.info("import_file: " + (String) json.get("sStatemnt"));
//                    //if not duplicate continue:1022x & 1062x, Out range:1690x, 1264x
//                    //Data too long: 1406x
//                    System.out.println("Error Code: " + ex.getErrorCode());
//                    //Please remove 1064x later...
//                    if(!exceptnx.contains(String.valueOf(ex.getErrorCode()))){
//                        System.out.println("Sending Report...");
//                        //sendMail(ex, (String) json.get("sTransNox") + "»" + (String) json.get("sStatemnt"));
//                        
//                        if(!sendError(ex, json, filename).equalsIgnoreCase("ignore")){
//                            System.out.println("Sent!");
//                            bErr = true;
//                            break;
//                        }    
//                    }
//                }
//            }
//            
//            a = null;
//            stmt = null;
//            
//        } catch (FileNotFoundException ex) {
//            logwrapr.severe("import_file: FileNotFoundException error detected.", ex);
//            bErr = true;
//        } catch (IOException ex) {
//            logwrapr.severe("import_file: IOException error detected.", ex);
//            bErr = true;
//        } catch (ParseException ex) {
//            logwrapr.severe("import_file: ParseException error detected.", ex);
//            bErr = true;
//        } catch (SQLException ex) {
//            logwrapr.severe("import_file: SQLException error detected.", ex);
//            bErr = true;
//        }
//        
//        parser = null;
//        return !bErr;
//    }
    
    private static boolean untar_json(String filename){
        boolean bErr = false;
        
        try {
            MiscReplUtil.untar(instance.getApplPath() + "/repl/download/zipped/" + filename + ".json.tar.gz", instance.getApplPath() + "/repl/download/unzipped/");
        } catch (IOException ex) {
            logwrapr.severe("untar_json: IOException error detected.", ex);
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
            logwrapr.severe("isDuplicate: SQLException error detected.", ex);
        }
        finally{
            MiscUtil.close(rs);
            rs = null;
        }

        return duplicate;
    }
    
    private static void loadConfig(){
        GCrypt loEnc = new GCrypt(SIGNATURE);
        GProperty loProp = new GProperty("ReplicaXP");

        mailuser = loProp.getConfig("watchdog");
        mailpass = loEnc.decrypt(loProp.getConfig("watchpss")); 
        mailtoxx = loProp.getConfig("mailtoxx");
        exceptnx = loProp.getConfig("ignoreme");
        mailsrvr = loProp.getConfig("mailsrvr");
        domainxx = loProp.getConfig("domainxx");
        readtype = loProp.getConfig("readtype");
        
        String sWebHost[] = loProp.getConfig("websrvcs").split(";");
        for(int x=0;x < sWebHost.length;x++){
           if(WebClient.isHttpURLConOk(sWebHost[x])){
               GUANZON_SITE = sWebHost[x];
               x = sWebHost.length;
           }    
        }        
    }

    private static void sendMail(SQLException ex, String lsSQL) {
        try {
            StringBuilder lsMessage= new StringBuilder();   
            Email email = new SimpleEmail();
            email.setSmtpPort(587);
            email.setAuthenticator(new DefaultAuthenticator(mailuser, mailpass)); 
            email.setDebug(false);
            email.setHostName(mailsrvr);
            email.setFrom(mailuser + (domainxx.trim().length() > 0 ? "@" + domainxx : ""));
            email.setSubject("Error in Import: " + instance.getBranchName());
            lsMessage.append("Error No.: " + String.valueOf(ex.getErrorCode()));
            lsMessage.append("\r\n");
            lsMessage.append(ex.getMessage());
            lsMessage.append("\r\n");
            lsMessage.append(lsSQL);
            email.setMsg(lsMessage.toString());
            email.addTo(mailtoxx);
            //email.setTLS(true);
            email.setSSLOnConnect(true);
            System.out.println(email.getFromAddress().toString());
            email.send();
            System.out.println("Mail sent!");
        } catch (EmailException ex1) {
        }        
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
            logwrapr.severe("report_error: ParseException error detected.", ex);
        } catch (MalformedURLException ex) {
            logwrapr.severe("report_error: MalformedURLException error detected.", ex);
        } catch (IOException ex) {
            logwrapr.severe("report_error: IOException error detected.", ex);
        } catch (SQLException ex) {
            logwrapr.severe("report_error: SQLException error detected.", ex);
        }
        return success;
    }

//    @Override
//    public int hashCode() {
//       return id.hashCode();
//    }    
//    
//    @Override
//    public boolean equals(Object o) {
//       boolean response = false;
//       if (o instanceof Import) {
//          response = (((Import)o).id).equals(this.id);
//       }
//   return response;
//}

}
