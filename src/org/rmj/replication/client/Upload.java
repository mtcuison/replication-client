//Use in uploading data to the app server

//upload_info/(json) => inform appserver about file to be uploaded to it
    //server side=>create a record in the database that a file with an info as specified in the json
    //returns success if successfully registered
//upload_success/(json) => inform that file was successfully uploaded
    //server side->tag the record in the database that the file was successfully uploaded
    //returns success if file was uploaded successfully
//upload_backlog/(branch_code) => inquire about files for upload of a branch.... 
    //server side->searches the database about list of files tagged as for upload 
    // returns json of not yet uploaded file
//upload_hash/(filename) => inquire about md5hash of uploaded file
    //server side->performs a md5hashing of the file being inquired
    //return the md5hash of the file

//Status: testing

package org.rmj.replication.client;

import com.google.gson.stream.JsonWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.Socket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
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
import org.rmj.lib.net.MiscReplUtil;
import static org.rmj.lib.net.MiscReplUtil.RStoLinkList;
import org.rmj.lib.net.SFTP_DU;
import org.rmj.lib.net.WebClient;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.appdriver.agent.MsgBox;

public class Upload {
    private static String SIGNATURE = "08220326";
    private static String GUANZON_SITE = null; 
    private static LogWrapper logwrapr = new LogWrapper("Client.Upload", "temp/Upload.log");
    private static String host_dir = null;
    private static SFTP_DU sftp;
    private static GRiderX instance = null;
    private static JSONObject json_obj = null;
    private static Properties prop = new Properties();
    
    public static void main(String[] args){
        UploadServer sds = null;  
        
        try {
            Socket clientSocket = new Socket("localhost", UploadServer.PORT);
            System.out.println("*** Already running!");
            System.exit(1);
        }
        catch (Exception e) {
            sds = new UploadServer();
            sds.start();
        }
        
        instance = new GRiderX("gRider");

        if(!instance.getErrMsg().isEmpty()){
            MsgBox.showOk(instance.getMessage() + instance.getErrMsg());
            sds.interrupt();
            System.exit(1);
        }
        
        instance.setOnline(false);

        //load FTP information
        prepareSFTPHost();

        ResultSet loRSB = extract_branch();
        try{
            while(loRSB.next()){
                doBackup(loRSB.getString("sBranchCD"));

                //get last file info for export local server => filename, md5hash, status
                json_obj = new JSONObject();
                json_obj.put("sFileName", "X");

                if(!upload_backlog(loRSB.getString("sBranchCD"))){
                    sftp.xDisconnect();
                    sds.interrupt();
                    System.exit(1);
                }

                //if(status = 0) 
                System.out.println("Record Stat: " + (String) json_obj.get("cTranStat"));
                if(((String) json_obj.get("cTranStat")).equals("1") && !((String) json_obj.get("sFileName")).equals("")){
                    //Upload files
                    if(!upload_file((String) json_obj.get("sFileName"))){ 
                        sftp.xDisconnect();
                        sds.interrupt();
                        System.exit(1);
                    }

                    //(*)Get MD5 of the uploaded files from the app server
                        //server should create the MD5 of the uploaded file and send it to the client
                    String sMD5Hash = upload_hash((String) json_obj.get("sFileName"));

                    System.out.println("Server Hash: " + sMD5Hash);
                    System.out.println("Localx Hash: " + (String) json_obj.get("sMD5Hashx"));

                    //if(MD5 of upload files is equals to previously get MD5)
                    if(((String) json_obj.get("sMD5Hashx")).equalsIgnoreCase(sMD5Hash)){

                        System.out.println("Same Hash");

                        //(*)inform app server that upload of file is successful...
                        if(upload_success(json_obj.toJSONString()).equalsIgnoreCase("success")){
                            //update local database that file was already exported...
                            if(!post_success((String) json_obj.get("sFileName"))) {
                                sftp.xDisconnect();
                                sds.interrupt();
                                System.exit(1);
                            }
                        }else{
                            sftp.xDisconnect();
                            sds.interrupt();
                            System.exit(1);
                        }
                    }else{
                        sftp.xDisconnect();
                        sds.interrupt();
                        System.exit(1);
                    }
                }

                //create file for export to the server
                json_obj = new JSONObject();

                System.out.println("create another upload");
                if(!create_upload(loRSB.getString("sBranchCD"))) {
                    sftp.xDisconnect();
                    sds.interrupt();
                    System.exit(1);
                }

                //(*) Inform app server about files for upload
                System.out.println(json_obj.toJSONString());
                String lsFile = (String)json_obj.get("sFileName");
                //2017.11.06 11:25pm
                //includes testing if file is created from create_upload...
                if(!lsFile.equalsIgnoreCase("")){
                    if(upload_info(json_obj.toJSONString()).equalsIgnoreCase("success")){
                        if(!notify_success((String)json_obj.get("sFileName"))){
                            sftp.xDisconnect();
                            sds.interrupt();
                            System.exit(1);
                        }

                        if(!upload_file((String) json_obj.get("sFileName"))){
                            sftp.xDisconnect();
                            sds.interrupt();
                            System.exit(1);
                        }        

                        String sMD5Hash = upload_hash((String) json_obj.get("sFileName"));

                        //if(MD5 of upload files is equals to previously get MD5)
                        if(((String) json_obj.get("sMD5Hashx")).equalsIgnoreCase(sMD5Hash)){
                            //(*)inform app server that upload of file is successful...
                            System.out.println("Informing app server that upload of file is successful: " + Calendar.getInstance().getTime());
                            if(upload_success(json_obj.toJSONString()).equalsIgnoreCase("success")){
                                //update local database that file was already exported...
                                System.out.println("Updating local database that file was already exported: " + Calendar.getInstance().getTime());
                                post_success((String) json_obj.get("sFileName"));
                            }
                        }//if(((String) json_obj.get("sMD5Hashx")).equalsIgnoreCase(sMD5Hash)){
                    } //if(upload_info(json_obj.toJSONString()).equalsIgnoreCase("success")){
                }
            }//while(loRSB.next()){ 
        }
        catch (SQLException ex) {
            logwrapr.severe("post_success: IOException error detected.", ex);
        }
        
        System.out.println("Disconnecting SFTF: " + Calendar.getInstance().getTime());
        sftp.xDisconnect();
        System.out.println("Stopping Thread: " + Calendar.getInstance().getTime());
        sds.interrupt();
        System.out.println("Thread Stopped: " + Calendar.getInstance().getTime());
        System.exit(0);
    }

    private static boolean upload_backlog(String sBranchCD){
        JSONParser oParser = new JSONParser();
        Boolean bErr = false;
        try{
            String lsSQL = "SELECT *" +
                          " FROM xxxOutgoingLog" + 
                          " WHERE sFileName LIKE " + SQLUtil.toSQL(sBranchCD + "%") + 
                            " AND sMD5Hashx <> ''" + 
                          " ORDER BY sFileName DESC";    
                            //" AND cTranStat IN ('1', '0')" + 
                                    
            ResultSet rs = instance.getConnection().createStatement().executeQuery(lsSQL);
            System.out.println(lsSQL);
            
            if (rs.next()){
               //kalyptus - 2016.06.24 09:05am
               //regardless of cTranStat check repl server 
               
               if (rs.getString("cTranStat").equalsIgnoreCase("1")){
                    json_obj.put("sFileName", rs.getString("sFileName"));
                    json_obj.put("sLogFromx", rs.getString("sLogFromx"));
                    json_obj.put("sLogThrux", rs.getString("sLogThrux"));                
                    json_obj.put("sMD5Hashx", rs.getString("sMD5Hashx"));
                    json_obj.put("sFileSize", rs.getString("sFileSize"));
                    json_obj.put("dCreatedx", MiscReplUtil.format(rs.getTimestamp("dCreatedx"), "yyyy-MM-dd HH:mm:ss"));
                    json_obj.put("cTranStat", "1"); 
               }
               else if(rs.getString("cTranStat").equalsIgnoreCase("0")) {
                    System.out.println("upload_backlog:FileName is empty!");
                    json_obj.put("sFileName", rs.getString("sFileName"));
                    json_obj.put("sLogFromx", rs.getString("sLogFromx"));
                    json_obj.put("sLogThrux", rs.getString("sLogThrux"));                
                    json_obj.put("sMD5Hashx", rs.getString("sMD5Hashx"));
                    json_obj.put("sFileSize", rs.getString("sFileSize"));
                    json_obj.put("dCreatedx", MiscReplUtil.format(rs.getTimestamp("dCreatedx"), "yyyy-MM-dd HH:mm:ss"));
                    json_obj.put("cTranStat", "0"); 
                    System.out.println("upload_backlog:calling upload info!");
                    if(!upload_info(json_obj.toJSONString()).equalsIgnoreCase("success")) return false;
                    System.out.println("upload_backlog:calling notify_sucess!");
                    if(!notify_success((String)json_obj.get("sFileName"))) return false;
                    System.out.println("upload_backlog:done uploading backlog info!");
               }
               else{
                  json_obj.put("sFileName", "");
                  json_obj.put("cTranStat", "");                     
               }
            }
            else{
               json_obj.put("sFileName", "");
               json_obj.put("cTranStat", "");               
            }
//        } catch (ParseException ex) {
//            logwrapr.severe("upload_backlog: ParseException error detected.", ex);
//            bErr = true;
//        } catch (MalformedURLException ex) {
//            logwrapr.severe("upload_backlog: MalformedURLException error detected.", ex);
//            bErr = true;
//        } catch (IOException ex) {
//            logwrapr.severe("upload_backlog: IOException error detected.", ex);
//            bErr = true;
        } catch (SQLException ex) {
            logwrapr.severe("upload_backlog: SQLException error detected.", ex);
            bErr = true;
        }    

        return !bErr;
    }
    
   private static void prepareSFTPHost(){
       GCrypt loEnc = new GCrypt(SIGNATURE);
       GProperty loProp = new GProperty("ReplicaXP");

       sftp = new SFTP_DU();
       //sftp.setHost(loProp.getConfig("sftphost"));
       sftp.setPort(Integer.valueOf(loProp.getConfig("sftpport")));
       sftp.setUser(loEnc.decrypt(loProp.getConfig("sftpuser")));
       sftp.setPassword(loEnc.decrypt(loProp.getConfig("sftppass")));

       host_dir = loProp.getConfig("sftpfldr");

       String sFTPHost[] = loProp.getConfig("sftphost").split(";");

       for(int x=0;x < sFTPHost.length;x++){
          if(sftp.xConnect(sFTPHost[x])){
              System.out.println("Connected to SFTP: " + sFTPHost[x]);
              GUANZON_SITE = "http://" + sFTPHost[x] + ":2007";
              x = sFTPHost.length;
          }
       }

//        String sWebHost[] = loProp.getConfig("websrvcs").split(";");
//        for(int x=0;x < sWebHost.length;x++){
//           if(WebClient.isHttpURLConOk(sWebHost[x])){
//               GUANZON_SITE = sWebHost[x];
//               System.out.println("Connected to WEB: " + sWebHost[x]);
//               x = sWebHost.length;
//           }    
//        }        
   }

    private static String upload_success(String json){
        String success = "";
        //(*)inform app server that upload of file is successful...
        try {
            System.out.println("Upload success:");
            System.out.println("Json:" + json);
            success = WebClient.httpPostJSon(GUANZON_SITE + "/upload/success.php", json);
            System.out.println("Success: " + success);
        } catch (MalformedURLException ex) {
            logwrapr.severe("upload_success: MalformedURLException error detected.", ex);
        } catch (IOException ex) {
            logwrapr.severe("upload_success: IOException error detected.", ex);
        }
        
        return success;
    }
    
    private static boolean notify_success(String filename){
        boolean bErr = false;
        
        try {
            String lsSQL = "UPDATE xxxOutgoingLog" +
                          " SET cTranStat = '1'" + 
                          " WHERE sFileName = " + SQLUtil.toSQL(filename);
            instance.getConnection().createStatement().executeUpdate(lsSQL);
            json_obj.put("cTranStat", "1"); 
        } 
        catch (SQLException ex) {
            logwrapr.severe("notify_success: IOException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }

    private static boolean post_success(String filename){
        boolean bErr = false;
        
        try {
            String lsSQL = "UPDATE xxxOutgoingLog" +
                          " SET cTranStat = '2'" + 
                             ", dExported = " + SQLUtil.toSQL(instance.getServerDate()) +  
                          " WHERE sFileName = " + SQLUtil.toSQL(filename);
            instance.getConnection().createStatement().executeUpdate(lsSQL); 
            
            File file = new File(instance.getApplPath() + "/repl/upload/unzipped/" + filename + ".json");
            if (file.exists()) {
                file.delete();
            }   
        } 
        catch (SQLException ex) {
            logwrapr.severe("post_success: IOException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }
    
    private static boolean create_upload(String sBranchCD){
        ResultSet rs = null;
        String filename;

        //get server date
        json_obj.put("dCreatedx", MiscReplUtil.format(instance.getServerDate(), "yyyy-MM-dd HH:mm:ss"));
        json_obj.put("cTranStat", "0");
        
        //set filename;
        //remove space and colon from the value return by getTime.toString
        filename = sBranchCD + MiscReplUtil.format(Calendar.getInstance().getTime(), "yyyyMMddHHmmss");

        json_obj.put("sFileName", filename);
        
        //kalyptus - 2015.12.22 01:54pm
        //This part was previously at the top. I transferred it here so that the date of creation will
        //be immediately recorded here...
        
        //Extract records to be exported
        rs = extract_record(sBranchCD);
        if(rs == null){ 
            //reset name to empty and return true...
            json_obj.put("sFileName", "");
            return true;
        }
        
        //Save extracted record as json
        if(!write_file(rs, filename)) return false;
        
        if(!tar_json(filename)) return false;
        
        //record info to the JSON objet obj
        json_obj.put("sMD5Hashx", MiscReplUtil.md5Hash(instance.getApplPath() + "/repl/upload/zipped/" + filename + ".json.tar.gz"));

        //Save size of tarred file...
        File file = new File(instance.getApplPath() + "/repl/upload/zipped/" + filename + ".json.tar.gz");
        if (file.exists()) {
            //DecimalFormat df = new DecimalFormat("##.##");
            json_obj.put("sFileSize", String.format("%.2f", (float)file.length()/1024));
        }           
        
        //Save info the local database
        String lsSQL = "INSERT INTO xxxOutgoingLog" + 
                      " SET sFileName = " + SQLUtil.toSQL(filename) + 
                         ", sLogFromx = " + SQLUtil.toSQL((String) json_obj.get("sLogFromx")) + 
                         ", sLogThrux = " + SQLUtil.toSQL((String) json_obj.get("sLogThrux")) + 
                         ", sFileSize = " + SQLUtil.toSQL((String) json_obj.get("sFileSize")) + 
                         ", sMD5Hashx = " + SQLUtil.toSQL((String) json_obj.get("sMD5Hashx")) + 
                         ", dCreatedx = " + SQLUtil.toSQL(json_obj.get("dCreatedx")) + 
                         ", cTranStat = '0'";
         
        instance.beginTrans();
        
        boolean bErr = false;
        try {
            instance.getConnection().createStatement().execute(lsSQL);
        } catch (SQLException ex) {
            logwrapr.severe("create_upload: SQLException error detected.", ex);
            bErr = true;
        }
        
        if(bErr)
            instance.rollbackTrans();
        else{
            instance.commitTrans();
            
        }
        
        return !bErr;
    }

    private static ResultSet extract_record(String sBranchCD){
        boolean bErr = false;
        String from = "";
        String thru = "";
        ResultSet rs = null;
        
        if(!getFilter(sBranchCD)) return rs;
        
        try {
            String lsSQL = "SELECT *" +
                          " FROM xxxReplicationLog" + 
                          " WHERE sTransNox LIKE " + SQLUtil.toSQL(sBranchCD + "%") + 
                            " AND sTransNox BETWEEN " + SQLUtil.toSQL((String) json_obj.get("sLogFromx")) + " AND " + SQLUtil.toSQL((String) json_obj.get("sLogThrux"));
            System.out.println(lsSQL);
            rs = instance.getConnection().createStatement().executeQuery(lsSQL);
        } 
        catch (SQLException ex) {
            logwrapr.severe("extract_record: IOException error detected.", ex);
            bErr = true;
        }
        
        return rs;
    }
    
    private static boolean write_file(ResultSet rs, String filename){
        boolean bErr = false;
        try {
           
            String destpath = instance.getApplPath() + "/repl/upload/unzipped/";
            File destpth = new File(destpath);
            if (!destpth.exists()) {
                System.out.println("I am here");
                destpth.mkdirs();
            } 
           
            OutputStream out = new FileOutputStream(instance.getApplPath() + "/repl/upload/unzipped/" + filename + ".json");
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));

            LinkedList<Map<String, String>> link = RStoLinkList(rs);
            
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

    
//Try other write_file
////    private static boolean write_file(ResultSet rs, String filename){
////        boolean bErr = false;
////        try {
////
////            String destpath = instance.getApplPath() + "/repl/upload/unzipped/";
////            File destpth = new File(destpath);
////            if (!destpth.exists()) {
////                System.out.println("I am here");
////                destpth.mkdirs();
////            }           
////            
////            OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(instance.getApplPath() + "/repl/upload/unzipped/" + filename + ".json"),"UTF-8");
////            out.write(MiscReplUtil.RStoJSON(rs));
////            out.flush();
////            out.close();
////            
//////            FileWriter file = new FileWriter(instance.getApplPath() + "/repl/upload/unzipped/" + filename + ".json");
//////            file.write(MiscReplUtil.RStoJSON(rs));
//////            file.flush();
//////            file.close();          
////            
////        } catch(IOException ex){
////            logwrapr.severe("write_file: IOException error detected.", ex);
////            bErr = true;
////        } catch (SQLException ex) {
////            logwrapr.severe("write_file: SQLException error detected.", ex);
////            bErr = true;
////        }
////        
////        return !bErr;
////    }
    
    private static boolean tar_json(String filename){
        boolean bErr = false;
        
        try {
            MiscReplUtil.tar(instance.getApplPath() + "/repl/upload/unzipped/" + filename + ".json", instance.getApplPath() + "/repl/upload/zipped/");
            
        } catch (IOException ex) {
            logwrapr.severe("tar_json: IOException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }

    private static boolean upload_file(String filename){
        boolean bErr = false;
        try {
            System.out.println("uploading file:" + filename);
//            System.out.println(instance.getApplPath() + "/repl/upload/zipped/"  + "»" +  host_dir + "upload/zipped/" + instance.getBranchCode() + "/" + "»" + filename + ".json.tar.gz");
//            sftp.Upload(instance.getApplPath() + "/repl/upload/zipped/"  , host_dir + "upload/zipped/" + instance.getBranchCode() + "/", filename + ".json.tar.gz") ;

            System.out.println(instance.getApplPath() + "/repl/upload/zipped/"  + "»" +  host_dir + "upload/zipped/" + filename.substring(0, 4) + "/" + "»" + filename + ".json.tar.gz");
            sftp.Upload(instance.getApplPath() + "/repl/upload/zipped/"  , host_dir + "upload/zipped/" + filename.substring(0, 4) + "/", filename + ".json.tar.gz") ;
        } catch (Exception ex) {
            logwrapr.severe("upload_file: Exception error detected.", ex);
            bErr = true;
        }

        return !bErr;
    }
    
    private static String upload_hash(String filename){
        String sMD5Hash = "";
        try {
            sMD5Hash = WebClient.httpGet(GUANZON_SITE + "/upload/md5_hash.php?file=" + filename);
        } catch (MalformedURLException ex) {
            logwrapr.severe("upload_hash: MalformedURLException error detected.", ex);
        } catch (IOException ex) {
            logwrapr.severe("upload_hash: IOException error detected.", ex);
        }

        return sMD5Hash;
    }

    public static String upload_info(String json){
        String success = "";
        try {
            System.out.println("info:" + json);
            System.out.println(GUANZON_SITE + "/upload/info.php");
            success = WebClient.httpPostJSon(GUANZON_SITE + "/upload/info.php", json);
            System.out.println("success: " + success);
        } catch (MalformedURLException ex) {
            logwrapr.severe("upload_info: MalformedURLException error detected.", ex);
        } catch (IOException ex) {
            logwrapr.severe("upload_info: IOException error detected.", ex);
        }
        return success;
    }
    
    private static boolean getFilter(String sBranchCD){
        boolean bErr = false;
        ResultSet rs = null;
        
        String from = "";
        String thru = "";
        
        try {
            //kalyptus - 2019.11.29 09:47am
            //Start from records that are tagged as sent/exported...
            String lsSQL = "SELECT sLogFromx, sLogThrux, dCreatedx" + 
                          " FROM xxxOutgoingLog" + 
                          " WHERE sFileName LIKE " + SQLUtil.toSQL(sBranchCD + '%') + 
                              " AND cTranStat IN ('2', '4')" +
                          " ORDER BY dCreatedx DESC LIMIT 1";
            System.out.println("Filter: " + lsSQL);
            rs = instance.getConnection().createStatement().executeQuery(lsSQL);
            
            if(rs.next()){
               //Previously dModified >= dCreated
                String lsSQLx = "SELECT sTransNox, COUNT(*)" + 
                               " FROM xxxReplicationLog" + 
                               " WHERE sTransNox BETWEEN " + SQLUtil.toSQL(rs.getString("sLogFromx")) + " AND " + SQLUtil.toSQL(rs.getString("sLogThrux")) + 
                                 " AND dModified >= " + SQLUtil.toSQL(rs.getTimestamp("dCreatedx")) +
                               " GROUP BY sTransNox" + 
                               " HAVING COUNT(*) >= 1" + 
                               " ORDER BY sTransNox" +
                               " LIMIT 1";
                System.out.println("Check previous: " + lsSQLx);
                ResultSet rsx = instance.getConnection().createStatement().executeQuery(lsSQLx);
                if(rsx.next()){
                    from = rsx.getString("sTransNox");
                    json_obj.put("sLogFromx", from);
                }
                else{
                    from = rs.getString("sLogThrux");
                    json_obj.put("sLogFromx", from);
                }
            }
            else{
                lsSQL = "SELECT sExportNo" + 
                       " FROM xxxSysClient" + 
                       " WHERE sBranchCD = " + SQLUtil.toSQL(sBranchCD);
                rs = instance.getConnection().createStatement().executeQuery(lsSQL);
                System.out.println("Filter #2" + lsSQL);
                if(rs.next()){
                    from = rs.getString("sExportNo");
                    json_obj.put("sLogFromx", from);
                }
            }
            
            if(from.isEmpty())
                return false;
            
            lsSQL = "SELECT sTransNox" + 
                   " FROM xxxReplicationLog" + 
                   " WHERE sTransNox LIKE " + SQLUtil.toSQL(sBranchCD + "%") + 
                   " ORDER BY sTransNox DESC LIMIT 1";
            System.out.println(lsSQL);
            rs = instance.getConnection().createStatement().executeQuery(lsSQL);
            if(rs.next()){
                thru = rs.getString("sTransNox");
                json_obj.put("sLogThrux", thru);
            }
            
            if(thru.isEmpty())
                return false;
            
        } catch (SQLException ex) {
            logwrapr.severe("getFilter: SQLException error detected.", ex);
            bErr = true;
        } finally {
            MiscUtil.close(rs);
        }
        
        if(bErr)
            return false;
        else
        {
            if(from.equals(thru))
                return false;
            else
                return true;
        }
    }

    private static void doBackup(String sBranchCD){
       BackUp loback = new BackUp(instance);
       Calendar cal = Calendar.getInstance();
       String dateformat = "yyyyMMddHHmmss";
       
       //String last = MiscReplUtil.fileRead(instance.getApplPath() + "/backup-" + sBranchCD + ".me");
       String last = getModified(sBranchCD);
       
       if(!last.isEmpty()){
          if(MiscReplUtil.format(cal.getTime(), dateformat).compareTo(last) < 0 )
             return;
       }
       
       if(loback.doBackup(sBranchCD)){
            cal.add(Calendar.DATE, 7);
            //MiscReplUtil.fileWrite(instance.getApplPath() + "/backup.me", MiscReplUtil.format(cal.getTime(), dateformat));
            setModified(sBranchCD, MiscReplUtil.format(cal.getTime(), dateformat));
       }
    }

    private static ResultSet extract_branch(){
        ResultSet rs = null;
        try {
            String lsSQL = "SELECT sBranchCD" + 
                          " FROM Branch_Others a" + 
                          " WHERE xBranchCD = " + SQLUtil.toSQL(instance.getBranchCode()) + 
                          " ORDER BY sBranchCD";  
            
            //create statement to be use in executing queries
            System.out.println(lsSQL);
            rs = instance.getConnection().createStatement().executeQuery(lsSQL);
            System.out.println(rs.getRow());
        } 
        catch (SQLException ex) {
            logwrapr.severe("IOException error detected.", ex);
        }
        catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
         }                
        return rs;
    }    

   public static String getModified(String sBranchCD) {
      try {
         prop.load(new FileInputStream("backup.me"));
         String lsModified = prop.getProperty(sBranchCD + "-upload");
         return lsModified;
      } 
      catch (IOException ex) {
         return "";
      }
   }
   
   public static void setModified(String sBranchCD, String fdModified) {
      try {
         prop.load(new FileInputStream("backup.me"));
         prop.setProperty(sBranchCD + "-upload", fdModified);
         prop.store(new FileOutputStream("backup.me"), null);
      } 
      catch (IOException ex) {
      }
   }

    protected void finalize() throws Throwable {
        System.out.println("Starting to finalize Import.java: " + Calendar.getInstance().getTime());
    }
}

