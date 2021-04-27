//Use in downloading data to the app server

//download_info/(branch)
    //server side->searches the database about list-of-files-for-download of a branch 
    //get json info of files for download of a branch
//download_success/(json)
    //server side->tag the record in the database that the file was successfully downloaded
    //returns success if file was downloaded successfully

//download_init/(json)
    //

//status: tested and okey

package org.rmj.replication.client;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.Socket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GProperty;
import org.rmj.appdriver.SQLUtil;
import static org.rmj.replication.client.Upload.upload_info;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.MiscReplUtil;
import org.rmj.lib.net.SFTP_DU;
import org.rmj.lib.net.WebClient;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.appdriver.agent.MsgBox;

public class Download {
    private static String SIGNATURE = "08220326";
    private static String GUANZON_SITE = null; 
    private static LogWrapper logwrapr = new LogWrapper("Client.Download", "temp/Download.log");
    private static String host_dir = null;
    private static SFTP_DU sftp;
    private static GRiderX instance = null;
    private static JSONObject json_obj = null;

    public static void main(String[] args) {
        DownloadServer sds = null;  
        
        try {
            Socket clientSocket = new Socket("localhost", DownloadServer.PORT);
            System.out.println("*** Already running!");
            System.exit(1);
        }
        catch (Exception e) {
            sds = new DownloadServer();
            sds.start();
        }
       
        System.out.println(System.getProperty("user.dir"));
        
        instance = new GRiderX("gRider");

        if(!instance.getErrMsg().isEmpty()){
            MsgBox.showOk(instance.getMessage() + instance.getErrMsg());
            sds.interrupt();
            System.exit(1);
        }

        instance.setOnline(false);
        
        json_obj = new JSONObject();
        
        //System.out.println(instance.getImagePath());
        //load FTP information
        prepareSFTPHost();
        
        //While server replies positive for existences of for import files...
        while(download_info()){
            
            System.out.println("Filename: " + (String) json_obj.get("sFileName"));
            if(((String) json_obj.get("sFileName")).equals("")) break;
            
            //Save filename and MD5 hash to local database
            if(!save_download_info()) break;
            
            //Download files
            if(!download_file((String) json_obj.get("sFileName"))) break;

            //if(MD5 of downloaded files is equals to previously get MD5)
            if(MiscReplUtil.md5Hash(instance.getApplPath() + "/repl/download/zipped/" + (String) json_obj.get("sFileName") + ".json.tar.gz").equalsIgnoreCase((String) json_obj.get("sMD5Hashx"))){
                //inform app server that download of file is successful...
                String res = download_success(json_obj.toJSONString());
                if(!res.equalsIgnoreCase("success")){
                    System.out.println(res);
                    break;
                }
                if (!post_success((String) json_obj.get("sFileName"))) break;
            }    
        }     
        
        sftp.xDisconnect();
        sds.interrupt();
        System.exit(0);
   }
   
    public static  boolean download_info(){
        JSONParser oParser = new JSONParser();
        Boolean bErr = false;
        try{
            //System.out.println(GUANZON_SITE + "/download/info.php?branch=" + instance.getBranchCode());
            json_obj = (JSONObject) oParser.parse(WebClient.httpGetJSon(GUANZON_SITE + "/download/info.php?branch=" + instance.getBranchCode()));
            System.out.println(json_obj.toJSONString());
        } catch (ParseException ex) {
            logwrapr.severe("download_info: ParseException error detected.", ex);
            bErr = true;
        } catch (MalformedURLException ex) {
            logwrapr.severe("download_info: MalformedURLException error detected.", ex);
            bErr = true;
        } catch (IOException ex) {
            logwrapr.severe("download_info: IOException error detected.", ex);
            bErr = true;
        }    

        return !bErr;
    }   
    
    private static void prepareSFTPHost(){
        GCrypt loEnc = new GCrypt(SIGNATURE);
        
        
        GProperty loProp = loProp = new GProperty("ReplicaXP");
                
        sftp = new SFTP_DU();

        // sftp.setHost(loProp.getConfig("sftphost"));
        
        sftp.setPort(Integer.valueOf(loProp.getConfig("sftpport")));
        sftp.setUser(loEnc.decrypt(loProp.getConfig("sftpuser")));
        sftp.setPassword(loEnc.decrypt(loProp.getConfig("sftppass")));

        //System.out.println(loEnc.decrypt(loProp.getConfig("sftpuser")));
        //System.out.println(loEnc.decrypt(loProp.getConfig("sftppass")));
        //System.out.println(Integer.valueOf(loProp.getConfig("sftpport")));
        
//        System.exit(0);
                
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
//               x = sWebHost.length;
//           }    
//        }        
    }    

    private static boolean save_download_info(){
        boolean bErr = false;
        
        try {
            String lsSQL = "SELECT *" +
                          " FROM xxxIncomingLog" + 
                          " WHERE sFileName = " + SQLUtil.toSQL((String) json_obj.get("sFileName"));
            ResultSet rs = instance.getConnection().createStatement().executeQuery(lsSQL);
            System.out.println(lsSQL);
            if(!rs.next()){
                lsSQL = "INSERT INTO xxxIncomingLog" +
                        " SET sFileName = " + SQLUtil.toSQL((String) json_obj.get("sFileName")) +  
                           ", sMD5Hashx = " + SQLUtil.toSQL((String) json_obj.get("sMD5Hashx")) +  
                           ", sFileSize = " + SQLUtil.toSQL((String) json_obj.get("sFileSize")) +  
                           ", dCreatedx = " + SQLUtil.toSQL((String) json_obj.get("dCreatedx")) +  
                           ", cTranStat = '0'";    
                instance.getConnection().createStatement().executeUpdate(lsSQL);
            }
        } 
        catch (SQLException ex) {
            logwrapr.severe("save_download_info: SQLException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
        
    }
    
    private static boolean download_file(String filename){
        boolean bErr = false;
        try {
            
            String downpath = instance.getApplPath() + "/repl/download/zipped/";
            File downpth = new File(downpath);
            if (!downpth.exists()) {
                downpth.mkdirs();
            }            
            
            System.out.println("Downloading " + filename);
            //System.out.println(host_dir + "download/zipped/" + instance.getBranchCode() + "/");
            sftp.Download(host_dir + "download/zipped/" + instance.getBranchCode() + "/"
                        , instance.getApplPath() + "/repl/download/zipped/"
                        , filename + ".json.tar.gz");
            System.out.println("Downloaded " + filename);
            
        } catch (Exception ex) {
            logwrapr.severe("download_file: Exception error detected.", ex);
            bErr = true;
        }

        return !bErr;
    }    

    private static String download_success(String json){
        String success = "";
        //(*)inform app server that upload of file is successful...
        try {
            success = WebClient.httpPostJSon(GUANZON_SITE + "/download/success.php", json);

        } catch (MalformedURLException ex) {
            success = "";
            logwrapr.severe("download_success: MalformedURLException error detected.", ex);
        } catch (IOException ex) {
            success = "";
            logwrapr.severe("download_success: IOException error detected.", ex);
        }
        
        return success;
    }    
    
    private static boolean post_success(String filename){
        boolean bErr = false;
        
        try {
            String lsSQL = "UPDATE xxxIncomingLog" +
                          " SET dReceived = " + SQLUtil.toSQL(instance.getServerDate()) +  
                             ", cTranStat = '1'" +
                          " WHERE sFileName = " + SQLUtil.toSQL(filename);   

            instance.getConnection().createStatement().executeUpdate(lsSQL);              
        } 
        catch (SQLException ex) {
            logwrapr.severe("post_success: SQLException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }
}
