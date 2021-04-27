/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.branch.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.appdriver.agent.MsgBox;
import static org.rmj.branch.util.UploadEDocFile.encodeFileToBase64Binary;
import org.rmj.lib.net.MiscReplUtil;
import org.rmj.lib.net.WebClient;
import org.rmj.xapitoken.util.RequestAccess;

/**
 *
 * @author sayso
 */
public class UploadEDocFiles {
    private static GRiderX instance = null;
    private static String url = "https://webfsgk.guanzongroup.com.ph/x-api/v1.0/edocsys/upload_edoc_file.php";        
    private static JSONObject token = null;
    public static void main(String []args){
        
        //Set important path configuration for this utility
        String path;
        if(System.getProperty("os.name").toLowerCase().contains("win")){
            path = "D:/GGC_Java_Systems";
        }
        else{
            path = "/srv/GGC_Java_Systems";
        }
        System.setProperty("sys.default.path.temp", path + "/temp");
        System.setProperty("sys.default.path.config", path);

        String args_0 = "";      //file name of access token
        
        switch(args.length){
            case 0:
                args_0 = System.getProperty("sys.default.path.config") + "/access.token";
                break;
            case 1:    
                args_0 = args[0];
                break;
            default: 
                System.out.println("UploadEDocFiles <acess_token>");
                System.exit(1);
        }
        
        instance = new GRiderX("gRider");
        if(!instance.getErrMsg().isEmpty()){
            System.out.println("error");
            System.exit(1);
        }

        instance.setOnline(false);
        
        uploadClientImages(args_0);
        
    }
    
    private static void uploadClientImages(String access){
        JSONObject param = new JSONObject();
        JSONParser parser = new JSONParser();
        String sql = "SELECT sClientID, sImageNme, sMD5Hashx, dImgeDate, sImagePth" +
                    " FROM Client_Images" +
                    " WHERE sScanndID = " + SQLUtil.toSQL(instance.getBranchCode()) +
                      " AND cImgeStat = '0'" +
                    " ORDER BY dImgeDate";
        try {
            ResultSet edocs = instance.getConnection().createStatement().executeQuery(sql);
            
            while(edocs.next()){
                String hash;
                File file = new File(edocs.getString("sImagePth") + "/" + edocs.getString("sImageNme"));
                
                //check if file is existing
                if(!file.exists()){
                    continue;
                }
                
                //check if file hash is not empty
                hash = edocs.getString("sMD5Hashx");
                if(edocs.getString("sMD5Hashx").isEmpty()){
                    hash = MiscReplUtil.md5Hash(edocs.getString("sImagePth") + "/" + edocs.getString("sImageNme"));
                }
                
                param = new JSONObject();
                param.put("g-edoc-type", "0002");
                param.put("g-edoc-ownr", edocs.getString("sClientID"));
                param.put("g-edoc-srcd", "");
                param.put("g-edoc-srno", "");
                param.put("g-edoc-file", edocs.getString("sImageNme"));
                param.put("g-edoc-scnr", instance.getBranchCode());
                param.put("g-edoc-imge", encodeFileToBase64Binary(file));
                param.put("g-edoc-hash", hash);
                
                //Set the access_key as the header of the URL Request
                JSONObject headers = new JSONObject();
                headers.put("g-access-token", getAccessToken(access));

                String response = WebClient.httpPostJSon(url, param.toJSONString(), (HashMap<String, String>) headers);
                //System.out.println(param.toJSONString());
                if(response == null){
                    System.out.println("HTTP Error detected: " + System.getProperty("store.error.info"));
                    return;
                }
                
                JSONObject result = (JSONObject) parser.parse(response);
                
                if(((String)result.get("result")).equalsIgnoreCase("success")){
                    sql = "UPDATE Client_Images" + 
                         " SET cImgeStat = '1'" + 
                         " WHERE sClientID = " + SQLUtil.toSQL(edocs.getString("sClientID")) + 
                           " AND sImageNme = " + SQLUtil.toSQL(edocs.getString("sImageNme"));
                    instance.executeQuery(sql, "Client_Images", instance.getBranchCode(), "");
                }
                
            }
            
        } catch (SQLException ex) {
            Logger.getLogger(UploadEDocFiles.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(UploadEDocFiles.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
    }
    
    public static void uploadRegDocImages(String args_0){
        
    }
    
    private static String encodeFileToBase64Binary(File file) throws Exception{
         FileInputStream fileInputStreamReader = new FileInputStream(file);
         byte[] bytes = new byte[(int)file.length()];
         fileInputStreamReader.read(bytes);
         return new String(Base64.encodeBase64(bytes), "UTF-8");
     }    
    
    private static String getAccessToken(String access){
        try {
            JSONParser oParser = new JSONParser();
            
            if(token == null){
                token = (JSONObject)oParser.parse(new FileReader(access));
                
            }
            
            Calendar current_date = Calendar.getInstance();
            current_date.add(Calendar.MINUTE, -25);
            Calendar date_created = Calendar.getInstance();
            date_created.setTime(SQLUtil.toDate((String) token.get("created") , SQLUtil.FORMAT_TIMESTAMP));
            
            //Check if token is still valid within the time frame
            //Request new access token if not in the current period range
            if(current_date.after(date_created)){
                String[] xargs = new String[] {(String) token.get("parent"), access};
                RequestAccess.main(xargs);
                token = (JSONObject)oParser.parse(new FileReader(access));
            }
            
            return (String)token.get("access_key");
        } catch (IOException ex) {
            return null;
        } catch (ParseException ex) {
            return null;
        }
    }
}
