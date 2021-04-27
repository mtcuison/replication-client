/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.client;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.appdriver.agent.MsgBox;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.WebClient;

/**
 *
 * @author sayso
 */
public class SendBio {
    private static LogWrapper logwrapr = new LogWrapper("Client.SendBio", "D:/GGC_Java_Systems/temp/SendBio.log");
    private static GRiderX instance = null;
    
    public static void main(String[] args) {
        logwrapr.severe("I am inside main...");
        String lsEmployID;  
        //Check validity of parameter
        if(args.length == 0){
            lsEmployID = "M00108011504";
            System.exit(1);
        }
        else
            lsEmployID = args[0];
        
        logwrapr.severe("After extracting parameters...");
        
        instance = new GRiderX("gRider");
        logwrapr.severe("After initializing GRiderX...");
        
        if(!instance.getErrMsg().isEmpty()){
            logwrapr.severe(instance.getErrMsg());
            logwrapr.severe("GRiderX has error...");
            System.exit(1);
        }

        instance.setOnline(false);        
        logwrapr.severe("After initializing offline");
        
        //Initialize variables needed for sending BIO info
        String lsRightThm = "";
        String lsLeftThmb = "";
        
        //Extract needed BIO info
        String lsSQL = "SELECT * FROM Employee_Thumbmark WHERE sEmployID = " + SQLUtil.toSQL(lsEmployID);
        try {
            ResultSet loRS = instance.getConnection().createStatement().executeQuery(lsSQL);
            
            if(loRS.next()){
                lsRightThm = loRS.getString("sRghtThmb");
                lsLeftThmb = loRS.getString("sLeftThmb");                
            }
        } catch (SQLException ex) {
            logwrapr.severe("main: SQLException error detected.", ex);
            ex.printStackTrace();
            System.exit(1);
        }
        
        String sURL = "https://restgk.guanzongroup.com.ph/petmgr/sendbio.php";        
//        String sURL = "http://localhost/petmgr/sendbio.php";        
        Calendar calendar = Calendar.getInstance();
        //Create the header section needed by the API
        Map<String, String> headers = 
                        new HashMap<String, String>();
        headers.put("Accept", "application/json");
        headers.put("Content-Type", "application/json");
        headers.put("g-api-id", "IntegSys");
        headers.put("g-api-imei", "GMC_CLTXX");
        headers.put("g-api-key", SQLUtil.dateFormat(calendar.getTime(), "yyyyMMddHHmmss"));    
        headers.put("g-api-hash", org.apache.commons.codec.digest.DigestUtils.md5Hex((String)headers.get("g-api-imei") + (String)headers.get("g-api-key")));    
        headers.put("g-api-client", "GGC_BM002");    
        headers.put("g-api-user", "GAP0190001");    
        headers.put("g-api-log", "");    
        headers.put("g-api-token", "");    

        JSONObject param = new JSONObject();
        param.put("employid", lsEmployID);
        param.put("rghtthmb", lsRightThm);        
        param.put("leftthmb", lsLeftThmb);    
        
        JSONParser oParser = new JSONParser();
        JSONObject json_obj = null;

        //String response = WebClient.httpPostJSon(sURL, param.toJSONString(), (HashMap<String, String>) headers);
        String response;
        try {
            response = WebClient.httpPostJSon(sURL, param.toJSONString(), (HashMap<String, String>) headers);
            if(response == null){
                System.out.println("HTTP Error detected: " + System.getProperty("store.error.info"));
                logwrapr.severe("main: HTTP Error detected: " + System.getProperty("store.error.info"));
                System.exit(1);
            }
            
            //System.out.println(response);
            json_obj = (JSONObject) oParser.parse(response);
            String result = (String)json_obj.get("result");
            if(result.equalsIgnoreCase("error")){
                //write to a json file            
                FileWriter file = new FileWriter("D:/GGC_Java_Systems/temp/sendbio-response.json");
                file.write(response); 
                file.close();
                System.exit(2);
            }
            
            lsSQL = "UPDATE Employee_Thumbmark SET cSendStat = '1' WHERE sEmployID = " + SQLUtil.toSQL(lsEmployID);
            if(instance.executeUpdate(lsSQL)==0){
                //System.out.println("main: INSERT ERROR: " + instance.getErrMsg());
                logwrapr.severe("main: UPDATE ERROR: " + instance.getErrMsg());
                System.exit(1);                
            }            
            System.exit(0);
            
        } catch (IOException ex) {
            logwrapr.severe("main: IOException error detected.", ex);
            System.exit(1);
        } catch (ParseException ex) {
            logwrapr.severe("main: ParseException error detected.", ex);
            System.exit(1);
        }
    }
}