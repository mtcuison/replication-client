/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.xapitoken.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.net.WebClient;

/**
 *
 * @author sayso
 */
public class RequestToken {
    public static String url = "https://restgk.guanzongroup.com.ph/x-api/v1.0/auth/token_request.php";
    public static String FORMAT_TIMESTAMP  = "yyyy-MM-dd HH:mm:ss";
    
    public static void main(String[] args) throws IOException{
        String path;
        if(System.getProperty("os.name").toLowerCase().contains("win")){
            path = "D:/GGC_Java_Systems";
        }
        else{
            path = "/srv/GGC_Java_Systems";
        }
        System.setProperty("sys.default.path.temp", path + "/temp");
        System.setProperty("sys.default.path.config", path);
        
        Map<String, String> headers = 
                        new HashMap<String, String>();
        headers.put("Accept", "application/json");
        headers.put("Content-Type", "application/json");
        
        switch(args.length){
            case 3:
                headers.put("g-api-id", args[0]);
                headers.put("g-api-client", args[1]);    
                headers.put("g-api-user", args[2]);    
                break;
            case 0:
                headers.put("g-api-id", "IntegSys");
                headers.put("g-api-client", "XPI_GX001");    
                headers.put("g-api-user", "GAP0190004");    
                break;
            default:
                System.out.println("RequestToken <Product ID> <Client ID> <User ID>");
                System.exit(1);
        }

        //JSONParser oParser = new JSONParser();
        //JSONObject json_obj = null;

        //String response = WebClient.httpPostJSon(sURL, param.toJSONString(), (HashMap<String, String>) headers);
        String response = WebClient.httpsPostJSon(url, null, (HashMap<String, String>) headers);
        if(response == null){
            System.out.println("HTTP Error detected: " + System.getProperty("store.error.info"));
            //return null;
            System.exit(1);
        }
        
        JSONParser oParser = new JSONParser();
        try {
            JSONObject oJson = (JSONObject) oParser.parse(response);
            
            if(((String)oJson.get("result")).equalsIgnoreCase("success")){
                JSONObject oData = new JSONObject();
                
                oData.put("client_key", (String)((JSONObject)oJson.get("payload")).get("token"));
                //oData.put("client_key", (String)((JSONObject) oParser.parse((String)oJson.get("payload"))).get("token"));
                oData.put("created", dateFormat(Calendar.getInstance().getTime(), FORMAT_TIMESTAMP));

                OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(System.getProperty("sys.default.path.config") + "/client.token"),"UTF-8");
                out.write(oData.toJSONString());
                out.flush();
                out.close();
            }
            
        } catch (ParseException ex) {
            //Logger.getLogger(RequestToken.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        }

        //System.out.println(json_obj.toJSONString());
        System.out.println(response);
    }
    
   private static String dateFormat(Object date, String format){
      SimpleDateFormat sf = new SimpleDateFormat(format);
      String ret;
      if ( date instanceof Timestamp )
         ret = sf.format((Date)date);
      else if ( date instanceof Date )
         ret = sf.format(date);
      else if ( date instanceof Calendar ){
         Calendar loDate = (Calendar) date;
         ret = sf.format(loDate.getTime());
         loDate = null;
      }
      else
         ret = null;

      sf = null;
      return ret;
    }    
    
}
