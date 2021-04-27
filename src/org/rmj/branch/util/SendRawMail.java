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
import java.util.Calendar;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.net.WebClient;
import org.rmj.xapitoken.util.RequestAccess;

/**
 *
 * @author sayso
 */
public class SendRawMail {
    
    public static void main(String []args){
        //String sURL = "http://localhost/x-api/v1.0/email/sendrawmail.php";        
        String sURL = "https://restgk.guanzongroup.com.ph/x-api/v1.0/mail/sendrawmail.php";        
        
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

        //Check access token
        JSONObject param = new JSONObject();
        JSONObject oJson;
        JSONParser oParser = new JSONParser();
        String args_0 = "";      //file name of access token
        String args_1 = "";
        JSONObject oPar = new JSONObject();
        try {
            File filex;
            int ctr;
            switch(args.length){
                case 0:
                    args_0 = System.getProperty("sys.default.path.config") + "/mac1975access.token";
                    args_1 = "D:/GGC_Java_Systems/temp/some-data.json";
                    oPar.put("to", "saysonm12@gmail.com");
                    oPar.put("subject", "Send Raw Mail");
                    oPar.put("body", "This is a sample email using sendrawmail.php");
                    oPar.put("filename1", "D:/Download/unlock_user_bg.jpg");
                    oPar.put("filename2", "D:/GGC_Java_Systems/images/title_bg2.jpg");

                    param.put("to", (String) oPar.get("to"));
                    param.put("subject", (String) oPar.get("subject"));
                    param.put("body", (String) oPar.get("body"));
                    

                    ctr = 1;
                    while(oPar.get("filename" + String.valueOf(ctr)) != null){
                        //System.out.println(ctr);
                        filex =  new File((String)oPar.get("filename" + String.valueOf(ctr)));
                        param.put("data" + String.valueOf(ctr), encodeFileToBase64Binary(filex));
                        param.put("filename" + String.valueOf(ctr), filex.getName());
                        ctr++;
                    }
                    
                    //System.out.println(param.get("g-edoc-imge"));
                    break;
                case 2:    
                    args_0 = args[0];
                    args_1 = args[1];
                    oPar = (JSONObject)oParser.parse(new FileReader(args_1));
                    
                    param.put("to", (String) oPar.get("to"));
                    param.put("subject", (String) oPar.get("subject"));
                    param.put("body", (String) oPar.get("body"));
                    
                    ctr = 1;
                    while(oPar.get("filename" + String.valueOf(ctr)) != null){
                        filex =  new File((String)oPar.get("filename" + String.valueOf(ctr)));
                        param.put("data" + String.valueOf(ctr), encodeFileToBase64Binary(filex));
                        param.put("filename" + String.valueOf(ctr), filex.getName());
                        ctr++;
                    }
                    
                    break;
                default: 
                    System.out.println("SendRawMail <acess_token> <json_info>");
                    System.exit(1);
            }
        
            //Load token
            oJson = (JSONObject)oParser.parse(new FileReader(args_0));
            Calendar current_date = Calendar.getInstance();
            current_date.add(Calendar.MINUTE, -25);
            Calendar date_created = Calendar.getInstance();
            date_created.setTime(SQLUtil.toDate((String) oJson.get("created") , SQLUtil.FORMAT_TIMESTAMP));
            
            //Check if token is still valid within the time frame
            //Request new access token if not in the current period range
            if(current_date.after(date_created)){
                String[] xargs = new String[] {(String) oJson.get("parent"), args_0};
                RequestAccess.main(xargs);
                oJson = (JSONObject)oParser.parse(new FileReader(args_0));
            }
            
            //Set the access_key as the header of the URL Request
            JSONObject headers = new JSONObject();
            headers.put("g-access-token", (String)oJson.get("access_key"));

            String response = WebClient.httpPostJSon(sURL, param.toJSONString(), (HashMap<String, String>) headers);
            //System.out.println(param.toJSONString());
            if(response == null){
                System.out.println("HTTP Error detected: " + System.getProperty("store.error.info"));
                System.exit(1);
            }
            //json_obj = (JSONObject) oParser.parse(response);
            //System.out.println(json_obj.toJSONString());

            oPar.putAll((JSONObject) oParser.parse(response));
            OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(args_1),"UTF-8");
            out.write(oPar.toJSONString());
            out.flush();
            out.close();

            System.out.println(response);

        } catch (ParseException ex) {
            //Logger.getLogger(SendRawMail.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        } catch (FileNotFoundException ex) {
            //Logger.getLogger(SendRawMail.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        } catch (IOException ex) {
            //Logger.getLogger(SendRawMail.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        } catch (Exception ex) {
            //Logger.getLogger(SendRawMail.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        }
    }
    
    public static String encodeFileToBase64Binary(File file) throws Exception{
         FileInputStream fileInputStreamReader = new FileInputStream(file);
         byte[] bytes = new byte[(int)file.length()];
         fileInputStreamReader.read(bytes);
         return new String(Base64.encodeBase64(bytes), "UTF-8");
     }
    
}
