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
import org.rmj.lib.net.MiscReplUtil;
import org.rmj.lib.net.WebClient;
import org.rmj.xapitoken.util.RequestAccess;

/**
 *
 * @author sayso
 */
public class UploadEDocFile {
    
    public static void main(String []args){
        String sURL = "https://webfsgk.guanzongroup.com.ph/x-api/v1.0/edocsys/upload_edoc_file.php";        
        
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
        File file;
        
        try {
        
            switch(args.length){
                case 0:
                    args_0 = System.getProperty("sys.default.path.config") + "/access.token";
                    args_1 = "D:/GGC_Java_Systems/temp/upload-data.json";

                    oPar.put("g-edoc-type", "0002");
                    oPar.put("g-edoc-ownr", "M00120000001");
                    oPar.put("g-edoc-srcd", "");
                    oPar.put("g-edoc-srno", "");
                    oPar.put("g-edoc-file", "D:/Download/unlock_user_bg.jpg");
                    oPar.put("g-edoc-scnr", "M001");
                    
                    
                    //System.out.println((String)param.get("g-edoc-hash"));
                    //System.out.println(param.get("g-edoc-imge"));
                    break;
                case 2:    
                    args_0 = args[0];
                    args_1 = args[1];
                    
                    oPar = (JSONObject)oParser.parse(new FileReader(args_1));

//                    param.put("g-edoc-type", (String)oPar.get("g-edoc-type"));
//                    param.put("g-edoc-ownr", (String)oPar.get("g-edoc-ownr"));
//                    param.put("g-edoc-srcd", (String)oPar.get("g-edoc-srcd"));
//                    param.put("g-edoc-srno", (String)oPar.get("g-edoc-srno"));
//                    param.put("g-edoc-scnr", (String)oPar.get("g-edoc-scnr"));
//
//                    file =  new File((String) oPar.get("g-edoc-file"));
//                    param.put("g-edoc-imge", encodeFileToBase64Binary(file));
//                    param.put("g-edoc-file", file.getName());
//                    param.put("g-edoc-hash", MiscReplUtil.md5Hash((String) oPar.get("g-edoc-file")));
                    
                    break;
                default: 
                    System.out.println("UploadEDocFile <acess_token> <json_info>");
                    System.exit(1);
            }

            
            param.put("g-edoc-type", (String)oPar.get("g-edoc-type"));
            param.put("g-edoc-ownr", (String)oPar.get("g-edoc-ownr"));
            param.put("g-edoc-srcd", (String)oPar.get("g-edoc-srcd"));
            param.put("g-edoc-srno", (String)oPar.get("g-edoc-srno"));
            param.put("g-edoc-scnr", (String)oPar.get("g-edoc-scnr"));

            file =  new File((String) oPar.get("g-edoc-file"));
            param.put("g-edoc-imge", encodeFileToBase64Binary(file));
            param.put("g-edoc-file", file.getName());
            param.put("g-edoc-hash", MiscReplUtil.md5Hash((String) oPar.get("g-edoc-file")));
            
            
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

            System.out.println(response);
            
            oPar.putAll((JSONObject) oParser.parse(response));
            OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(args_1),"UTF-8");
            out.write(oPar.toJSONString());
            out.flush();
            out.close();

        } catch (ParseException ex) {
            //Logger.getLogger(UploadEDocFile.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        } catch (FileNotFoundException ex) {
            //Logger.getLogger(UploadEDocFile.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        } catch (IOException ex) {
            //Logger.getLogger(UploadEDocFile.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        } catch (Exception ex) {
            //Logger.getLogger(UploadEDocFile.class.getName()).log(Level.SEVERE, null, ex);
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