package org.rmj.mailer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.net.WebClient;
import org.rmj.xapitoken.util.RequestAccess;

/**
 *
 * @author Mac
 * @since 2021.04.29
 */
public class SendMail {
    public static void main(String [] args){
        final String sURL = "https://restgk.guanzongroup.com.ph/x-api/v1.0/mail/sendrawmail.php";
        final String sAccess = "access";
        
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
    
        JSONObject param = new JSONObject();
        JSONParser oParser = new JSONParser();
        JSONObject oJson;
        
        String sTokenName = ""; //filename of access token
        
        try {
            File filex;
            switch(args.length){
                case 0:
                    sTokenName = sAccess;
                    param.put("to", "xurpas7@gmail.com");
                    param.put("subject", "Send Raw Mail");
                    param.put("body", "This is a sample email using sendrawmail.php");
                    filex =  new File("D:/COL Webinar - 2020 03 20 - Investing in Today_s Market Volatility 1 - Slides.pdf");
                    param.put("data1", encodeFileToBase64Binary(filex));
                    param.put("filename1", filex.getName());
                    break;
                case 2:    
                    sTokenName = args[0];
                    param = (JSONObject) oParser.parse(new FileReader(System.getProperty("sys.default.path.temp") + "/" + args[1] + ".json"));
                    
                    int lnCtr = 1;
                    while (param.containsKey("filename" + String.valueOf(lnCtr))){
                        filex =  new File((String) param.get("filename" + String.valueOf(lnCtr)));
                        param.put("data" + String.valueOf(lnCtr), encodeFileToBase64Binary(filex));
                        param.put("filename" + String.valueOf(lnCtr), filex.getName());
                        lnCtr++;
                    }
                    
                    break;
                default: 
                    System.out.println("TestUploadImageSource <acess_token> <json_info>");
                    System.exit(1);
            }
        
            //Load token
            oJson = (JSONObject)oParser.parse(new FileReader(System.getProperty("sys.default.path.config") + "/" + sTokenName + ".token"));
            Calendar current_date = Calendar.getInstance();
            current_date.add(Calendar.MINUTE, -25);
            Calendar date_created = Calendar.getInstance();
            date_created.setTime(SQLUtil.toDate((String) oJson.get("created") , SQLUtil.FORMAT_TIMESTAMP));
            
            //Check if token is still valid within the time frame
            //Request new access token if not in the current period range
            if(current_date.after(date_created)){
                String[] xargs = new String[] {(String) oJson.get("parent")};
                RequestAccess.main(xargs);
                oJson = (JSONObject)oParser.parse(new FileReader(System.getProperty("sys.default.path.config") + "/" + sTokenName + ".token"));
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
        } catch (ParseException ex) {
            ex.printStackTrace();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
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
