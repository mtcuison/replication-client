package org.rmj.replication.mirroring;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.appdriver.agentfx.CommonUtils;
import org.rmj.appdriver.agentfx.WebClient;

/**
 * RestAPI
 * 
 * Call transaction mirroring APIs from Server
 * 
 * @author mac
 * @since 2021.02.26     
 */
public class RestAPI {   
    public static JSONObject REQUEST(GRiderX foGRider, String fsAPI, JSONObject foParam){
        if (foGRider == null) return sendError("100", "Application driver is not set.");
        
        JSONParser oParser = new JSONParser();
        
        try {
            //String lsAPI = CommonUtils.getConfiguration(foGRider, "WebSvr") + DCP_UPLOAD;
            String lsAPI = CommonUtils.getConfiguration(foGRider, "WebSvr") + fsAPI;
            String response = WebClient.sendHTTP(lsAPI, foParam.toJSONString(), (HashMap<String, String>) getHeader(foGRider));
            if(response == null) return sendError("", System.getProperty("store.error.info"));
            
            return (JSONObject) oParser.parse(response);
        } catch (IOException ex) {
            return sendError("250", "IO Exception: " + ex.getMessage());
        } catch (ParseException ex) {
            return sendError(ex.getErrorType(), "Parse Exception: " + ex.getMessage());
        }    
    }
    
    private static HashMap getHeader(GRiderX foGRider){
        String clientid = foGRider.getClientID();
        String productid = foGRider.getProductID();
        String imei = MiscUtil.getPCName();
        String user = "";
        String log = "";
        
        Calendar calendar = Calendar.getInstance();
        Map<String, String> headers = 
                        new HashMap<String, String>();
        headers.put("Accept", "application/json");
        headers.put("Content-Type", "application/json");
        headers.put("g-api-id", productid);
        headers.put("g-api-imei", imei);
        
        headers.put("g-api-key", SQLUtil.dateFormat(calendar.getTime(), "yyyyMMddHHmmss"));        
        headers.put("g-api-hash", org.apache.commons.codec.digest.DigestUtils.md5Hex((String)headers.get("g-api-imei") + (String)headers.get("g-api-key")));
        headers.put("g-api-client", clientid);    
        headers.put("g-api-user", user);    
        headers.put("g-api-log", log);    
        headers.put("g-char-request", "UTF-8");
        headers.put("g-api-token", "");    
        
        return (HashMap) headers;
    }
    
    private static JSONObject sendError(Object foErrorCde, String fsMessagex){
        JSONObject err_detl = new JSONObject();
        err_detl.put("message", fsMessagex);
        err_detl.put("code", foErrorCde);
        JSONObject err_mstr = new JSONObject();
        err_mstr.put("result", "error");
        err_mstr.put("error", err_detl);
        return err_mstr;
    }
}
