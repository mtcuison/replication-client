package testClass;



import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.rmj.appdriver.SQLUtil;

public class APIParam {
    public static HashMap getHeader1(){
        String clientid = "GGC_BM001";
        String productid = "IntegSys";
        String imei = "8f1815dc5a4eef1e";
        String user = "GAP0190004";
        String log = "GAP021012864";
        
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
        headers.put("g-api-mobile", "09260375777");    
        headers.put("g-char-request", "UTF-8");
        headers.put("g-api-token", "");    
        
        return (HashMap) headers;
    }
    
    public static HashMap getHeader2(){
        String clientid = "GGC_BM001";
        String productid = "IntegSys";
        String imei = "GMC_SEG01";
        String user = "M001111122";
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
}
