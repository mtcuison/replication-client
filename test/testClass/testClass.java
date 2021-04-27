
package testClass;

import java.util.Date;
import org.json.simple.JSONObject;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.lr.dcp.DCPUtil;
import org.rmj.replication.utility.LogWrapper;


public class testClass {
    public static void main (String [] args){
        LogWrapper logwrapr = new LogWrapper("DCP.Export2File", "dcp.log");
        
        GRiderX poGRider = new GRiderX("IntegSys");
        
        if(!poGRider.getErrMsg().isEmpty()){
            logwrapr.severe(poGRider.getErrMsg());
            logwrapr.severe("GRiderX has error...");
            System.exit(1);
        }
        
        poGRider.setOnline(false);
        
        JSONObject loJSON = DCPUtil.getDelay(poGRider, "M064190218");
        
        if ("success".equals(loJSON.get("result"))){
            System.out.println(loJSON.toJSONString());
            System.exit(0);
        } else System.exit(1);
    }
}
