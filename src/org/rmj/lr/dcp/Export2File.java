package org.rmj.lr.dcp;

import org.rmj.appdriver.agent.GRiderX;
import org.rmj.replication.utility.LogWrapper;

/**
 *
 * @author mac
 */
public class Export2File {
    public static void main(String [] args){
        LogWrapper logwrapr = new LogWrapper("DCP.Export2File", "temp/dcp.log");
        
        if (args.length <= 0){
            logwrapr.severe("Invalid parameter detected.");
            System.exit(1);
        }
        
        if (args[0].isEmpty()){
            logwrapr.severe("Invalid parameter detected.");
            System.exit(1);
        }
        
        GRiderX poGRider = new GRiderX("IntegSys");
        
        if(!poGRider.getErrMsg().isEmpty()){
            logwrapr.severe(poGRider.getErrMsg());
            logwrapr.severe("GRiderX has error...");
            System.exit(1);
        }
        
        poGRider.setOnline(false);
        
        if (DCPUtil.Export2File(poGRider, args[0], logwrapr)){
            logwrapr.info("File exported successfully. - " + args[0]);
            System.exit(0);
        } else
            System.exit(1);
        
        
     
    }
}
