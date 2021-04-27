
package org.rmj.replication.mirroring;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.replication.utility.LogWrapper;

public class Upload {   
    public static void main(String args[]){
        LogWrapper logwrapr = new LogWrapper("Repl.Upload", "repl-mirroring.log");
        
        String path;
        if(System.getProperty("os.name").toLowerCase().contains("win")){
            path = "D:/GGC_Java_Systems";
        }
        else{
            path = "/srv/GGC_Java_Systems";
        }
        System.setProperty("sys.default.path.config", path);
        
        GRiderX instance = new GRiderX("IntegSys");
        
        if(!instance.getErrMsg().isEmpty()){
            logwrapr.severe(instance.getErrMsg());
            logwrapr.severe("GRiderX has error...");
            System.exit(1);
        }
        
        instance.setOnline(false);
        
        TMRS loMirror;
        
        String lsSQL = "SELECT" +
                            "  sTransNox" +
                            ", sSourceNo" +
                            ", sSourceCD" +
                            ", cTranStat" +
                        " FROM TMRS_Log" +
                        " WHERE sTransNox LIKE " + SQLUtil.toSQL(instance.getBranchCode() + "%") +
                            " AND cSendStat = '0'" +
                        " ORDER BY sTransNox";
        ResultSet loRS = instance.executeQuery(lsSQL);
        
        try {
            while(loRS.next()){
                loMirror = TMRS_Factory.make(loRS.getString("sSourceCD"));
                loMirror.setGRider(instance);
                
                loMirror.setTransNox(loRS.getString("sSourceNo"));
                loMirror.setTranStat(loRS.getString("cTranStat"));
                
                if (loMirror.Upload()){
                    logwrapr.info(loRS.getString("sTransNox") + "(" + loRS.getString("cTranStat") + ") - " +  "uploaded successfully.");
                } else {
                    logwrapr.severe(loRS.getString("sTransNox") + "(" + loRS.getString("cTranStat") + ") - " + loMirror.getMessage());
                    System.exit(1);
                }
                
                lsSQL = "UPDATE TMRS_Log SET" +
                            "  cSendStat = '1'" +
                            ", dSentDate = " + SQLUtil.toSQL(instance.getServerDate()) +
                        " WHERE sTransNox = " + SQLUtil.toSQL(loRS.getString("sTransNox"));
                
                if (instance.executeQuery(lsSQL, "TMRS_Log", instance.getBranchCode(), "") == 0){
                    logwrapr.severe(loRS.getString("sTransNox") + "(" + loRS.getString("cTranStat") + ") - " +  instance.getErrMsg() + "; " + instance.getMessage());
                    System.exit(1);
                }
                
                loMirror = null;
            }
        } catch (SQLException ex) {
            logwrapr.severe(ex.getMessage());
            System.exit(1);
        }
        
        System.out.println("Thank you.");
        System.exit(0);
    }
}
