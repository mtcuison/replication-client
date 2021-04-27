/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.client;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.MiscReplUtil;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.appdriver.agent.MsgBox;

/**
 * Note:
 *     In this utility I uses RIGHT(sClientID, 4) for compatibility with
 *     the Repo - Warehouse with a ClientID of GGC_BM0R1 and branch code of M001
 */
public class RequestCreate {
    private static String SIGNATURE = "08220326";
    private static LogWrapper logwrapr = new LogWrapper("Client.RequestCreate", "temp/RequestCreate.log");
    private static GRiderX instance = null;

    public static void main(String[] args) throws SQLException, FileNotFoundException, UnsupportedEncodingException, IOException{
        
        instance = new GRiderX("gRider");
        
        if(!instance.getErrMsg().isEmpty()){
            MsgBox.showOk(instance.getMessage() + instance.getErrMsg());
            return;
        }

        instance.setOnline(false);
        
        String sExportCD = instance.getClientID().substring(instance.getClientID().length() - 4)+ MiscReplUtil.format(Calendar.getInstance().getTime(), "yyMMddHHmmss");
        
        String lsSQL = "SELECT" +
                              "  a.sClientID" +
                              ", a.sClientNm" +
                              ", a.sExportNo" +
                              ", a.sBranchCd" + 
                      " FROM xxxSysClient a" +
                          ", Branch b" +
                      " WHERE a.sBranchCD = b.sBranchCd" +
                        " AND b.cRecdStat = '1'" +
                        " AND a.sClientID <> " + SQLUtil.toSQL(instance.getClientID());               
        ResultSet rsbranch = instance.getConnection().createStatement().executeQuery(lsSQL);

        JSONArray json_arr = new JSONArray();
        JSONObject json_obj = null;

        System.out.println("Extracting last log for each branch...");
        while(rsbranch.next()){
            String sClientID = rsbranch.getString("sClientID");
            
            lsSQL = "SELECT" +
                           "  sTransNox" +
                   " FROM xxxReplicationLog" +
                   " WHERE sTransNox LIKE " + SQLUtil.toSQL(sClientID.substring(sClientID.length()- 4) + "%") +
                   " ORDER BY sTransNox DESC" +
                   " LIMIT 1";
            ResultSet rslog = instance.getConnection().createStatement().executeQuery(lsSQL);

            json_obj = new JSONObject();
            json_obj.put("sExportCd", sExportCD);
            json_obj.put("sClientID", rsbranch.getString("sClientID"));
            
            if(rslog.next())
                json_obj.put("sExportFr", rslog.getString("sTransNox"));
            else
                json_obj.put("sExportFr", "");
            
            json_arr.add(json_obj);
        }
        
        System.out.println("Writing collected info to file " + sExportCD + ".jbx");
        OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(sExportCD + ".jbx"),"UTF-8");
        out.write(json_arr.toJSONString());
        out.flush();
        out.close();
    }    
}
