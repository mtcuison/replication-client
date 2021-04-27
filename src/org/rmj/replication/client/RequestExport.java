/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.client;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.MiscReplUtil;
import static org.rmj.lib.net.MiscReplUtil.RStoLinkList;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.appdriver.agent.MsgBox;

public class RequestExport {
    private static String SIGNATURE = "08220326";
    private static LogWrapper logwrapr = new LogWrapper("Client.RequestCreate", "temp/RequestCreate.log");
    private static GRiderX instance = null;

    public static void main(String[] args){
        instance = new GRiderX("gRider");

        if(!instance.getErrMsg().isEmpty()){
            MsgBox.showOk(instance.getMessage() + instance.getErrMsg());
            return;
        }
        
        instance.setOnline(false);
        
        
        System.out.println("arguments [0]: " + args[0]);
        System.out.println("Memory Size: " + Runtime.getRuntime().totalMemory());
        
        String batch = instance.getBranchCode() + MiscReplUtil.format(Calendar.getInstance().getTime(), "yyyyMMddHHmmss");
        
        if(init_export(batch, args[0])){
            if(create_export(batch, args[0], "0"))
                System.out.println("Export " + batch + " was successfully created...");
            else
                System.out.println("Unable to create an export...");
        }
    }  
    
    private static boolean init_export(String batch, String filename){
        ResultSet loRS = null;
        Boolean bErr=false;
        JSONParser parser = new JSONParser();
        String sClientID=null;
        JSONArray list = new JSONArray();
        String lsSQL = null;
        try {
            JSONArray a = (JSONArray) parser.parse(new InputStreamReader(new FileInputStream(filename + ".jbx"), "UTF-8"));

            for (Object o : a){
                JSONObject json = (JSONObject) o;
                sClientID = (String) json.get("sClientID");
                
                lsSQL = "SELECT sTransNox" +
                       " FROM xxxReplicationLog" +
                       " WHERE sTransNox LIKE " + SQLUtil.toSQL(sClientID.substring(sClientID.length()- 4) + "%") +
                       " ORDER BY sTransNox DESC" +
                       " LIMIT 1";
                loRS = instance.getConnection().createStatement().executeQuery(lsSQL);
                if(loRS.next())
                    json.put("sExportTr", loRS.getString("sTransNox"));
                else
                    json.put("sExportTr", "");
                
                list.add(json);
                        
            }
        
            System.out.println("Writing collected info to file " + batch + ".jbc");
            OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(batch + ".jbc"),"UTF-8");
            out.write(list.toJSONString());
            out.flush();
            out.close();
            
        } catch (IOException ex) {
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        } catch (ParseException ex) {
            logwrapr.severe("ParseException error detected.", ex);
            bErr = true;
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }
    
    private static boolean create_export(String batch, String filename, String ismain){
        ResultSet rs;
        
        //Extract records to be exported
        rs = extract_record(batch, filename, ismain); 
        if(rs == null) 
            return false;
        else{
            //Save extracted record as json
            if(!write_file(rs, batch)) return false;
        }
        return true;
    }
    
    private static ResultSet extract_record(String batch, String filename, String ismain){
        ResultSet rs = null;
        
        try {
            String lsFilter = getFilter(batch);
            String lsSQL = "";
            
            if(ismain.equals("1")){
                lsSQL = " SELECT a.*" +
                        " FROM xxxReplicationLog a" +
                        " WHERE (" + lsFilter + ")" +
                        " ORDER BY a.dModified, a.sTransNox  LIMIT 50000";
            }
            else{
                lsSQL = " SELECT a.* " +
                        " FROM xxxReplicationLog a, xxxSysTable b" +
                        " WHERE (" + lsFilter + ")" +
                          " AND a.sTableNme = b.sTableNme" +
                          " AND ((b.cTableTyp = '0')" +
                             " OR (b.cTableTyp = '2' AND a.sDestinat = " + SQLUtil.toSQL(filename.substring(0, 4)) + ")" +
                             " OR (b.cTableTyp = '1' AND a.sBranchCd = " + SQLUtil.toSQL(filename.substring(0, 4)) + ")" +
                             " OR (b.cTableTyp = '3' AND a.sBranchCd LIKE " + SQLUtil.toSQL(filename.substring(0, 1) + "%") + ")" +
                             " OR (a.sBranchCD = " + SQLUtil.toSQL(filename.substring(0, 4)) + " AND a.sTransNox NOT LIKE " + SQLUtil.toSQL(filename.substring(0, 4) + "%") + "))" +
                        " ORDER BY a.dModified, a.sTransNox";
            }
            
            System.out.println(lsSQL);
            rs = instance.getConnection().createStatement().executeQuery(lsSQL);
            
        } 
        catch (SQLException ex) {
            logwrapr.severe("IOException error detected.", ex);
        }
        
        return rs;
    }
    
//    private static boolean write_file(ResultSet rs, String filename){
//        boolean bErr = false;
//        try {
//            
//            OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(filename + ".jbr"),"UTF-8");
//            System.out.println("Writing file...");
//            out.write(MiscReplUtil.RStoJSON(rs));
//            out.flush();
//            out.close();
//            System.out.println("Wrote successfully...");
//            
//        } catch(IOException ex){
//            logwrapr.severe("IOException error detected.", ex);
//            bErr = true;
//        } catch (SQLException ex) {
//            logwrapr.severe("SQLException error detected.", ex);
//            bErr = true;
//        }
//        
//        return !bErr;
//    }
    
    private static boolean write_file(ResultSet rs, String filename){
        boolean bErr = false;
        try {
            OutputStream out = new FileOutputStream(filename + ".jbr");
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));

            LinkedList<Map<String, String>> link = RStoLinkList(rs);
            
            writer.beginArray();
            
            //SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat sf = new SimpleDateFormat("MMM d, yyyy K:m:s");
            
            for(Map map : link){
               writer.beginObject();
               writer.name("sTransNox").value((String)map.get("sTransNox"));
               writer.name("sBranchCd").value((String)map.get("sBranchCd"));
               writer.name("sStatemnt").value((String)map.get("sStatemnt"));
               writer.name("sTableNme").value((String)map.get("sTableNme"));
               writer.name("sDestinat").value((String)map.get("sDestinat"));
               writer.name("sModified").value((String)map.get("sModified"));
               writer.name("dEntryDte").value(sf.format((Date)map.get("dEntryDte")));
               writer.name("dModified").value(sf.format((Date)map.get("dModified")));
               writer.endObject();
            }
            
            writer.endArray();
            writer.close();
            
        } catch(IOException ex){
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }    
    
    private static String getFilter(String filename){
        StringBuilder lsFilter = new StringBuilder();
        boolean bErr = false;
        JSONParser parser = new JSONParser();
        String sClientID=null;
        try {
            JSONArray a = (JSONArray) parser.parse(new InputStreamReader(new FileInputStream(filename + ".jbc"), "UTF-8"));

            for (Object o : a){

                JSONObject json = (JSONObject) o;
                sClientID = (String)json.get("sClientID");
                lsFilter.append(" OR (a.sTransNox LIKE " + SQLUtil.toSQL(sClientID.substring(sClientID.length()- 4) + "%") + " AND a.sTransNox BETWEEN " + SQLUtil.toSQL((String)json.get("sExportFr")) + " AND " + SQLUtil.toSQL((String)json.get("sExportTr")) + ")");
            }
            
        } catch (IOException ex) {
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        } catch (ParseException ex) {
            logwrapr.severe("ParseException error detected.", ex);
            bErr = true;
        }

        if(bErr)
            return "";
        else
            return lsFilter.toString().substring(4);
    }
}
