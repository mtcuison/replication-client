package org.rmj.replication.mirroring;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.appdriver.constants.TransactionStatus;
import org.rmj.replication.utility.MiscReplUtil;

public class LR_DCP implements TMRS{
    private final String API_UPLOAD = "integsys/dcp/dcp_upload.php";
    private final String API_DOWNLOAD = "integsys/dcp/dcp_android_download.php";
    private final String APP_LOG = "D:/GGC_Java_Systems/temp/dcp_android.tmp";
    
    private final String MASTER_TABLE = "LR_DCP_Collection_Master";
    private final String DETAIL_TABLE = "LR_DCP_Collection_Detail";
    private final String OTHERS_TABLE = "LR_DCP_Collection_Others";
    
    GRiderX instance = null;
    String transnox = "";
    String transtat = null;
    
    String messagex = "";
    
    @Override
    public void setGRider(GRiderX foGRider) {
        instance = foGRider;
    }

    @Override
    public void setTransNox(String fsTransNox) {
        transnox = fsTransNox;
    }
    
    @Override
    public void setTranStat(String fcTranStat) {
        transtat = fcTranStat;
    }

    @Override
    public boolean Download() {
        if (instance == null){
            messagex = "Application driver is not set.";
            return false;
        }
        
        if (transnox.trim().isEmpty()){
            messagex = "Transaction number must not be empty.";
            return false;
        }
        
        JSONObject param = new JSONObject();
        param.put("sTransNox", transnox.trim());
        
        param = RestAPI.REQUEST(instance, API_DOWNLOAD, param);
        
        MiscReplUtil.fileWrite(APP_LOG, param.toJSONString());
        
        return true;
    }

    @Override
    public boolean Upload() {
        if (instance == null){
            messagex = "Application driver is not set.";
            return false;
        }
        
        if (transnox.trim().isEmpty()){
            messagex = "Transaction number must not be empty.";
            return false;
        }
        
        switch(transtat){
            case TransactionStatus.STATE_CLOSED:
                return closeTransaction();
            default:
                messagex = "Transaction status is not for upload.";
                return false;
        }
    }

    @Override
    public String getMessage() {
        return messagex;
    }
    
    private boolean closeTransaction(){
        String lsSQL;
        ResultSet loRS;
        
        JSONObject loMaster;
        JSONObject loJSON;
        JSONArray loDetail;
        
        try {
            //check if transaction exists
            lsSQL = "SELECT * FROM " + MASTER_TABLE +
                    " WHERE sTransNox = " + SQLUtil.toSQL(transnox);
            loRS = instance.executeQuery(lsSQL);

            if (loRS.next()){  
                //assign the master record to json object
                loMaster = new JSONObject();
                loMaster.put("sTransNox", loRS.getString("sTransNox"));
                loMaster.put("dTransact", loRS.getString("dTransact"));
                loMaster.put("sReferNox", loRS.getString("sReferNox"));
                loMaster.put("dReferDte", loRS.getString("dReferDte"));
                loMaster.put("cTranStat", "1");
                loMaster.put("cDCPTypex", "1");
                loMaster.put("sCollctID", loRS.getString("sCollctID"));
                loMaster.put("nEntryNox", 0);
                loMaster.put("sModified", loRS.getString("sModified"));
                loMaster.put("dModified", loRS.getString("dModified"));
                
                //assign the records to json array
                loDetail = new JSONArray();
                //get the detail records
                lsSQL = "SELECT * FROM " + DETAIL_TABLE +
                        " WHERE sTransNox = " + SQLUtil.toSQL(transnox);
                loRS = instance.executeQuery(lsSQL);
                while (loRS.next()){
                    loJSON = new JSONObject();
                    loJSON.put("sTransNox", loRS.getString("sTransNox"));
                    loJSON.put("nEntryNox", loRS.getInt("nEntryNox"));
                    loJSON.put("sAcctNmbr", loRS.getString("sAcctNmbr"));
                    loJSON.put("sReferNox", "");
                    loJSON.put("cPaymForm", loRS.getString("cPaymForm"));
                    loJSON.put("dPromised", null);
                    loJSON.put("sRemCodex", null);
                    loJSON.put("sRemarksx", "");
                    loJSON.put("cIsDCPxxx", "1");
                    loJSON.put("cIsNwNmbr", "0");
                    loJSON.put("cIsNwAddx", "0");
                    loJSON.put("cIsNwCltx", "0");
                    loJSON.put("dModified", loRS.getString("dModified"));
                    loDetail.add(loJSON);
                }               
            } else {
                messagex = "Transaction given does not exist.";
                return false;
            }

            JSONObject param = new JSONObject();
            param.put("master", loMaster);
            param.put("detail", loDetail);

            param = RestAPI.REQUEST(instance, API_UPLOAD, param);

            if (!"success".equals((String) param.get("result"))){
                param = (JSONObject) param.get("error");
                messagex = param.toJSONString();
                return false;
            }
        } catch (SQLException ex) {
            messagex = ex.getMessage();
            return false;
        }
        
        
        return true;
    }
}