
package org.rmj.replication.client;

import java.io.IOException;
import java.net.MalformedURLException;
import org.json.simple.JSONObject;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GProperty;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.MiscReplUtil;
import org.rmj.lib.net.SFTP_DU;
import org.rmj.lib.net.WebClient;
import org.rmj.appdriver.agent.GRiderX;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author kalyptus
 */
public class SMSCastDeliveryNotification {
    private static String SIGNATURE = "08220326";
    private static String GUANZON_SITE = null; 
    private static LogWrapper logwrapr = new LogWrapper("SMSCastDeliveryNotification.Download", "temp/SMSCastDeliveryNotification.log");
    private static String host_dir = null;
    private static GRiderX instance = null;

    public static void main(String[] args) {
        
        //load FTP information
        prepareSFTPHost();
        post_result();
        incoming_message();
   }
  
    private static void prepareSFTPHost(){
        GCrypt loEnc = new GCrypt(SIGNATURE);
        GProperty loProp = new GProperty("ReplicaXP");
        
        String sWebHost[] = loProp.getConfig("websrvcs").split(";");
        for(int x=0;x < sWebHost.length;x++){
           if(WebClient.isHttpURLConOk(sWebHost[x])){
               GUANZON_SITE = sWebHost[x];
               x = sWebHost.length;
           }    
        }        
    }    
    
    private static String post_result(){
        String success = "";
        //(*)inform app server that upload of file is successful...
        try {
            success = WebClient.httpGet(GUANZON_SITE + "/msgcast/status.php?msgid=1&msisdn=09175432929&msgcount=5&dateprocessed=20150608013654&status=20320" );
            
        } catch (MalformedURLException ex) {
            success = "";
            logwrapr.severe("MalformedURLException error detected.", ex);
        } catch (IOException ex) {
            success = "";
            logwrapr.severe("IOException error detected.", ex);
        }
        
        return success;
    }    
    
    private static String incoming_message(){
        String success = "";
        //(*)inform app server that upload of file is successful...
        try {
            System.out.println("Go in");
            success = WebClient.httpGet(GUANZON_SITE + "/msgcast/incoming.php?from=1234567890&to=987654321&ts=20150609114001&msg=Subuking%20lang%20po%20natin%20kung%20okey%20ito" );
            System.out.println(success);
            
        } catch (MalformedURLException ex) {
            success = "";
            logwrapr.severe("MalformedURLException error detected.", ex);
        } catch (IOException ex) {
            success = "";
            logwrapr.severe("IOException error detected.", ex);
        }
        
        return success;
    }        
}

