/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.client;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GProperty;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.SFTP_DU;

/**
 *
 * @author kalyptus
 */
public class RestartServer {
   private static LogWrapper logwrapr = new LogWrapper("Client.RestartServer", "temp/RestartServer.log");
   private static String host_dir = null;
   private static SFTP_DU sftp;
   private static String SIGNATURE = "08220326";
 
   public static void main(String[] args) {
      
      if(args.length == 0){
         System.out.println("Please indicate the name of the server to restart...");
         return;
      }
      
      prepareSFTPHost(args[0]);
      try {
         String output = sftp.sendCommand("ls");
         
         System.out.println(output);
         
      } catch (Exception ex) {
         logwrapr.severe("Erro found:", ex);
      }
   }
    private static void prepareSFTPHost(String host){
        GProperty loProp = new GProperty("ReplicaXP");
        
        GCrypt loEnc = new GCrypt(SIGNATURE);
        sftp = new SFTP_DU();
        sftp.setPort(Integer.valueOf(loProp.getConfig("sftpport")));
        sftp.setHost(loProp.getConfig("keypath"));
        
        if(host.equalsIgnoreCase("web")){
           sftp.setUser("root");
           sftp.setHostKey(loProp.getConfig("keypath") + host);
           sftp.setHost(loProp.getConfig(host));
        }
        else if(host.equalsIgnoreCase("tap")){
           sftp.setUser("root");
           sftp.setHostKey(loProp.getConfig("keypath") + host);
           sftp.setHost(loProp.getConfig(host));
        }
        else if(host.equalsIgnoreCase("mail")){
           sftp.setUser("root");
           sftp.setHostKey(loProp.getConfig("keypath") + host);
           sftp.setHost(loProp.getConfig(host));
        }
        else if(host.equalsIgnoreCase("mysql")){
           sftp.setUser("root");
           sftp.setHostKey(loProp.getConfig("keypath") + host);
           sftp.setHost(loProp.getConfig(host));
        }
        else{
           sftp.setUser(loEnc.decrypt(loProp.getConfig("sftpuser")));
           sftp.setPassword(loEnc.decrypt(loProp.getConfig("sftppass")));
           sftp.setHost(loProp.getConfig(host));
        }
    }    
}
