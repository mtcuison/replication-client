/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.binlog.util;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.rmj.appdriver.GConnection;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GProperty;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.lib.net.LogWrapper;

/**
 *
 * @author sayso
 */
public class CreateMaster {
    private static String SIGNATURE = "08220326";
    private static String GUANZON_SITE = null; 
    private static LogWrapper logwrapr = new LogWrapper("binlog.util.CreateMaster", "temp/CreateMaster.log");
    private static GRiderX instance = null;
    private static String psReplUser = "ggcrepladmin";
    private static String psReplPswd = "Tciadsoggc";
    private static String psDBNameXX;
    private static String psDBMstrNm;
    private static String psDBSlveNm;
    private static String psDBUserNm;
    private static String psDBPassWD;
    private static String psDBPortNo;
    private static int pnHexCrypt;
    private static GConnection gcon;
    
    public static void main(String[] args) throws SQLException {
        System.out.println("Loading server configuration...");
        
        if(!loadConfig()){
            System.out.println("Please make sure that Binlog.poperties is inside D:/GGC_Java_Systems/config");
            System.exit(0);
        }

        System.out.println("Connecting to the main server...");
        gcon = new GConnection();
        gcon.setupDataSource(psDBMstrNm, psDBNameXX, psDBUserNm, psDBPassWD, psDBPortNo);
        gcon.doConnect();
        
        //gcon.executeQuery("FLUSH TABLES WITH READ LOCK");
        System.out.println("Checking replication user...");
        ResultSet rs = gcon.executeQuery("SELECT * FROM mysql.user WHERE user='ggcrepladmin' AND host='%'");
        if(!rs.next()){
            ResultSet rsX = gcon.executeQuery("SELECT VERSION() AS vrsn");
            rsX.next();
            
            System.out.println("Creating user");
            if(rs.getFloat("vrsn") < 5.7){
                gcon.executeQuery("GRANT REPLICATION SLAVE ON *.* TO '" + psReplUser + "'@'%' IDENTIFIED BY '" + psReplPswd +  "';");
            }
            else{
                gcon.executeQuery("CREATE USER '" + psReplUser + "'@'%' IDENTIFIED BY '" + psReplPswd + "'");
                gcon.executeQuery("GRANT REPLICATION SLAVE ON * . * TO '" + psReplUser + "'@'%'");
            }
        }

        System.out.println("Checking status of server...");
        rs = gcon.executeQuery("SHOW MASTER STATUS");
        GProperty bin = new GProperty();
        
        if(!rs.next()){
            System.out.println("Please make sure that the server is configured as Replication Server");
            System.exit(0);
        }

        bin.setConfig("org.rmj.binlog.Host", psDBMstrNm);
        bin.setConfig("org.rmj.binlog.User", Encrypt(psReplUser));
        bin.setConfig("org.rmj.binlog.Password", Encrypt(psReplPswd));
        bin.setConfig("org.rmj.binlog.File", rs.getString("File"));
        bin.setConfig("org.rmj.binlog.Position", String.valueOf(rs.getLong("Position")));
        
        bin.save("D:/GGC_Java_Systems/config/Binlog-Init");
        //gcon.executeQuery("UNLOCK TABLES");
        System.out.println("Done...");
        
    }

   private static boolean loadConfig(){
      //System.out.println("loadConfig(String fsProductID)");
      //Get configuration values
      try{
         GProperty loProp = new GProperty("D:/GGC_Java_Systems/config/Binlog");
         
         if(loProp.getConfig("org.rmj.binlog.CryptType") != null){
            pnHexCrypt = Integer.valueOf(loProp.getConfig("org.rmj.binlog.CryptType"));
         }
         else{
            pnHexCrypt = 0;
         }
         
         psDBNameXX = loProp.getConfig("org.rmj.binlog.Database");
         System.out.println(psDBNameXX);
         psDBMstrNm = loProp.getConfig("org.rmj.binlog.Master");
         System.out.println(psDBMstrNm);
         psDBSlveNm = loProp.getConfig("org.rmj.binlog.Slave");
         System.out.println(psDBSlveNm);
            
         if(loProp.getConfig("org.rmj.binlog.UserName") != null){
            psDBUserNm = Decrypt(loProp.getConfig("org.rmj.binlog.UserName"));
            psDBPassWD = Decrypt(loProp.getConfig("org.rmj.binlog.Password"));
         }
         else{
            psDBUserNm = "";
            psDBPassWD = "";
         }

         psDBPortNo = loProp.getConfig("org.rmj.binlog.Port");

      }catch(Exception ex){
         ex.printStackTrace();
         return false;
      }
        
      //Test validity of Results
      if(psDBNameXX.equals("") ||
         psDBMstrNm.equals("") ||
         psDBSlveNm.equals("")) {
         return false;
      }

      if (psDBPortNo == null || psDBPortNo.equals("")){
         psDBPortNo = "3306";
      }

      return true;
   }

   private static String Encrypt(String value, String salt){
      if(value == null || value.trim().length() == 0 || salt == null || salt.trim().length() == 0)
         return null;
    
      try {
         GCrypt loCrypt = new GCrypt(salt.getBytes("ISO-8859-1"));
         byte[] ret = loCrypt.encrypt(value.getBytes("ISO-8859-1"));
         
         if(pnHexCrypt == 1){
            return Hex.encodeHexString(ret);
         }
         else{
            return new String(ret, "ISO-8859-1");
         }
      } catch (UnsupportedEncodingException e) {
         e.printStackTrace();
         return null;
      }
   }

   /**
    * Encrypts a string value.
    * <p>
    * Note: The returned encrypted value is a hexadecimal string.
    * 
    * @param value   the value to encrypt.
    * @return        the encrypted value of the value.
    * @see Decrypt
    */
   private static String Encrypt(String value) {
      return Encrypt(value, SIGNATURE);
   }
   
   /**
    * Decrypts the encrypted value.
    * <p>
    * Note: The encrypted value is a hexadecimal string.
    * 
    * @param value   the value to decrypt.
    * @param salt    the salt value to be used during decryption.
    * @return        the decrypted value of value.
    * @see Encrypt
    */
   public static String Decrypt(String value, String salt) {
      if(value == null || value.trim().length() == 0 || salt == null || salt.trim().length() == 0)
               return null;

      byte[] hex;
      try {
         if(pnHexCrypt == 1){
            try {
               hex = Hex.decodeHex(value);
            } catch (DecoderException e1) {
               return null;
            }
         }
         else{
            hex = value.getBytes("ISO-8859-1");
         }
         //System.out.println(new String(hex, "ISO-8859-1"));
         //System.out.println(value);
         //remove this part if returning the new logic...
         GCrypt loCrypt = new GCrypt(salt.getBytes("ISO-8859-1"));
         byte ret[] = loCrypt.decrypt(hex);

         return new String(ret, "ISO-8859-1");
      } catch (UnsupportedEncodingException e) {
         e.printStackTrace();
         return null;
      }
   }
   
   /**
    * Decrypts the encrypted value.
    * <p>
    * Note: The encrypted value is a hexadecimal string.
    * 
    * @param value   the envalue to decrypt
    * @return        the decrypted value of value.
    * @see Encrypt
    */
   private static String Decrypt(String value){
      return Decrypt(value, SIGNATURE);
   }
}
