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
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.lib.net.LogWrapper;

/**
 *
 * @author sayso
 */
public class CreateSlave {
    private static String SIGNATURE = "08220326";
    private static String GUANZON_SITE = null; 
    private static LogWrapper logwrapr = new LogWrapper("binlog.util.CreateMaster", "temp/CreateMaster.log");
    private static GRiderX instance = null;
    private static String psDBNameXX;
    private static String psDBMstrNm;
    private static String psDBSlveNm;
    private static String psDBUserNm;
    private static String psDBPassWD;
    private static String psDBPortNo;
    private static int pnHexCrypt;
    private static GConnection gcon;

    private static long pnLogPosxx;
    private static String psLogFilex;
    private static String psReplUser;
    private static String psReplPswd;
    
    public static void main(String[] args) throws SQLException {
        System.out.println("Loading server configuration...");
        if(!loadConfig()){
            System.out.println("Please make sure that Binlog.poperties is inside D:/GGC_Java_Systems/config");
            System.exit(0);
        }

        System.out.println("Loading binary log configuration...");
        if(!loadMasterConfig()){
            System.out.println("Please make sure that Binlog-Init.poperties is inside D:/GGC_Java_Systems/config");
            System.exit(0);
        }
        
        System.out.println("Connecting to the Slave server...");
        gcon = new GConnection();
        gcon.setupDataSource(psDBSlveNm, psDBNameXX, psDBUserNm, psDBPassWD, psDBPortNo);
        gcon.doConnect();


        System.out.println("Setting Up the Slave server...");
        //gcon.executeQuery("STOP SLAVE");
        
        String sql = "CHANGE MASTER TO" +
                "  MASTER_HOST=" + SQLUtil.toSQL(psDBMstrNm) +
                ", MASTER_USER=" + SQLUtil.toSQL(psReplUser) +
                ", MASTER_PASSWORD=" + SQLUtil.toSQL(psReplPswd) +
                ", MASTER_LOG_FILE=" + SQLUtil.toSQL(psLogFilex) +
                ", MASTER_LOG_POS=" + String.valueOf(pnLogPosxx) + ";";
        //gcon.executeQuery(sql);

        //gcon.executeQuery("START SLAVE");

        System.out.print(sql);
        
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

   private static boolean loadMasterConfig(){
      //System.out.println("loadConfig(String fsProductID)");
      //Get configuration values
      try{
         GProperty loProp = new GProperty("D:/GGC_Java_Systems/config/Binlog-Init");
         String pos;
         psLogFilex = loProp.getConfig("org.rmj.binlog.File");
         //System.out.println(psLogFilex);
         psDBMstrNm = loProp.getConfig("org.rmj.binlog.Host");
         //System.out.println(psDBMstrNm);
         psReplUser = Decrypt(loProp.getConfig("org.rmj.binlog.User"));
         //System.out.println(psReplUser);
         psReplPswd = Decrypt(loProp.getConfig("org.rmj.binlog.Password"));
         //System.out.println(psReplPswd);
         pos=loProp.getConfig("org.rmj.binlog.Position");
         //System.out.println(pos);
         pnLogPosxx = Long.parseLong(pos);
         //System.out.println(pnLogPosxx);

      }catch(Exception ex){
         ex.printStackTrace();
         return false;
      }
        
      //Test validity of Results
      if(psLogFilex.equals("") ||
         psDBMstrNm.equals("") ||
         psReplUser.equals("") ||
         psReplPswd.equals("") ||
         pnLogPosxx == 0) {
         return false;
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
