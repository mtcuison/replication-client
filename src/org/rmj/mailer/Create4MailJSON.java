
package org.rmj.mailer;

import java.io.FileWriter;
import java.io.IOException;
import org.json.simple.JSONObject;
import org.rmj.lib.net.MiscReplUtil;

/**
 *
 * @author Mac
 */
public class Create4MailJSON {
    public static void main(String [] args){
        String mailinfo = "d:/GGC_Java_Systems/temp/mailinfo.json";
        
        JSONObject loJSON = new JSONObject();
        loJSON.put("to", "xurpas7@gmail.com");
        loJSON.put("from", "Guanzon App");
        loJSON.put("subject", "Sample Raw Mail");
        loJSON.put("body", "The quick brown fox jumps over the lazy dog.");
        
        loJSON.put("filename1", "d:/COL Webinar - 2020 03 20 - Investing in Today_s Market Volatility 1 - Slides.pdf");
        loJSON.put("filename2", "d:/Crystal Reports ActiveX Designer - PayrollSummary-REG.pdf");
        
        
        if (MiscReplUtil.fileDelete(mailinfo))
            System.out.println("Mail info file was deleted successfully.");
        else
            System.err.println("Unable to delete mail info.");
        
        
        MiscReplUtil.fileWrite(mailinfo, loJSON.toJSONString());
        
        if (MiscReplUtil.fileExists(mailinfo))
            System.out.println("Mail info file was created successfully.");
        else
            System.err.println("Unable to create mail info.");
        
    }
    
    public static void fileWrite(String filename, String data, boolean append){
        try {
            FileWriter writer = new FileWriter(filename, append);
            writer.write(append == true ? "\n" + data : "" + data);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
   }
}
