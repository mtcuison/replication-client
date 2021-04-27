/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.client;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import org.rmj.lib.net.MiscReplUtil;

/**
 *
 * @author sayso
 */
public class TarFile {
    public static void main(String[] args){
        int nErr = 0;
        
        String filename = "D:/M00120181012123640M118.json";
        String output = "D:/";
        if(args.length == 2){
            filename = args[0];
            output = args[1];
        }
        else if(args.length == 1){
            filename = args[0];
            output = new File(filename).getParent();
        }
        
        try {
            MiscReplUtil.tar( filename, output);
        } catch (IOException ex) {
            ex.printStackTrace();
            nErr = 1;
        }
        
        System.exit(nErr); 
    }
}
