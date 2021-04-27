/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.client;

import java.io.*;
import java.net.*;
import java.util.Calendar;

/**
 *
 * @author kalyptus
 */
public class UploadServer extends Thread{
     public static final int PORT = 9207; 

     ServerSocket serverSocket = null;
     Socket clientSocket = null;

    public void run() {
        try {
            // Create the server socket
            serverSocket = new ServerSocket(PORT, 1);
            while (!Thread.currentThread().isInterrupted()) {
             // Wait for a connection
             clientSocket = serverSocket.accept();
             // System.out.println("*** Got a connection! ");
             clientSocket.close();
            }
        }
        catch (IOException ioe) {
            System.out.println("Error in UploadServer: " + ioe);
        }
    }       
    
    protected void finalize() throws Throwable {
        System.out.println("Starting to finalize UploadServer.java: " + Calendar.getInstance().getTime());
    }
}
