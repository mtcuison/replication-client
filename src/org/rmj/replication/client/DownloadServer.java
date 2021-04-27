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
public class DownloadServer extends Thread{
    // you may need to customize this for your machine
     public static final int PORT = 9206; 

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
            System.out.println("Error in DownloadServer: " + ioe);
        }
    }   
}
