/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.client;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 *
 * @author kalyptus
 */
public class BackupLogServer extends Thread{
    // you may need to customize this for your machine
     public static final int PORT = 9209; 

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
            System.out.println("Error in BackupLogServer: " + ioe);
        }
    }      
}
