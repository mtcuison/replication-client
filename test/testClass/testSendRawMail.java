/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testClass;

import org.rmj.mailer.SendMail;

/**
 *
 * @author Mac
 */
public class testSendRawMail {
    public static void main(String [] args){
        String [] argx = new String [2];

        argx[0] = "access";
        argx[1] = "mailinfo";
        SendMail.main(argx);
    }
}
