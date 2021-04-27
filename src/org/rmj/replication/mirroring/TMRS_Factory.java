package org.rmj.replication.mirroring;

public class TMRS_Factory {   
    public static TMRS make(String fsSourceCd){
        switch(fsSourceCd){
            case "DCPx":
                return new LR_DCP();
            default:
                return null;
        }
    }
}
