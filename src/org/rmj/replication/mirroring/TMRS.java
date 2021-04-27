package org.rmj.replication.mirroring;

import org.rmj.appdriver.agent.GRiderX;

public interface TMRS {
    public void setGRider(GRiderX foGRider);
    public void setTransNox(String fsTransNox);
    public void setTranStat(String fcTranStat);
    
    public boolean Download();
    public boolean Upload();
    
    public String getMessage();
}