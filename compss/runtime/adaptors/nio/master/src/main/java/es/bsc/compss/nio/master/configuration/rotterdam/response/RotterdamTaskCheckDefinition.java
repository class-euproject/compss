package es.bsc.compss.nio.master.configuration.rotterdam.response;

import java.util.ArrayList;
import java.util.List;


public class RotterdamTaskCheckDefinition extends RotterdamTaskCreateResponseTask {

    private List<PodConfig> pods;
    private String masterip;


    public RotterdamTaskCheckDefinition(String name, Integer replicas) {
        super(name, replicas);
    }

    public List<PodConfig> getPods() {
        if (this.pods == null) {
            this.pods = new ArrayList<>();
        }
        return this.pods;
    }

    public String getMasterip() {
        return masterip;
    }

    public void setMasterip(String masterip) {
        this.masterip = masterip;
    }
}
