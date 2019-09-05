package es.bsc.compss.nio.master.configuration.rotterdam.response;

import java.util.ArrayList;
import java.util.List;


public class RotterdamTaskCheckDefinition extends RotterdamTaskCreateResponseTask {

    private List<PodConfig> pods;
    private String masterip;


    /**
     * Representation of the different new fields received in the response body when checking whether a task is
     * correctly deployed in Rotterdam.
     * 
     * @param name Name of the task
     * @param replicas Amount of replicas
     */
    public RotterdamTaskCheckDefinition(String name, Integer replicas) {
        super(name, replicas);
    }

    /**
     * Returns the list of pods if any and creates an empty list if not initialized to avoid NullPointerExceptions.
     * 
     * @return List of pods
     */
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
