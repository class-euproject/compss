package es.bsc.compss.nio.master.starters.json;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Map;


public class NetworkSettings {

    private @SerializedName("Ports") Map<String, List<PortSettings>> ports;


    public Map<String, List<PortSettings>> getPorts() {
        return ports;
    }

    public void setPorts(Map<String, List<PortSettings>> ports) {
        this.ports = ports;
    }

}
