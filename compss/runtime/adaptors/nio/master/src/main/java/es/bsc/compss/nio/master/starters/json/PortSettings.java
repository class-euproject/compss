package es.bsc.compss.nio.master.starters.json;

import com.google.gson.annotations.SerializedName;


public class PortSettings {

    private @SerializedName("HostIp") String hostIp;
    private @SerializedName("HostPort") String hostPort;


    public String getHostIp() {
        return hostIp;
    }

    public void setHostIp(String hostIp) {
        this.hostIp = hostIp;
    }

    public String getHostPort() {
        return hostPort;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

}
