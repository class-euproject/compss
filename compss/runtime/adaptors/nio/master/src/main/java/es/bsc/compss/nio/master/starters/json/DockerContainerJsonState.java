package es.bsc.compss.nio.master.starters.json;

import com.google.gson.annotations.SerializedName;


public class DockerContainerJsonState {

    private ContainerState state;
    private @SerializedName("NetworkSettings") NetworkSettings networkSettings;
    private String message;


    public ContainerState getState() {
        return state;
    }

    public void setState(ContainerState state) {
        this.state = state;
    }

    public NetworkSettings getNetworkSettings() {
        return networkSettings;
    }

    public void setNetworkSettings(NetworkSettings networkSettings) {
        this.networkSettings = networkSettings;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
