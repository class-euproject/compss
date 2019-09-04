package es.bsc.compss.nio.master.configuration.rotterdam.config;

import java.util.ArrayList;
import java.util.List;


public class ContainerConfig {

    private String name;
    private String image;
    private List<PortConfig> ports = new ArrayList<>();
    private List<String> command;
    private List<String> args;


    public ContainerConfig(String name, String image) {
        this.name = name;
        this.image = image;
    }

    public void addPort(PortConfig portConfig) {
        if (ports == null) {
            this.ports = new ArrayList<>();
        }
        ports.add(portConfig);
    }

    public void addPort(Integer containerPort, Integer hostPort, String protocol) {
        addPort(new PortConfig(containerPort, hostPort, protocol));
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public List<PortConfig> getPorts() {
        return ports;
    }

    public void setPorts(List<PortConfig> ports) {
        this.ports = ports;
    }

    public List<String> getCommand() {
        return command;
    }

    public void setCommand(List<String> command) {
        this.command = command;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }
}
