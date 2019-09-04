package es.bsc.compss.nio.master.configuration.rotterdam.config;

import java.util.ArrayList;
import java.util.List;


public class RotterdamTaskDefinition {

    private String dbid;
    private String name;
    private String dock;
    private String id;
    private QosConfig qos;
    private Integer replicas;
    private List<ContainerConfig> containers = new ArrayList<>();


    public RotterdamTaskDefinition(String name, String dock, Integer replicas) {
        this.name = name;
        this.dock = dock;
        this.replicas = replicas;
    }

    public void addContainer(ContainerConfig config) {
        containers.add(config);
    }

    public String getDbid() {
        return dbid;
    }

    public void setDbid(String dbid) {
        this.dbid = dbid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDock() {
        return dock;
    }

    public void setDock(String dock) {
        this.dock = dock;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public QosConfig getQos() {
        return qos;
    }

    public void setQos(QosConfig qos) {
        this.qos = qos;
    }

    public Integer getReplicas() {
        return replicas;
    }

    public void setReplicas(Integer replicas) {
        this.replicas = replicas;
    }

    public List<ContainerConfig> getContainers() {
        return containers;
    }

    public void setContainers(List<ContainerConfig> containers) {
        this.containers = containers;
    }
}
