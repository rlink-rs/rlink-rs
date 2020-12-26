package rlink.yarn.manager.model;

import java.io.Serializable;

public class ContainerInfo implements Serializable {

    private String containerId;
    private String nodeId;

    public ContainerInfo(String containerId, String nodeId) {
        this.containerId = containerId;
        this.nodeId = nodeId;
    }

    public String getContainerId() {
        return containerId;
    }

    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
