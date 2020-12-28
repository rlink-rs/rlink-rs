package rlink.yarn.manager.model;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;

public class TaskResourceInfo implements Serializable {
    @JSONField(name = "task_id")
    private String taskId;
    @JSONField(name = "task_manager_address")
    private String taskManagerAddress;
    @JSONField(name = "task_manager_id")
    private String taskManagerId;
    @JSONField(name = "resource_info")
    private ContainerInfo resourceInfo;

    public TaskResourceInfo() {
    }

    public TaskResourceInfo(String taskManagerId, ContainerInfo resourceInfo) {
        this.taskManagerId = taskManagerId;
        this.resourceInfo = resourceInfo;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskManagerAddress() {
        return taskManagerAddress;
    }

    public void setTaskManagerAddress(String taskManagerAddress) {
        this.taskManagerAddress = taskManagerAddress;
    }

    public String getTaskManagerId() {
        return taskManagerId;
    }

    public void setTaskManagerId(String taskManagerId) {
        this.taskManagerId = taskManagerId;
    }

    public ContainerInfo getResourceInfo() {
        return resourceInfo;
    }

    public void setResourceInfo(ContainerInfo resourceInfo) {
        this.resourceInfo = resourceInfo;
    }
}
