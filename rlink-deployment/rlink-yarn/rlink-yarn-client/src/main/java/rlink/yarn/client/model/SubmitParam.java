package rlink.yarn.client.model;

import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.Map;

public class SubmitParam implements Serializable {

    private String applicationName;
    private Path rustStreamingPath;
    private Path javaManagerPath;
    private Path dashboardPath;
    private int memoryMb;
    private int vCores;
    private int masterMemoryMb;
    private int masterVCores;
    private String queue;
    private Map<String, String> paramMap;

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public Path getRustStreamingPath() {
        return rustStreamingPath;
    }

    public void setRustStreamingPath(Path rustStreamingPath) {
        this.rustStreamingPath = rustStreamingPath;
    }

    public Path getJavaManagerPath() {
        return javaManagerPath;
    }

    public void setJavaManagerPath(Path javaManagerPath) {
        this.javaManagerPath = javaManagerPath;
    }

    public Path getDashboardPath() {
        return dashboardPath;
    }

    public void setDashboardPath(Path dashboardPath) {
        this.dashboardPath = dashboardPath;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

    public void setMemoryMb(int memoryMb) {
        this.memoryMb = memoryMb;
    }

    public int getvCores() {
        return vCores;
    }

    public void setvCores(int vCores) {
        this.vCores = vCores;
    }

    public int getMasterMemoryMb() {
        return masterMemoryMb;
    }

    public void setMasterMemoryMb(int masterMemoryMb) {
        this.masterMemoryMb = masterMemoryMb;
    }

    public int getMasterVCores() {
        return masterVCores;
    }

    public void setMasterVCores(int masterVCores) {
        this.masterVCores = masterVCores;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public Map<String, String> getParamMap() {
        return paramMap;
    }

    public void setParamMap(Map<String, String> paramMap) {
        this.paramMap = paramMap;
    }
}
