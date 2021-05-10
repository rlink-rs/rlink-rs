package rlink.yarn.manager.model;

import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.List;

public class LaunchParam implements Serializable {

    private String webUrl;
    private Path resourcePath;
    private int memoryMb;
    private int vCores;
    private List<String> exclusionNodes;

    public String getWebUrl() {
        return webUrl;
    }

    public void setWebUrl(String webUrl) {
        this.webUrl = webUrl;
    }

    public Path getResourcePath() {
        return resourcePath;
    }

    public void setResourcePath(Path resourcePath) {
        this.resourcePath = resourcePath;
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

    public List<String> getExclusionNodes() {
        return exclusionNodes;
    }

    public void setExclusionNodes(List<String> exclusionNodes) {
        this.exclusionNodes = exclusionNodes;
    }
}
