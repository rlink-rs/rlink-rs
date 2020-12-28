package rlink.yarn.manager.model;

import org.apache.hadoop.fs.Path;

import java.io.Serializable;

public class LaunchParam implements Serializable {

    private String webUrl;
    private Path resourcePath;
    private int memoryMb;
    private int vCores;

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
}
