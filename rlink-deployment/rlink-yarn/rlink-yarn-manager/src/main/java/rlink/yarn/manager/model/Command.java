package rlink.yarn.manager.model;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Command implements Serializable {

    private CommandType cmd;
    @JSONField(name = "cmd_id")
    private String cmdId;
    private List<Map> data;

    public Command() {
    }

    public Command(CommandType cmd, String cmdId, List<Map> data) {
        this.cmd = cmd;
        this.cmdId = cmdId;
        this.data = data;
    }

    public CommandType getCmd() {
        return cmd;
    }

    public void setCmd(CommandType cmd) {
        this.cmd = cmd;
    }

    public String getCmdId() {
        return cmdId;
    }

    public void setCmdId(String cmdId) {
        this.cmdId = cmdId;
    }

    public List<Map> getData() {
        return data;
    }

    public void setData(List<Map> data) {
        this.data = data;
    }

    public enum CommandType {
        // allocate resource
        allocate,
        // stop container
        stop
    }
}
