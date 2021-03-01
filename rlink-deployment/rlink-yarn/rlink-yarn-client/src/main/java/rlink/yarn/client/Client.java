package rlink.yarn.client;

import rlink.yarn.client.model.SubmitParam;
import rlink.yarn.client.utils.ParameterUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

public class Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    public static final String APPLICATION_NAME_KEY = "applicationName";
    public static final String RUST_STREAMING_PATH_KEY = "worker_process_path";
    public static final String JAVA_MANAGER_PATH_KEY = "java_manager_path";
    public static final String DASHBOARD_PATH_KEY = "dashboard_path";
    public static final String MEMORY_MB_KEY = "memory_mb";
    public static final String VIRTUAL_CORES_KEY = "v_cores";
    public static final String MASTER_MEMORY_MB_KEY = "master_memory_mb";
    public static final String MASTER_VIRTUAL_CORES_KEY = "master_v_cores";
    public static final String QUEUE_KEY = "queue";
    public static final String APPLICATION_ID_KEY = "application_id";
    public static final String MAX_ATTEMPTS_KEY = "max_attempts";

    public static void main(String[] args) {
        try {
            LOGGER.info("client main args: {}", Arrays.toString(args));

            SubmitParam submitParam = parseArgs(args);
            ClientExecutor clientExecutor = new ClientExecutor();
            clientExecutor.run(submitParam);
        } catch (Exception e) {
            LOGGER.error("client error", e);
        }
    }

    private static SubmitParam parseArgs(String[] args) {
        SubmitParam submitParam = new SubmitParam();
        Map<String, String> parameterMap = ParameterUtil.fromArgs(args);

        String applicationName = parameterMap.get(APPLICATION_NAME_KEY);
        if (StringUtils.isBlank(applicationName)) {
            throw new RuntimeException(APPLICATION_NAME_KEY + " is blank.");
        }
        parameterMap.remove(APPLICATION_NAME_KEY);
        submitParam.setApplicationName(applicationName);

        String rustStreamingPath = parameterMap.get(RUST_STREAMING_PATH_KEY);
        if (StringUtils.isBlank(rustStreamingPath)) {
            throw new RuntimeException(RUST_STREAMING_PATH_KEY + " is blank.");
        }
        parameterMap.remove(RUST_STREAMING_PATH_KEY);
        submitParam.setRustStreamingPath(new Path(rustStreamingPath));

        String javaManagerPath = parameterMap.get(JAVA_MANAGER_PATH_KEY);
        if (StringUtils.isBlank(javaManagerPath)) {
            throw new RuntimeException(JAVA_MANAGER_PATH_KEY + " is blank.");
        }
        parameterMap.remove(JAVA_MANAGER_PATH_KEY);
        submitParam.setJavaManagerPath(new Path(javaManagerPath));

        String dashboardPath = parameterMap.get(DASHBOARD_PATH_KEY);
        if (StringUtils.isBlank(dashboardPath)) {
            throw new RuntimeException(DASHBOARD_PATH_KEY + " is blank.");
        }
        parameterMap.remove(DASHBOARD_PATH_KEY);
        submitParam.setDashboardPath(new Path(dashboardPath));

        String memoryMb = parameterMap.get(MEMORY_MB_KEY);
        if (StringUtils.isBlank(memoryMb)) {
            throw new RuntimeException(MEMORY_MB_KEY + " is blank.");
        }
        try {
            submitParam.setMemoryMb(Integer.parseInt(memoryMb));
        } catch (NumberFormatException e) {
            throw new RuntimeException(MEMORY_MB_KEY + " is not Integer.");
        }
        parameterMap.remove(MEMORY_MB_KEY);

        String vCores = parameterMap.get(VIRTUAL_CORES_KEY);
        if (StringUtils.isBlank(vCores)) {
            throw new RuntimeException(VIRTUAL_CORES_KEY + " is blank.");
        }
        try {
            submitParam.setvCores(Integer.parseInt(vCores));
        } catch (NumberFormatException e) {
            throw new RuntimeException(VIRTUAL_CORES_KEY + " is not Integer.");
        }
        parameterMap.remove(VIRTUAL_CORES_KEY);

        String masterMemoryMb = parameterMap.get(MASTER_MEMORY_MB_KEY);
        if (StringUtils.isBlank(masterMemoryMb)) {
            throw new RuntimeException(MASTER_MEMORY_MB_KEY + " is blank.");
        }
        try {
            submitParam.setMasterMemoryMb(Integer.parseInt(masterMemoryMb));
        } catch (NumberFormatException e) {
            throw new RuntimeException(MASTER_MEMORY_MB_KEY + " is not Integer.");
        }
        parameterMap.remove(MASTER_MEMORY_MB_KEY);

        String masterVCores = parameterMap.get(MASTER_VIRTUAL_CORES_KEY);
        if (StringUtils.isBlank(masterVCores)) {
            throw new RuntimeException(MASTER_VIRTUAL_CORES_KEY + " is blank.");
        }
        try {
            submitParam.setMasterVCores(Integer.parseInt(masterVCores));
        } catch (NumberFormatException e) {
            throw new RuntimeException(MASTER_VIRTUAL_CORES_KEY + " is not Integer.");
        }
        parameterMap.remove(MASTER_VIRTUAL_CORES_KEY);

        String queue = parameterMap.get(QUEUE_KEY);
        if (StringUtils.isBlank(queue)) {
            throw new RuntimeException(QUEUE_KEY + " is blank.");
        }
        parameterMap.remove(QUEUE_KEY);
        submitParam.setQueue(queue);

        String maxAttempts = parameterMap.get(MAX_ATTEMPTS_KEY);
        if (StringUtils.isBlank(maxAttempts)) {
            submitParam.setMaxAppAttempts(1);
        } else {
            try {
                submitParam.setMaxAppAttempts(Integer.parseInt(maxAttempts));
            } catch (NumberFormatException e) {
                throw new RuntimeException(MAX_ATTEMPTS_KEY + " is not Integer.");
            }
        }
        parameterMap.remove(MAX_ATTEMPTS_KEY);

        submitParam.setParamMap(parameterMap);

        return submitParam;
    }

}
