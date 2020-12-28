package rlink.yarn.manager;

import com.alibaba.fastjson.JSON;
import rlink.yarn.manager.model.Command;
import rlink.yarn.manager.model.LaunchParam;
import rlink.yarn.manager.utils.ParameterUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;

public class ResourceManagerCli {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceManagerCli.class);

    private static final String WEB_URL_KEY = "coordinator_address";
    private static final String RESOURCE_PATH_KEY = "worker_process_path";
    private static final String MEMORY_MB_KEY = "memory_mb";
    private static final String VIRTUAL_CORES_KEY = "v_cores";

    public static void main(String[] args) {
        try {
            LOGGER.info("ResourceManagerCli launch args={}", Arrays.toString(args));
            ResourceManager resourceManager = new ResourceManager();
            resourceManager.launch(parseLaunchParams(args));

            Scanner scanner = new Scanner(System.in);
            while (true) {
                String command = scanner.nextLine();
                if (StringUtils.isNotBlank(command)) {
                    Command cmd = parseCommand(command);
                    if (null == cmd || StringUtils.isBlank(cmd.getCmdId())) {
                        continue;
                    }
                    LOGGER.info("ResourceManagerCli command={}", command);
                    resourceManager.run(cmd);
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            LOGGER.error("ResourceManagerCli error", e);
        }
    }

    private static LaunchParam parseLaunchParams(String[] args) {
        LaunchParam launchParam = new LaunchParam();
        Map<String, String> parameterMap = ParameterUtil.fromArgs(args);

        String webUrl = parameterMap.get(WEB_URL_KEY);
        if (StringUtils.isBlank(webUrl)) {
            throw new RuntimeException(WEB_URL_KEY + " is blank.");
        }
        launchParam.setWebUrl(webUrl);

        String resourcePath = parameterMap.get(RESOURCE_PATH_KEY);
        if (StringUtils.isBlank(resourcePath)) {
            throw new RuntimeException(RESOURCE_PATH_KEY + " is blank.");
        }
        launchParam.setResourcePath(new Path(resourcePath));

        String memoryMb = parameterMap.get(MEMORY_MB_KEY);
        if (StringUtils.isBlank(memoryMb)) {
            throw new RuntimeException(MEMORY_MB_KEY + " is blank.");
        }
        try {
            launchParam.setMemoryMb(Integer.parseInt(memoryMb));
        } catch (NumberFormatException e) {
            throw new RuntimeException(MEMORY_MB_KEY + " is not Integer.");
        }

        String vCores = parameterMap.get(VIRTUAL_CORES_KEY);
        if (StringUtils.isBlank(vCores)) {
            throw new RuntimeException(VIRTUAL_CORES_KEY + " is blank.");
        }
        try {
            launchParam.setvCores(Integer.parseInt(vCores));
        } catch (NumberFormatException e) {
            throw new RuntimeException(VIRTUAL_CORES_KEY + " is not Integer.");
        }

        return launchParam;
    }

    private static Command parseCommand(String command) {
        try {
            return JSON.parseObject(command, Command.class);
        } catch (Exception e) {
            return null;
        }
    }
}

