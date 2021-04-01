package rlink.yarn.client;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rlink.yarn.client.model.SubmitParam;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static rlink.yarn.client.Client.APPLICATION_ID_KEY;
import static rlink.yarn.client.Client.APPLICATION_NAME_KEY;
import static rlink.yarn.client.Client.MEMORY_MB_KEY;
import static rlink.yarn.client.Client.RUST_STREAMING_PATH_KEY;
import static rlink.yarn.client.Client.VIRTUAL_CORES_KEY;

public class ClientExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientExecutor.class);

    public void run(SubmitParam submitParam) throws Exception {
        String applicationName = submitParam.getApplicationName();
        int masterMemoryMb = submitParam.getMasterMemoryMb();
        int masterVCores = submitParam.getMasterVCores();
        String queue = submitParam.getQueue();
        int maxAppAttempts = submitParam.getMaxAppAttempts();

        // Create yarnClient
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        // Create application via yarnClient
        YarnClientApplication app = yarnClient.createApplication();

        // Set up the container launch context for the application master
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        ContainerLaunchContext resourceManagerContext = createResourceManagerContext(submitParam, yarnConfiguration, appId.toString());

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(masterMemoryMb);
        capability.setVirtualCores(masterVCores);

        // Finally, set-up ApplicationSubmissionContext for the application
        appContext.setApplicationName(applicationName);
        appContext.setAMContainerSpec(resourceManagerContext);
        appContext.setResource(capability);
        appContext.setQueue(queue);
        appContext.setMaxAppAttempts(maxAppAttempts);

        // Submit application
        LOGGER.info("Submitting application {}, appContext={}", appId, appContext.toString());
        yarnClient.submitApplication(appContext);

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.KILLED &&
                appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        LOGGER.info("Application {} finished with state {} at {}", appId, appState, appReport.getFinishTime());
    }

    private ContainerLaunchContext createResourceManagerContext(SubmitParam submitParam, YarnConfiguration yarnConfiguration, String appId) throws Exception {
        int memoryMb = submitParam.getMemoryMb();
        int vCores = submitParam.getvCores();
        Path rustStreamingPath = submitParam.getRustStreamingPath();
        Path javaManagerPath = submitParam.getJavaManagerPath();
        Path dashboardPath = submitParam.getDashboardPath();
        List<Path> pathList = new ArrayList<>();
        pathList.add(rustStreamingPath);
        pathList.add(javaManagerPath);
        pathList.add(dashboardPath);
        Map<String, String> paramMap = submitParam.getParamMap();
        paramMap.put(APPLICATION_NAME_KEY, submitParam.getApplicationName());
        paramMap.put(MEMORY_MB_KEY, String.valueOf(memoryMb));
        paramMap.put(VIRTUAL_CORES_KEY, String.valueOf(vCores));
        paramMap.put(RUST_STREAMING_PATH_KEY, rustStreamingPath.toString());
        paramMap.put(APPLICATION_ID_KEY, appId);

        ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);

        String command = "./" + rustStreamingPath.getName() + " " +
                paramMap.toString().replaceAll("[,{}]", "") +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobManager.out" +
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobManager.err";
        context.setCommands(Collections.singletonList(command));

        // Setup localResource for ApplicationMaster
        Map<String, LocalResource> localResourceMap = setupAppMasterResource(pathList, yarnConfiguration);
        context.setLocalResources(localResourceMap);

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = setupAppMasterEnv(yarnConfiguration);
        context.setEnvironment(appMasterEnv);

        return context;
    }

    private Map<String, LocalResource> setupAppMasterResource(List<Path> resourcePathList, YarnConfiguration yarnConfiguration) throws IOException {
        Map<String, LocalResource> resourceMap = new HashMap<>(resourcePathList.size());
        for (Path resourcePath : resourcePathList) {
            LocalResourceType localResourceType = isCompressFile(resourcePath.getName()) ?
                    LocalResourceType.ARCHIVE : LocalResourceType.FILE;

            FileStatus fileStatus = FileSystem.get(yarnConfiguration).getFileStatus(resourcePath);
            LocalResource localResource = Records.newRecord(LocalResource.class);
            localResource.setResource(ConverterUtils.getYarnUrlFromPath(resourcePath));
            localResource.setSize(fileStatus.getLen());
            localResource.setTimestamp(fileStatus.getModificationTime());
            localResource.setType(localResourceType);
            localResource.setVisibility(LocalResourceVisibility.APPLICATION);

            resourceMap.put(resourcePath.getName(), localResource);
        }
        return resourceMap;
    }

    private Map<String, String> setupAppMasterEnv(YarnConfiguration yarnConfiguration) {
        Map<String, String> appMasterEnv = new HashMap<>();
        for (String c : yarnConfiguration.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                    c.trim(), File.pathSeparator);
        }
        Apps.addToEnvironment(appMasterEnv,
                ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "*", File.pathSeparator);
        return appMasterEnv;
    }

    private static boolean isCompressFile(String fileName) {
        return fileName.endsWith(".zip")
                || fileName.endsWith(".tar")
                || fileName.endsWith(".tar.gz");
    }
}
