package rlink.yarn.manager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.*;
import rlink.yarn.manager.model.Command;
import rlink.yarn.manager.model.ContainerInfo;
import rlink.yarn.manager.model.LaunchParam;
import rlink.yarn.manager.model.TaskResourceInfo;
import rlink.yarn.manager.utils.MessageUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ResourceManager implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceManager.class);

    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(
            1, 1, 1L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(4096),
            new ThreadFactoryBuilder().setNameFormat("exec-pool-%d").build());

    private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

    private NMClientAsync nodeManagerClient;

    private YarnConfiguration yarnConfiguration = new YarnConfiguration();

    private int memoryMb;
    private int vCores;
    private Path resourcePath;

    private List<Map> allocateParams;
    private List<TaskResourceInfo> list = new ArrayList<>();
    private int containerCount;
    private Command preCommand;
    private Command curCommand;
    private boolean commandRunning;

    private List<TaskResourceInfo> stopTaskList = new ArrayList<>();
    private AtomicInteger stopContainerCount = new AtomicInteger(0);

    public void launch(LaunchParam launchParam) throws Exception {
        String webUrl = launchParam.getWebUrl();
        resourcePath = launchParam.getResourcePath();
        memoryMb = launchParam.getMemoryMb();
        vCores = launchParam.getvCores();

        int hostStartIndex = !webUrl.contains("://") ? 0 : webUrl.indexOf("://") + 3;
        int hostEndIndex = webUrl.indexOf(":", hostStartIndex);
        String appMasterHostname = webUrl.substring(hostStartIndex, hostEndIndex);
        int portStartIndex = hostEndIndex + 1;
        int portEndIndex = webUrl.indexOf("/", hostEndIndex) == -1 ? webUrl.length() : webUrl.indexOf("/", hostEndIndex);
        int appMasterRpcPort = Integer.parseInt(webUrl.substring(portStartIndex, portEndIndex));
        executor.execute(() -> {
            try {
                resourceManagerClient = createResourceManagerClient(appMasterHostname, appMasterRpcPort, webUrl, yarnConfiguration);
                nodeManagerClient = createNodeManagerClient(yarnConfiguration);
                LOGGER.info("waiting for request yarn container command");
            } catch (Exception e) {
                LOGGER.error("createResourceManagerClient error", e);
            }
        });
    }

    public void run(Command command) {
        LOGGER.info("run command={}", JSON.toJSONString(command));
        startCommand(command);
        switch (command.getCmd()) {
            case allocate:
                executeAllocate(command);
                break;
            case stop:
                executeStop(command);
                break;
            default:
                throw new RuntimeException("invalid command");
        }
    }

    private void startCommand(Command command) {
        preCommand = curCommand;
        curCommand = command;
        commandRunning = true;
    }

    private void endCommand() {

    }

    private void executeAllocate(Command command) {
        executor.execute(() -> {
            try {
                allocateParams = command.getData();
                containerCount = command.getData().size();
                requestYarnContainer(memoryMb, vCores, containerCount);
            } catch (Exception e) {
                LOGGER.error("executeAllocate error", e);
            }
        });
    }

    private void executeStop(Command command) {
        executor.execute(() -> {
            try {
                List<Map> data = command.getData();
                List<TaskResourceInfo> taskInfoList = JSONArray.parseArray(JSON.toJSONString(data), TaskResourceInfo.class);
                stopTaskList = taskInfoList;
                stopContainerCount = new AtomicInteger(0);
                for (TaskResourceInfo taskResourceInfo : taskInfoList) {
                    stopTaskContainer(taskResourceInfo);
                }
            } catch (Exception e) {
                LOGGER.error("executeStop error", e);
            }
        });
    }

    private AMRMClientAsync<AMRMClient.ContainerRequest> createResourceManagerClient(String appMasterHostname, int appMasterRpcPort, String appMasterTrackingUrl,
                                                                                     YarnConfiguration yarnConfiguration) throws Exception {

        LOGGER.info("createResourceManagerClient, appMasterHostname={},appMasterRpcPort={},appMasterTrackingUrl={}",
                appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);
        AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient = AMRMClientAsync.createAMRMClientAsync(2000, this);
        resourceManagerClient.init(yarnConfiguration);
        resourceManagerClient.start();

        LOGGER.info("resourceManagerClient start");

        RegisterApplicationMasterResponse registerApplicationMasterResponse = resourceManagerClient
                .registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);

        LOGGER.info("registerApplicationMaster success,response={}", registerApplicationMasterResponse);

        return resourceManagerClient;
    }

    private NMClientAsync createNodeManagerClient(YarnConfiguration yarnConfiguration) {
        NMClientAsync nodeManagerClient = NMClientAsync.createNMClientAsync(this);
        nodeManagerClient.init(yarnConfiguration);
        nodeManagerClient.start();
        LOGGER.info("nodeManagerClient start");
        return nodeManagerClient;
    }

    private void requestYarnContainer(int containerMemory, int containerVCores, int numContainers) {
        Resource resource = Resource.newInstance(containerMemory, containerVCores);
        AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(resource, null, null, Priority.newInstance(1));
        for (int i = 0; i < numContainers; i++) {
            LOGGER.info("requestYarnContainer {}", i);
            resourceManagerClient.addContainerRequest(containerRequest);
        }
    }

    private void startTaskExecutorInContainer(Container container, Map taskParamMap) {
        try {
            ContainerLaunchContext taskExecutorLaunchContext = createTaskExecutorLaunchContext(yarnConfiguration, resourcePath, taskParamMap);
            LOGGER.info("startTaskExecutor {}, context={}", container.getId(), taskExecutorLaunchContext.toString());
            nodeManagerClient.startContainerAsync(container, taskExecutorLaunchContext);
        } catch (Throwable t) {
            LOGGER.error("start container error", t);
        }
    }

    private void stopTaskContainer(TaskResourceInfo taskResourceInfo) {
        try {
            ContainerInfo containerInfo = taskResourceInfo.getResourceInfo();
            ContainerId containerId = ContainerId.fromString(containerInfo.getContainerId());
            String host = StringUtils.substringBefore(containerInfo.getNodeId(), ":");
            String port = StringUtils.substringAfter(containerInfo.getNodeId(), ":");
            NodeId nodeId = NodeId.newInstance(host, Integer.parseInt(port));
            nodeManagerClient.stopContainerAsync(containerId, nodeId);
            LOGGER.info("stopTaskContainer containerInfo={}", JSON.toJSONString(containerInfo));
        } catch (Exception e) {
            LOGGER.error("stop container error,taskResourceInfo={}", JSON.toJSONString(taskResourceInfo), e);
        }
    }

    private ContainerLaunchContext createTaskExecutorLaunchContext(YarnConfiguration yarnConfiguration, Path resourcePath, Map taskParamMap) throws Exception {
        FileStatus fileStatus = FileSystem.get(yarnConfiguration).getFileStatus(resourcePath);
        LocalResource localResource = Records.newRecord(LocalResource.class);
        localResource.setResource(ConverterUtils.getYarnUrlFromPath(resourcePath));
        localResource.setSize(fileStatus.getLen());
        localResource.setTimestamp(fileStatus.getModificationTime());
        localResource.setType(LocalResourceType.FILE);
        localResource.setVisibility(LocalResourceVisibility.APPLICATION);

        String taskCommand = "./" + resourcePath.getName() + " " +
                taskParamMap.toString().replaceAll("[,{}]", "") +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/taskManager.out" +
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/taskManager.err";
        Map<String, LocalResource> taskManagerLocalResources = new HashMap<>();
        taskManagerLocalResources.put(resourcePath.getName(), localResource);
        ContainerLaunchContext containerLaunchContext = ContainerLaunchContext.newInstance(
                taskManagerLocalResources, null, Collections.singletonList(taskCommand), null, null, null);
        return containerLaunchContext;
    }

    // ------------------------------------------------------------------------
    //  AMRMClientAsync CallbackHandler methods
    // ------------------------------------------------------------------------

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        LOGGER.info("YARN ResourceManager reported the following containers completed: {}.", statuses);

    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            TaskResourceInfo taskResourceInfo = new TaskResourceInfo(container.getId().toString(),
                    new ContainerInfo(container.getId().toString(), container.getNodeId().toString()));
            startTaskExecutorInContainer(container, allocateParams.get(list.size()));
            list.add(taskResourceInfo);
        }
        if (list.size() == containerCount) {
            List<Map> data = JSONArray.parseArray(JSON.toJSONString(list), Map.class);
            Command commandMsg = new Command(curCommand.getCmd(), curCommand.getCmdId(), data);
            MessageUtil.send(commandMsg);
        }
    }

    @Override
    public void onShutdownRequest() {
        LOGGER.warn("onShutdownRequest");

    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        LOGGER.warn("onNodesUpdated,{}", updatedNodes);

    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable e) {
        LOGGER.warn("onError.", e);

    }

    // ------------------------------------------------------------------------
    //  NMClientAsync CallbackHandler methods
    // ------------------------------------------------------------------------

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        LOGGER.info("Succeeded to call YARN Node Manager to start container {}.", containerId);

    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        stopContainerCount.addAndGet(1);
        LOGGER.info("Succeeded stop container {}. [{}/{}]", containerId, stopContainerCount.get(), stopTaskList.size());
        if (stopContainerCount.get() == stopTaskList.size()) {
            List<Map> data = JSONArray.parseArray(JSON.toJSONString(stopTaskList), Map.class);
            Command commandMsg = new Command(curCommand.getCmd(), curCommand.getCmdId(), data);
            MessageUtil.send(commandMsg);
        }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        LOGGER.error("Error start container {}.", containerId, t);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {

    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        LOGGER.error("Error stop container {}.", containerId, t);
    }
}
