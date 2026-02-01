package top.fateironist.net_relay.core.communication;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import top.fateironist.net_relay.core.common.TaskScheduler;
import top.fateironist.net_relay.core.filter.base.RegisterPortFilterChain;
import top.fateironist.net_relay.core.relay.RelayManager;
import top.fateironist.net_relay.model.common.enums.WorkingStatusEnum;
import top.fateironist.net_relay.model.communication.*;
import top.fateironist.net_relay.model.filter.RegisterPortRequest;
import top.fateironist.net_relay.model.filter.enums.RegisterPortRequestTypeEnum;
import top.fateironist.net_relay.model.relay.enums.TransportLayerProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

// fixme udp注册通道无filter逻辑

@Slf4j
@Component
public class CommunicationManager implements DisposableBean {
    private final Queue<CommunicationTask> taskQueue;
    // 被代理端注册表
    private final Map<String, Agent> agentMap;
    private WorkingStatusEnum workingStatus = WorkingStatusEnum.STARTING;
    private final Selector selector;

    @Value("${net-relay.server.port}")
    private int serverPort;

    /**
     * 过滤链一共有两条
     * -RegisterPortFilterChain
     * -ProxyPortFilterChain
     * 三处被调用
     * -CommunicationManager 的 processAccept 初衷是方便直接决定是否接收连接
     * -CommunicationManager 的 processReadable 这里是决定是否接收被代理端的请求
     * -RelayManager 的 processAcceptable 方便直接决定是否接收连接
     *
     * 这里可以直接继承
     * -RegisterPortFilter
     * -ProxyPortFilter
     * 进行拓展
     */
    @Autowired
    private RegisterPortFilterChain filterChain;

    @Autowired
    private RelayManager relayManager;

    public CommunicationManager() throws IOException {
        this.taskQueue = new ConcurrentLinkedQueue<>();
        this.agentMap = new HashMap<>();
        this.selector = Selector.open();
    }

    @PostConstruct
    public void init() {
        this.start();

        relayManager.start(this);

        // 定时清理掉线的被代理端
        TaskScheduler.scheduleWithFixedRate(() -> {
            for (String agentId : agentMap.keySet()) {
                agentMap.compute(agentId, (k, v) -> {
                    if (v != null && !v.isActive()) {
                        closeAgent(k);
                        return null;
                    }
                    return v;
                });
            }
        }, 5, 5, TimeUnit.MINUTES);
    }

    public void start() {
        workingStatus = WorkingStatusEnum.STARTING;
        try {
            ServerSocketChannel ssc = ServerSocketChannel.open(StandardProtocolFamily.INET);
            ssc.configureBlocking(false);
            ssc.bind(new InetSocketAddress(serverPort));
            ssc.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            logError("CommunicationManager start error!!! exception:{}", e.getMessage());
            return;
        }

        Thread.ofVirtual().name("CommunicationManagerThread").uncaughtExceptionHandler((t, e) -> {
            if (e instanceof ClosedSelectorException) {
                if (workingStatus.getCode() <= WorkingStatusEnum.WORKING.getCode()) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException(e);
            }
        }).start(() -> {
            while (workingStatus.getCode() <= WorkingStatusEnum.WORKING.getCode()) {
                try {
                    selector.select();
                } catch (IOException e) {
                    logError("Communication Select error; exception:{}", e.getMessage());
                    shutdown();
                    continue;
                }

                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

                if (log.isDebugEnabled()) {
                    log.debug("CommunicationManager select; length:{}", selector.selectedKeys().size());
                }

                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    if (selectionKey.isValid() && selectionKey.isAcceptable()) {
                        processChannelAcceptable(selectionKey);
                    }
                    if (selectionKey.isValid() && selectionKey.isReadable()) {
                        processChannelReadable(selectionKey);
                    }
                    if (selectionKey.isValid() && selectionKey.isWritable()) {
                        processChannelWritable(selectionKey);
                    }

                    iterator.remove();
                }

                CommunicationTask communicationTask = null;
                while ((communicationTask = taskQueue.poll()) != null) {
                    processTask(communicationTask);
                }
            }
        });

        log.info("CommunicationManager started");
        workingStatus = WorkingStatusEnum.WORKING;
    }

    // 处理新的连接
    private void processChannelAcceptable(SelectionKey selectionKey) {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel channel = null;
        try {
            channel = serverSocketChannel.accept();
        } catch (IOException e) {
            if (isRunning()) log.warn("CommunicationManager accept error;exception:{}", e.getMessage());
        }

        RegisterPortRequest request = new RegisterPortRequest(
                channel.socket().getInetAddress().getHostAddress(),
                channel.socket().getPort(),
                channel,
                RegisterPortRequestTypeEnum.CONNECTION
        );

        AtomicBoolean drop = new AtomicBoolean(true);

        if (log.isDebugEnabled()) {
            log.debug("CommunicationManager processChannelAcceptable;ip:{}", channel.socket().getRemoteSocketAddress());
        }

        final SocketChannel finalChannel = channel;

        // 连接过滤链
        filterChain.startChain(request, (r) -> {
            try {
                drop.set(false);
                finalChannel.configureBlocking(false);
                finalChannel.register(selector, SelectionKey.OP_READ, new CommunicationChannelAttachment());
            } catch (IOException e) {
                if (isRunning()) log.warn("CommunicationManager register select error;ip:{},exception:{}", finalChannel.socket().getRemoteSocketAddress() , e.getMessage());
            }
        });

        if (drop.get()) {
            closeChannel(channel);
        }
    }

    // 处理写事件
    private void processChannelReadable(SelectionKey selectionKey) {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        CommunicationChannelAttachment attachment = (CommunicationChannelAttachment) selectionKey.attachment();

        if (!selectionKey.isValid() || (agentMap.containsKey(attachment.getAgentId()) && agentMap.get(attachment.getAgentId()).shouldClose())) {
            if (!closeAgent(attachment.getAgentId())) closeChannel(socketChannel);
            return;
        }

        ByteBuffer buffer = attachment.getReadBuffer();
        buffer = buffer == null ? ByteBuffer.allocateDirect(CommunicationProtocol.MAX_MSG_SIZE) : buffer;
        int len = 0;

        try {
            len = socketChannel.read(buffer);

            if (log.isDebugEnabled()) {
                log.debug("CommunicationManager(agentId:{},ip:{}) processChannelReadable;len:{}", attachment.getAgentId(), socketChannel.socket().getRemoteSocketAddress(), len);
            }

            if (log.isTraceEnabled() && len > 0) {
                ByteBuffer slice = buffer.duplicate();
                slice.flip();
                byte[] bytes = new byte[slice.remaining()];
                slice.get(bytes);
                System.out.println("-------------------CommunicationRead------------------");
                log.trace("CommunicationManager(agentId:{},ip:{}) processChannelReadable;\ncontent:{}", attachment.getAgentId(), socketChannel.socket().getRemoteSocketAddress(), new String(bytes, StandardCharsets.UTF_8));
                System.out.println("------------------------------------------------------");
            }

        } catch (IOException e) {
            if (agentMap.containsKey(attachment.getAgentId()) && agentMap.get(attachment.getAgentId()).isRunning()) log.warn("SocketChannel(agentId:{}, ip:{}) read error; exception:{}", attachment.getAgentId(), socketChannel.socket().getRemoteSocketAddress(), e.getMessage());
            if (!closeAgent(attachment.getAgentId())) closeChannel(socketChannel);
        }

        if (len == 0) {

        } else if (len == -1) {
            if (!closeAgent(attachment.getAgentId())) closeChannel(socketChannel);
        } else {
            buffer.flip();
            byte[] bytes = new byte[len];
            buffer.get(bytes);
            int index = CommunicationMsg.findMsgEnd(bytes);

            // Long messages will be seen as attacks
            // High-frequency attack requests should be restricted by middleware like Nginx
            if (len >= CommunicationProtocol.MAX_MSG_SIZE && index == -1) {
                log.warn("CommunicationManager(agentId:{},ip:{}) message too long", attachment.getAgentId(), socketChannel.socket().getRemoteSocketAddress());
            } else {
                buffer.position(0);
                if (index != -1) {
                    bytes = new byte[index + 1];
                    buffer.get(bytes);
                    if (buffer.hasRemaining()) {
                        attachment.setReadBuffer(buffer);
                    } else {
                        attachment.setReadBuffer(null);
                    }

                    CommunicationMsg communicationMsg = CommunicationMsg.parse(bytes);
                    RegisterPortRequest registerPortRequest = new RegisterPortRequest(
                            socketChannel.socket().getInetAddress().getHostAddress(),
                            socketChannel.socket().getPort(),
                            socketChannel,
                            communicationMsg
                    );

                    // todo 等待更好的解法，比如给msg加属性
                    CommunicationTask communicationTask = CommunicationTask.buildRequestTask(registerPortRequest);

                    switch (communicationMsg.getRequest().getName()) {
                        case CommunicationProtocol.BODY_REQUIRE_TCP_RELAY_CHANNEL_RESPONSE_MSG:
                            selectionKey.cancel();
                            break;
                        case CommunicationProtocol.BODY_REGISTER_COMMUNICATION_CHANNEL_MSG:
                            registerPortRequest.setSelectionKey(selectionKey);
                            break;
                    }

                    // 请求过滤链
                    filterChain.startChain(registerPortRequest, (request) -> {
                        taskQueue.offer(communicationTask);
                    });
                }
                buffer.flip();
            }

        }

    }

    private void processChannelWritable(SelectionKey selectionKey) {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        CommunicationChannelAttachment attachment = (CommunicationChannelAttachment) selectionKey.attachment();

        if (!selectionKey.isValid() || (agentMap.containsKey(attachment.getAgentId()) && agentMap.get(attachment.getAgentId()).shouldClose())) {
            if (!closeAgent(attachment.getAgentId())) closeChannel(socketChannel);
            return;
        }

        ByteBuffer buffer = attachment.getWriteBuffer();
        if (buffer == null) {
            log.warn("WriteBuffer is null in communication write task!");
            selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_WRITE);
            return;
        }
        try {
            if (log.isDebugEnabled()) {
                log.debug("CommunicationManager(agentId:{},ip:{}) processChannelWritable;len:{}", attachment.getAgentId(), socketChannel.socket().getRemoteSocketAddress(), buffer.remaining());
            }

            if (log.isTraceEnabled()) {
                ByteBuffer slice = buffer.slice();
                byte[] bytes = new byte[slice.remaining()];
                slice.get(bytes);
                System.out.println("-------------------CommunicationWrite------------------");
                log.trace("CommunicationManager(agentId:{},ip:{}) processChannelWritable;\ncontent:{}", attachment.getAgentId(), socketChannel.socket().getRemoteSocketAddress(), new String(bytes, StandardCharsets.UTF_8));
                System.out.println("-------------------------------------------------------");
            }

            socketChannel.write(buffer);

        } catch (IOException e) {
            if (agentMap.containsKey(attachment.getAgentId()) && agentMap.get(attachment.getAgentId()).isRunning()) log.warn("SocketChannel(agentId:{}, ip:{}) write error; exception:{}", attachment.getAgentId(), socketChannel.socket().getRemoteSocketAddress(), e.getMessage());
        }
        if (!buffer.hasRemaining()) {
            attachment.setWriteBuffer(null);
            selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_WRITE);
        }

    }

    private void processTask(CommunicationTask communicationTask) {

        if (log.isDebugEnabled()) {
            log.debug("CommunicationManager(agentId:{}) processTask;\ncontent:{}", communicationTask.getAgentId(), communicationTask);
        }

        switch (communicationTask.getType()) {
            case ORDER:
                Agent agent = agentMap.get(communicationTask.getAgentId());
                if (agent != null) {
                    SocketChannel socketChannel = agent.getCommunicationSocketChannel();
                    byte[] bytes = communicationTask.getCommunicationMsg().buildBytesOrderMessage();
                    ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
                    buffer.put(bytes);
                    buffer.flip();

                    agent.getAttachment().setWriteBuffer(buffer);
                    // fixme 这里因为是LT模式且attachment全局共享且普通channel只有读写两种事件，因此可以直接覆盖，等待更优雅的解法
                    try {
                        socketChannel.register(selector, SelectionKey.OP_WRITE | SelectionKey.OP_READ, agent.getAttachment());
                    } catch (ClosedChannelException e) {
                        if (isRunning()) log.warn("CommunicationManager(agentId:{}, ip:{}) register writing task error; exception:{}", agent.getAgentId(), socketChannel.socket().getRemoteSocketAddress(), e.getMessage());
                    }
                }
                break;
            case REQUEST:
                RegisterPortRequest registerPortRequest = communicationTask.getRegisterPortRequest();

                // todo 考虑可以引入interceptor
                CommunicationMsg msg = registerPortRequest.getCommunicationMsg();
                String agentId = msg.getAgentId();

                switch (msg.getRequest().getName()) {
                    case CommunicationProtocol.BODY_REGISTER_PING_MSG:
                        refreshAgent(agentId);
                        break;
                    case CommunicationProtocol.BODY_REGISTER_COMMUNICATION_CHANNEL_MSG:
                        registerCommunicationChannel(registerPortRequest);
                        break;
                    case CommunicationProtocol.BODY_REGISTER_TCP_PROXY_MSG:
                        registerTcpProxy(registerPortRequest);
                        break;
                    case CommunicationProtocol.BODY_REGISTER_UDP_PROXY_MSG:
                        registerUdpProxy(registerPortRequest);
                        break;
                    case CommunicationProtocol.BODY_REQUIRE_TCP_RELAY_CHANNEL_RESPONSE_MSG:
                        registerTcpRelayChannel(registerPortRequest);
                        break;
                    case CommunicationProtocol.BODY_REQUIRE_UDP_RELAY_CHANNEL_RESPONSE_MSG:
                        registerUdpRelayChannel(registerPortRequest);
                        break;
                    case CommunicationProtocol.BODY_SHUTDOWN_MSG:
                        closeAgent(agentId);
                        break;
                }

                break;
                // todo 其他消息拓展
        }
    }

    public void refreshAgent(String agentId) {
        Agent agent = agentMap.get(agentId);
        if (agent != null) {
            agent.setLastActiveTime(System.currentTimeMillis());
        }
    }

    private String registerCommunicationChannel(RegisterPortRequest registerPortRequest) {
        String agentId = UUID.randomUUID().toString();
        CommunicationChannelAttachment attachment = new CommunicationChannelAttachment(agentId);

        try {
            registerPortRequest.getSocketChannel().register(selector, SelectionKey.OP_READ, attachment);
        } catch (ClosedChannelException e) {
            if (isRunning()) log.warn("CommunicationManager(agentId:{}, ip:{}) register communication error; exception:{}", agentId, registerPortRequest.getSocketChannel().socket().getRemoteSocketAddress(), e.getMessage());
        }
        agentMap.put(agentId, Agent.builder()
                        .agentId(agentId)
                        .attachment(attachment)
                        .ip(registerPortRequest.getIp())
                        .port(registerPortRequest.getPort())
                        .selectionKey(registerPortRequest.getSelectionKey())
                        .communicationSocketChannel(registerPortRequest.getSocketChannel())
                .build());

        CommunicationMsg communicationMsg = new CommunicationMsg();
        communicationMsg.setAgentId(agentId);
        communicationMsg.setOrder(new CommunicationMsg.Method(CommunicationProtocol.BODY_REGISTER_COMMUNICATION_CHANNEL_RESPONSE_MSG, new String[]{"1",agentId}));
        sendMessage(agentId, communicationMsg);

        log.info("Agent(id:{}, ip:{}) register success", agentId, registerPortRequest.getSocketChannel().socket().getRemoteSocketAddress());
        return agentId;
    }

    private void registerTcpProxy(RegisterPortRequest registerPortRequest) {
        String agentId = registerPortRequest.getCommunicationMsg().getAgentId();
        String[] proxyPorts = new String[registerPortRequest.getCommunicationMsg().getRequest().getArgs().length];
        if (agentId != null && agentMap.containsKey(agentId)) {
            String[] agentPorts = registerPortRequest.getCommunicationMsg().getRequest().getArgs();

            for (int i = 0; i < agentPorts.length; i++) {
                ServerSocketChannel serverSocketChannel = null;
                int port = 0;
                try {
                    serverSocketChannel = ServerSocketChannel.open(StandardProtocolFamily.INET);
                    serverSocketChannel.bind(new InetSocketAddress(0));
                    port = serverSocketChannel.socket().getLocalPort();
                } catch (IOException e) {
                    if (isRunning()) log.warn("CommunicationManager(agentId:{}) bind tcp proxyPort error; exception:{}", agentId, e.getMessage());
                    return;
                }

                relayManager.registerProxyPort(agentId, Integer.parseInt(agentPorts[i]), serverSocketChannel, TransportLayerProtocol.TCP);

                proxyPorts[i] = String.valueOf(port);
                agentMap.get(agentId).addTcpProxyPort(Integer.parseInt(agentPorts[i]), Integer.parseInt(proxyPorts[i]));
            }
        }


        CommunicationMsg communicationMsg = new CommunicationMsg();
        communicationMsg.setAgentId(agentId);
        communicationMsg.setOrder(new CommunicationMsg.Method(CommunicationProtocol.BODY_REGISTER_TCP_PROXY_RESPONSE_MSG, proxyPorts));
        sendMessage(agentId, communicationMsg);
    }

    private void registerUdpProxy(RegisterPortRequest registerPortRequest) {
        String agentId = registerPortRequest.getCommunicationMsg().getAgentId();
        String[] proxyPorts = new String[registerPortRequest.getCommunicationMsg().getRequest().getArgs().length];
        if (agentId != null && agentMap.containsKey(agentId)) {
            String[] agentPorts = registerPortRequest.getCommunicationMsg().getRequest().getArgs();
            for (int i = 0; i < agentPorts.length; i++) {
                DatagramChannel datagramChannel = null;
                int port = 0;
                try {
                    datagramChannel = DatagramChannel.open(StandardProtocolFamily.INET);
                    datagramChannel.bind(new InetSocketAddress(0));
                    port = datagramChannel.socket().getLocalPort();
                } catch (IOException e) {
                    if (isRunning()) log.warn("CommunicationManager(agentId:{}) bind udp proxyPort error; exception:{}", agentId, e.getMessage());
                }

                relayManager.registerProxyPort(agentId, Integer.parseInt(agentPorts[i]), datagramChannel, TransportLayerProtocol.UDP);

                proxyPorts[i] = String.valueOf(port);
                agentMap.get(agentId).addUdpProxyPort(Integer.parseInt(agentPorts[i]), Integer.parseInt(proxyPorts[i]));
            }
        }

        CommunicationMsg communicationMsg = new CommunicationMsg();
        communicationMsg.setAgentId(agentId);
        communicationMsg.setOrder(new CommunicationMsg.Method(CommunicationProtocol.BODY_REGISTER_UDP_PROXY_RESPONSE_MSG, proxyPorts));
        sendMessage(agentId, communicationMsg);
    }

    private void registerTcpRelayChannel(RegisterPortRequest registerPortRequest) {
        String agentId = registerPortRequest.getCommunicationMsg().getAgentId();
        int proxiedPort = Integer.parseInt(registerPortRequest.getCommunicationMsg().getRequest().getArgs()[0]);
        String tempId = registerPortRequest.getCommunicationMsg().getRequest().getArgs()[1];

        if (agentId != null && proxiedPort != 0 && agentMap.containsKey(agentId) && tempId != null) {
            relayManager.registerTcpRelayChannel(agentId, proxiedPort, tempId, registerPortRequest.getSocketChannel());
        }

    }

    private void registerUdpRelayChannel(RegisterPortRequest registerPortRequest) {
        String agentId = registerPortRequest.getCommunicationMsg().getAgentId();
        int proxiedPort = Integer.parseInt(registerPortRequest.getCommunicationMsg().getRequest().getArgs()[0]);
        String id = registerPortRequest.getCommunicationMsg().getRequest().getArgs()[1];

        if (agentId != null && proxiedPort != 0 && agentMap.containsKey(agentId) && id != null) {
            relayManager.registerUdpRelayChannel(agentId, proxiedPort, id);
        }
    }

    public void sendMessage(String agentId, CommunicationMsg msg) {
        if (isRunning()) {
            Agent agent = agentMap.get(agentId);
            if (agent != null) {
                CommunicationTask communicationTask = CommunicationTask.buildOrderTask(agentId, msg);
                taskQueue.offer(communicationTask);
                selector.wakeup();
            }
        }
    }

    private boolean closeAgent(String agentId) {
        if (agentId!= null && agentMap.containsKey(agentId)) {
            Agent agent = agentMap.get(agentId);
            agentMap.remove(agentId);

            agent.getTcpProxyPort().forEach((proxiedPort, proxyPort) -> {
                relayManager.closeProxyPort(agentId, TransportLayerProtocol.TCP, proxiedPort);
            });

            agent.getUdpProxyPort().forEach((proxiedPort, proxyPort) -> {
                relayManager.closeProxyPort(agentId, TransportLayerProtocol.UDP, proxiedPort);
            });

            agent.close();

            log.info("Agent(id:{},ip:{}) closed", agentId, agent.getCommunicationSocketChannel().socket().getRemoteSocketAddress());
            return true;
        }

        return false;
    }

    private void closeChannel(Channel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
            }
        }
    }

    public void shutdown() {
        workingStatus = WorkingStatusEnum.STOPPING;

        List<String> agentIds = new ArrayList<>(agentMap.keySet());
        agentIds.forEach(this::closeAgent);

        relayManager.shutdown();

        try {
            selector.close();
        } catch (IOException e) {
        }
        workingStatus = WorkingStatusEnum.STOPPED;
    }

    @Override
    public void destroy() {
        shutdown();
        log.info("NetRelay shutdown gracefully");
    }

    private boolean isRunning() {
        return workingStatus.getCode() <= WorkingStatusEnum.WORKING.getCode();
    }

    private void logError(String msg, Object... objects) {
        log.error(msg, objects);
    }
}
