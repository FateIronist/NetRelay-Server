package top.fateironist.net_relay.core.relay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import top.fateironist.net_relay.core.common.AsyncIoThreadPool;
import top.fateironist.net_relay.core.common.TaskScheduler;
import top.fateironist.net_relay.core.communication.CommunicationManager;
import top.fateironist.net_relay.core.filter.base.ProxyPortFilterChain;
import top.fateironist.net_relay.model.common.enums.WorkingStatusEnum;
import top.fateironist.net_relay.model.communication.CommunicationMsg;
import top.fateironist.net_relay.model.communication.CommunicationProtocol;
import top.fateironist.net_relay.model.filter.ProxyPortRequest;
import top.fateironist.net_relay.model.relay.*;
import top.fateironist.net_relay.model.relay.enums.RelayTaskType;
import top.fateironist.net_relay.model.relay.enums.TransportLayerProtocol;


import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class RelayManager {
    private static final int DEFAULT_RELAY_WORKER_NUM = 8;
    private final AtomicInteger relayWorkerId = new AtomicInteger(0);

    // key = agentId-protocol-proxiedPort
    private final Map<String, WeakReference<RelayWorker>> navigationTable;
    private final List<RelayWorker> relayWorkers;

    private CommunicationManager communicationManager;

    @Autowired
    private ProxyPortFilterChain proxyPortFilterChain;

    public RelayManager() {
        this.navigationTable = new ConcurrentHashMap<>();
        this.relayWorkers = new ArrayList<>();
    }

    public void start(CommunicationManager communicationManager) {
        this.communicationManager = communicationManager;

        for (int i = 0; i < DEFAULT_RELAY_WORKER_NUM; i++) {
            addWorker();
        }
    }

    public void addWorker() {
        RelayWorker relayWorker = new RelayWorker(relayWorkerId.getAndIncrement());
        relayWorker.setWorkingStatus(WorkingStatusEnum.STARTING);

        // 虚拟线程，多路复用核心
        Thread.ofVirtual().name("RelayWorker-" + relayWorker.getId()).inheritInheritableThreadLocals(true)
                .uncaughtExceptionHandler((t, e) -> {
                    if (e instanceof ClosedSelectorException) {
                        if (relayWorker.isRunning()) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        throw new RuntimeException(e);
                    }
                })
                .start(() -> {
                    Selector selector = null;
                    try {
                        selector = Selector.open();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    relayWorker.setSelector(selector);

                    while (relayWorker.isRunning()) {
                        try {
                            long blockingBeginTime = relayWorker.getAverageBlockingTime();
                            selector.select();

                            relayWorker.updateBlockingTime(System.currentTimeMillis() - blockingBeginTime);
                        } catch (IOException e) {
                            if(relayWorker.isRunning()) log.error("RelayWorker-{} select error; exception:{}", relayWorker.getId(), e.getMessage());
                            break;
                        }

                        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                        if (log.isDebugEnabled()) {
                            log.debug("RelayWorker-{} select; length:{}", relayWorker.getId(), selector.selectedKeys().size());
                        }

                        while (iterator.hasNext()) {
                            SelectionKey key = iterator.next();
                            iterator.remove();
                            if (key.isValid() && key.isAcceptable()) {
                                processAcceptable(key, relayWorker);
                            }
                            if (key.isValid() && key.isReadable()) {
                                processReadable(key, relayWorker);
                            }
                            if (key.isValid() && key.isWritable()) {
                                processWritable(key, relayWorker);
                            }
                        }

                        RelayTask relayTask = null;
                        while((relayTask = relayWorker.pollTask()) != null) {
                            processTask(relayTask, relayWorker);
                        }
                    }

                    closeWorker(relayWorker.getId());
                });

        relayWorkers.add(relayWorker);
        relayWorker.setWorkingStatus(WorkingStatusEnum.WORKING);

        // 定期清理或重发临时缓存
        TaskScheduler.scheduleWithFixedRate(() -> {
            if (relayWorker.getWorkingStatus().getCode() <= WorkingStatusEnum.WORKING.getCode()) {
                List<String> delId = new ArrayList<>();
                relayWorker.getTempCache().forEach((k, v) -> {
                    if (System.currentTimeMillis() - v.getCreateTime() > 1000 * 12) {
                        v.close();
                        delId.add(k);
                    }else if (System.currentTimeMillis() - v.getCreateTime() > 1000 * 5) {
                        CommunicationMsg msg = new CommunicationMsg();
                        msg.setOrder(new CommunicationMsg.Method(CommunicationProtocol.BODY_REQUIRE_TCP_RELAY_CHANNEL_MSG, new String[]{v.getProxiedPort().toString(), k}));
                        communicationManager.sendMessage(v.getAgentId(), msg);
                    }
                });

                delId.forEach(id -> relayWorker.getTempCache().remove(id));
            }
        }, 1, 5, TimeUnit.SECONDS);
    }

    //处理接收事件
    private void processAcceptable(SelectionKey key, RelayWorker relayWorker) {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

        ProxyPortAttachment attachment = (ProxyPortAttachment) key.attachment();
        SocketChannel socketChannel = null;
        try {
            socketChannel = serverSocketChannel.accept();
        } catch (IOException e) {
            if (!attachment.isClosed()) log.warn("RelayWorker-{}(agentId:{},localPort:{}) accept error; exception:{}", relayWorker.getId(), attachment.getAgentId(), serverSocketChannel.socket().getLocalPort(), e.getMessage());
            return;
        }

        AtomicBoolean drop = new AtomicBoolean(true);
        ProxyPortRequest proxyPortRequest = ProxyPortRequest.buildTcpRequest(attachment, socketChannel);

        final SocketChannel finalSocketChannel = socketChannel;

        if (log.isDebugEnabled()) {
            log.debug("RelayWorker-{}(agentId:{},localPort:{}) accept; ip:{}",relayWorker.getId(),attachment.getAgentId() , serverSocketChannel.socket().getLocalPort(), socketChannel.socket().getRemoteSocketAddress());
        }

        // 过滤器
        proxyPortFilterChain.startChain(proxyPortRequest, (request -> {
            drop.set(false);
            String tempId = UUID.randomUUID().toString();
            TcpRelayChannelPairAttachment tcpRelayChannelPairAttachment = new TcpRelayChannelPairAttachment(attachment.getAgentId(), attachment.getProxiedPort(), attachment.getProxyPort(), finalSocketChannel);
            tcpRelayChannelPairAttachment.setProxyPortId(genNavigationKey(attachment.getAgentId(), attachment.getProtocol(), attachment.getProxiedPort()));
            relayWorker.putTempCache(tempId, tcpRelayChannelPairAttachment);

            CommunicationMsg msg = new CommunicationMsg();
            msg.setOrder(new CommunicationMsg.Method(CommunicationProtocol.BODY_REQUIRE_TCP_RELAY_CHANNEL_MSG, new String[]{attachment.getProxiedPort().toString(), tempId}));
            communicationManager.sendMessage(attachment.getAgentId(), msg);
        }));

        if (drop.get()) {
            closeChannel(socketChannel);
            return;
        }
    }

    // 处理读事件
    private void processReadable(SelectionKey key, RelayWorker relayWorker) {
        Channel channel = key.channel();

        if (channel instanceof SocketChannel) {

            TcpRelayChannelPairAttachmentWrapper wrapper = (TcpRelayChannelPairAttachmentWrapper) key.attachment();
            TcpRelayChannelPairAttachment attachment = wrapper.getAttachment();

            // 关闭逻辑
            if (!key.isValid() || relayWorker.shouldRelayChannelClose(attachment.getProxyPortId(), attachment.getId())) {
                if (!relayWorker.closeRelayChannel(attachment.getProxyPortId(), attachment.getId())) attachment.close();
                return;
            }

            ByteBuffer buffer = null;
            if (wrapper.isIn()) {
                buffer = attachment.getInBuffer();
            } else {
                buffer = attachment.getOutBuffer();
            }


            boolean isInitial = buffer.position() == 0;

            int len = 0;
            try {
                len = ((SocketChannel) channel).read(buffer);

                if (log.isDebugEnabled()) {
                    log.debug("RelayWorker-{}(agentId:{},localPort:{}) Tcp read; ip:{},len:{}", relayWorker.getId(), attachment.getAgentId() , attachment.getProxyPort(), ((SocketChannel) channel).socket().getRemoteSocketAddress(), len);
                }

                if (log.isTraceEnabled() && len > 0) {
                    ByteBuffer slice = buffer.duplicate();
                    slice.flip();
                    byte[] bytes = new byte[slice.remaining()];
                    slice.get(bytes);
                    System.out.println("-------------------RelayTcpRead------------------");
                    log.trace("RelayWorker-{}(agentId:{},localPort:{}) read Tcp; ip:{}\ncontent:{}", relayWorker.getId(), attachment.getAgentId() , attachment.getProxyPort(), ((SocketChannel) channel).socket().getRemoteSocketAddress(), new String(bytes, StandardCharsets.UTF_8));
                    System.out.println("-------------------------------------------------");
                }
            } catch (IOException e) {
                if (!attachment.isClosed()) log.warn("RelayWorker-{}(agentId:{}, localPort:{}) Tcp read error;ip:{},exception:{}",relayWorker.getId() ,attachment.getAgentId(), attachment.getProxyPort(), ((SocketChannel) channel).socket().getRemoteSocketAddress(), e.getMessage());
                // 关闭逻辑
                if (!relayWorker.closeRelayChannel(attachment.getProxyPortId(), attachment.getId())) attachment.close();
                return;
            }

            if (len == -1) {
                // 关闭逻辑
                if (!relayWorker.closeRelayChannel(attachment.getProxyPortId(), attachment.getId())) attachment.close();
            }else if(len == 0) {

            } else if (len > 0) {
                SocketChannel writeChannel = null;
                try {
                    if (wrapper.isIn()) {
                        if (!buffer.hasRemaining()) {
                            writeChannel = attachment.getTcpRelayChannel();
                            writeChannel.register(relayWorker.getSelector(), SelectionKey.OP_READ | SelectionKey.OP_WRITE, new TcpRelayChannelPairAttachmentWrapper(false, attachment));
                        } else if (isInitial) {
                            TaskScheduler.schedule(() -> {
                                if (attachment.isWriteInTimeout()) {
                                    RelayTask relayTask = new RelayTask(RelayTaskType.TCP_INTERSET_EVENT, wrapper);
                                    relayWorker.submitTask(relayTask);
                                }
                            }, TcpRelayChannelPairAttachment.MTU_AGGREGATION_WAIT_TIME, TimeUnit.MILLISECONDS);
                        }
                   } else {
                        if (!buffer.hasRemaining()) {
                            writeChannel = attachment.getTcpRequestChannel();
                            writeChannel.register(relayWorker.getSelector(), SelectionKey.OP_READ | SelectionKey.OP_WRITE, new TcpRelayChannelPairAttachmentWrapper(true, attachment));
                        } else if (isInitial) {
                            TaskScheduler.schedule(() -> {
                                if (attachment.isWriteOutTimeout()) {
                                    RelayTask relayTask = new RelayTask(RelayTaskType.TCP_INTERSET_EVENT, wrapper);
                                    relayWorker.submitTask(relayTask);
                                }
                            }, TcpRelayChannelPairAttachment.MTU_AGGREGATION_WAIT_TIME, TimeUnit.MILLISECONDS);
                        }
                    }
                } catch (ClosedChannelException e) {
                    if (!relayWorker.closeRelayChannel(attachment.getProxyPortId(), attachment.getId())) attachment.close();
                    if (!attachment.isClosed()) log.warn("RelayWorker-{}(agentId:{}, localPort:{}) register Tcp writing error;ip:{},exception:{}",relayWorker.getId() ,attachment.getAgentId(), attachment.getProxyPort(), ((SocketChannel)channel).socket().getRemoteSocketAddress(), e.getMessage());
                    return;
                }
            }

        } else if (channel instanceof DatagramChannel) {

            Object attachment = key.attachment();
            DatagramChannel datagramChannel = (DatagramChannel) channel;


            // 若为代理端口Channel
            if (attachment instanceof ProxyPortAttachment) {

                ProxyPortAttachment proxyPortAttachment = (ProxyPortAttachment) attachment;

                ByteBuffer tempBuffer = proxyPortAttachment.getUdpTempBuffer();

                SocketAddress address = null;
                try {
                    address = datagramChannel.receive(tempBuffer);
                } catch (IOException e) {
                    if (!proxyPortAttachment.isClosed())
                        log.warn("RelayWorker-{}(agentId:{}, localPort:{}) ProxyPort Udp receive error;ip:{},exception:{}", relayWorker.getId(), proxyPortAttachment.getAgentId(), proxyPortAttachment.getProxyPort(), address, e.getMessage());
                    return;
                }

                // 区分内外部UdpChannel
                if (proxyPortAttachment.isExternalUdpChannel(address)) {

                    String id = proxyPortAttachment.genUdpRelayChannelId(address);

                    UdpRelayChannelAttachment udpRelayChannelAttachment = proxyPortAttachment.getUdpRelayChannel(id);

                    if (udpRelayChannelAttachment == null) {
                        DatagramChannel udpChannel = null;
                        try {
                            udpChannel = DatagramChannel.open(StandardProtocolFamily.INET);
                            udpChannel.bind(new InetSocketAddress(0));
                            udpChannel.configureBlocking(true);
                        } catch (IOException e) {
                            if (!proxyPortAttachment.isClosed())
                                log.warn("RelayWorker-{}(agentId:{}, localPort:{}) ProxyPort Udp relayChannel bind error;ip:{},exception:{}", relayWorker.getId(), proxyPortAttachment.getAgentId(), proxyPortAttachment.getProxyPort(), address, e.getMessage());
                            return;
                        }

                        String innerId = proxyPortAttachment.genUdpRelayChannelId(new InetSocketAddress("127.0.0.1", udpChannel.socket().getLocalPort()));

                        UdpRelayChannelAttachment udpAttachment = new UdpRelayChannelAttachment(proxyPortAttachment.getAgentId(), proxyPortAttachment.getId(), proxyPortAttachment.getProxiedPort(), proxyPortAttachment.getProxyPort());
                        udpAttachment.setId(id);
                        udpAttachment.setInnerId(innerId);
                        udpAttachment.setDatagramChannel(udpChannel);
                        udpAttachment.setBindPort(udpChannel.socket().getLocalPort());
                        udpAttachment.setRemoteAddress(address);

                        final DatagramChannel finalChannel = udpChannel;

                        CommunicationMsg msg = new CommunicationMsg();
                        msg.setAgentId(udpAttachment.getAgentId());
                        msg.setOrder(new CommunicationMsg.Method(CommunicationProtocol.BODY_REQUIRE_UDP_RELAY_CHANNEL_MSG, new String[]{String.valueOf(udpAttachment.getProxiedPort()), String.valueOf(udpChannel.socket().getLocalPort()), String.valueOf(udpAttachment.getId())}));
                        communicationManager.sendMessage(udpAttachment.getAgentId(), msg);

                        proxyPortAttachment.registerUdpRelayChannel(id, udpAttachment);
                        proxyPortAttachment.registerUdpRelayChannel(innerId, udpAttachment);

                        AsyncIoThreadPool.executeWithTimeoutIgnoreException(() -> {
                            try {
                                ByteBuffer buffer = ByteBuffer.allocateDirect(RelayChannelAttachment.DEFAULT_UDP_BUFFER_SIZE);

                                SocketAddress socketAddress = finalChannel.receive(buffer);
                                buffer.flip();
                                byte[] bytes = new byte[buffer.remaining()];
                                buffer.get(bytes);
                                CommunicationMsg resMsg = CommunicationMsg.parse(bytes);
                                if (resMsg.getAgentId().equals(udpAttachment.getAgentId()) && resMsg.getRequest().getName().equals(CommunicationProtocol.BODY_UDP_PENETRATION_MEG)) {
                                    udpAttachment.setProxiedRelayAddress(socketAddress);
                                    udpAttachment.setPenetration(true);

                                    buffer.flip();

                                    CommunicationMsg communicationMsg = new CommunicationMsg();
                                    communicationMsg.setAgentId(udpAttachment.getAgentId());
                                    communicationMsg.setOrder(new CommunicationMsg.Method(CommunicationProtocol.BODY_UDP_PENETRATION_RESPONSE_MSG, null));

                                    buffer.put(communicationMsg.buildBytesOrderMessage());
                                    buffer.flip();

                                    finalChannel.send(buffer, socketAddress);

                                    finalChannel.configureBlocking(false);
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }, 1, TimeUnit.MINUTES, (e) -> {
                            relayWorker.closeRelayChannel(udpAttachment.getProxyPortId(), udpAttachment.getId());
                            log.warn("RelayWorker-{}(agentId:{}, localPort:{}) Udp relayChannel register error;ip:{},exception:{}", relayWorker.getId(), proxyPortAttachment.getAgentId(), proxyPortAttachment.getProxyPort(), udpAttachment.getRemoteAddress(), e.getMessage());
                        });

                        udpRelayChannelAttachment = udpAttachment;
                    }

                    tempBuffer.flip();

                    if (log.isDebugEnabled()) {
                        log.debug("RelayWorker-{}(agentId:{},localPort:{}) ProxyPort Udp receive; ip:{},length:{}", relayWorker.getId(), proxyPortAttachment.getAgentId(), proxyPortAttachment.getProxyPort(), address, tempBuffer.remaining());
                    }

                    if (log.isTraceEnabled()) {
                        ByteBuffer slice = tempBuffer.duplicate();
                        byte[] bytes = new byte[slice.remaining()];
                        slice.get(bytes);
                        System.out.println("-------------------RelayUdpReceive------------------");
                        log.trace("RelayWorker-{}(agentId:{},localPort:{}) ProxyPort Udp receive; ip:{},\ncontent:{}", relayWorker.getId(), proxyPortAttachment.getAgentId(), proxyPortAttachment.getProxyPort(), address, new String(bytes, StandardCharsets.UTF_8));
                        System.out.println("-----------------------------------------------------");
                    }

                    try {
                        if (!udpRelayChannelAttachment.isPenetration()) {
                            ByteBuffer buffer = ByteBuffer.allocateDirect(tempBuffer.remaining());
                            buffer.put(tempBuffer);
                            udpRelayChannelAttachment.setInitialBufferQueueEmpty(false);
                            udpRelayChannelAttachment.getInitialBufferQueue().offer(buffer);
                        } else {
                            datagramChannel.send(tempBuffer, new InetSocketAddress("127.0.0.1", udpRelayChannelAttachment.getBindPort()));
                        }
                    } catch (IOException e) {
                        if (e.getMessage().toLowerCase().contains("buffer")
                                || e.getMessage().toLowerCase().contains("space")
                                || e.getMessage().toLowerCase().contains("unavailable")
                                || e.getMessage().toLowerCase().contains("full")
                                || e.getMessage().toLowerCase().contains("resource")
                                || e.getMessage().toLowerCase().contains("queue")
                        ) {
                            ByteBuffer buffer = ByteBuffer.allocateDirect(tempBuffer.remaining());
                            buffer.put(tempBuffer);
                            udpRelayChannelAttachment.getInitialBufferQueue().offer(buffer);
                        } else {
                            if (!udpRelayChannelAttachment.shouldClose())
                                log.warn("RelayWorker-{}(agentId:{}, localPort:{}) ProxyPort send Udp message error;ip:{},exception:{}", relayWorker.getId(), udpRelayChannelAttachment.getAgentId(), udpRelayChannelAttachment.getProxyPort(), "/127.0.0.1:" + udpRelayChannelAttachment.getBindPort(), e.getMessage());
                            tempBuffer.clear();
                            return;
                        }
                    } catch (Exception e) {
                        if (!udpRelayChannelAttachment.shouldClose())
                            log.warn("RelayWorker-{}(agentId:{}, localPort:{}) ProxyPort send Udp message error;ip:{},exception:{}", relayWorker.getId(), udpRelayChannelAttachment.getAgentId(), udpRelayChannelAttachment.getProxyPort(), "/127.0.0.1:" + udpRelayChannelAttachment.getBindPort(), e.getMessage());
                        if (!relayWorker.closeRelayChannel(udpRelayChannelAttachment.getProxyPortId(), udpRelayChannelAttachment.getId()))
                            udpRelayChannelAttachment.close();
                        tempBuffer.clear();
                        return;
                    }
                } else {
                    tempBuffer.flip();

                    String innerId = proxyPortAttachment.genUdpRelayChannelId(address);

                    UdpRelayChannelAttachment udpRelayChannelAttachment = proxyPortAttachment.getUdpRelayChannel(innerId);

                    if (udpRelayChannelAttachment != null) {
                        try {
                            datagramChannel.send(tempBuffer, udpRelayChannelAttachment.getRemoteAddress());
                        } catch (IOException e) {
                            // 当写缓冲区不够则入队，注册写事件
                            ByteBuffer buffer = ByteBuffer.allocateDirect(tempBuffer.remaining());
                            buffer.put(tempBuffer);
                            proxyPortAttachment.addUdpOutBuffer(address, buffer);
                            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        } catch (Exception e) {
                            if (!udpRelayChannelAttachment.shouldClose())
                                log.warn("RelayWorker-{}(agentId:{}, localPort:{}) ProxyPort send Udp message error;ip:{},exception:{}", relayWorker.getId(), udpRelayChannelAttachment.getAgentId(), udpRelayChannelAttachment.getProxyPort(), address.toString(), e.getMessage());
                            tempBuffer.clear();
                            return;
                        }
                    }
                }

            tempBuffer.clear();

        } else if (attachment instanceof UdpRelayChannelAttachment) {
            UdpRelayChannelAttachment udpRelayChannelAttachment = (UdpRelayChannelAttachment) attachment;

            ByteBuffer buffer = udpRelayChannelAttachment.getBuffer();

            SocketAddress address = null;

            try {
                udpRelayChannelAttachment.refresh();
                address = datagramChannel.receive(buffer);
            } catch (IOException e) {
                if (!udpRelayChannelAttachment.shouldClose())
                    log.warn("RelayWorker-{}(agentId:{}, localPort:{}) RelayChannel receive Udp message error;ip:{},exception:{}", relayWorker.getId(), udpRelayChannelAttachment.getAgentId(), udpRelayChannelAttachment.getProxyPort(), address.toString(), e.getMessage());
                return;
            }

            buffer.flip();

            if (log.isDebugEnabled()) {
                log.debug("RelayWorker-{}(agentId:{},localPort:{}) RelayChannel Udp receive; ip:{},length:{}", relayWorker.getId(), udpRelayChannelAttachment.getAgentId(), udpRelayChannelAttachment.getProxyPort(), address, buffer.remaining());
            }

            if (ProxyPortAttachment.isExternalUdpChannel(address)) {
                if (address.toString().equals(udpRelayChannelAttachment.getProxiedRelayAddress().toString())) {
                    try {
                        udpRelayChannelAttachment.refresh();
                        datagramChannel.send(buffer, new InetSocketAddress("127.0.0.1", udpRelayChannelAttachment.getProxyPort()));
                    } catch (IOException e) {
                        udpRelayChannelAttachment.getInBufferOrCreate().put(buffer);
                        key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    } catch (Exception e) {
                        if (!udpRelayChannelAttachment.shouldClose())
                            log.warn("RelayWorker-{}(agentId:{}, localPort:{}) relayChannel send Udp message error;ip:{},exception:{}", relayWorker.getId(), udpRelayChannelAttachment.getAgentId(), udpRelayChannelAttachment.getProxyPort(), "/127.0.0.1" + udpRelayChannelAttachment.getProxyPort(), e.getMessage());
                        if (!relayWorker.closeRelayChannel(udpRelayChannelAttachment.getProxyPortId(), udpRelayChannelAttachment.getId()))
                            udpRelayChannelAttachment.close();
                        buffer.clear();
                        return;
                    }
                }

            } else {
                if (ProxyPortAttachment.extractPort(address).equals(udpRelayChannelAttachment.getProxyPort())) {
                    try {
                        udpRelayChannelAttachment.refresh();
                        if (udpRelayChannelAttachment.isInitialBufferQueueEmpty()) {
                            datagramChannel.send(buffer, udpRelayChannelAttachment.getProxiedRelayAddress());
                        } else {
                            ByteBuffer queueBuffer = ByteBuffer.allocateDirect(buffer.remaining());
                            queueBuffer.put(buffer);
                            udpRelayChannelAttachment.setInitialBufferQueueEmpty(false);
                            udpRelayChannelAttachment.getInitialBufferQueue().offer(queueBuffer);
                        }

                    } catch (IOException e) {
                        udpRelayChannelAttachment.getOutBufferOrCreate().put(buffer);
                        key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    } catch (Exception e) {
                        if (!udpRelayChannelAttachment.shouldClose())
                            log.warn("RelayWorker-{}(agentId:{}, localPort:{}) relayChannel send Udp message error;ip:{},exception:{}", relayWorker.getId(), udpRelayChannelAttachment.getAgentId(), udpRelayChannelAttachment.getProxyPort(), udpRelayChannelAttachment.getProxiedRelayAddress().toString(), e.getMessage());
                        if (!relayWorker.closeRelayChannel(udpRelayChannelAttachment.getProxyPortId(), udpRelayChannelAttachment.getId()))
                            udpRelayChannelAttachment.close();
                        buffer.clear();
                        return;
                    }
                }
            }

            buffer.clear();
        }
        } else {
            // 关闭逻辑
        }

    }


    // 处理写事件
    private void processWritable(SelectionKey key, RelayWorker relayWorker) {

        Channel channel = key.channel();

        if (channel instanceof SocketChannel) {
            TcpRelayChannelPairAttachmentWrapper wrapper = (TcpRelayChannelPairAttachmentWrapper) key.attachment();
            TcpRelayChannelPairAttachment attachment = wrapper.getAttachment();
            // 关闭逻辑
            if (!key.isValid() || relayWorker.shouldRelayChannelClose(attachment.getProxyPortId(), attachment.getId())) {
                if (!relayWorker.closeRelayChannel(attachment.getProxyPortId(), attachment.getId())) attachment.close();
                return;
            }

            ByteBuffer buffer = null;
            if (wrapper.isIn()) {
                buffer = attachment.getOutBuffer();
                attachment.setOutBufferLastWriteTime(System.currentTimeMillis());
            } else {
                buffer = attachment.getInBuffer();
                attachment.setInBufferLastWriteTime(System.currentTimeMillis());
            }

            buffer.flip();
            try {
                if (log.isDebugEnabled()) {
                    log.debug("RelayWorker-{}(agentId:{},localPort:{}) Tcp write; ip:{},len:{}", relayWorker.getId(), attachment.getAgentId() , attachment.getProxyPort(), ((SocketChannel) channel).socket().getRemoteSocketAddress(), buffer.remaining());
                }
                if (log.isTraceEnabled()) {
                    ByteBuffer slice = buffer.slice();
                    byte[] bytes = new byte[slice.remaining()];
                    slice.get(bytes);
                    System.out.println("-------------------RelayTcpWrite------------------");
                    log.trace("RelayWorker-{}(agentId:{},localPort:{}) Tcp write; ip:{}\ncontent:{}", relayWorker.getId(), attachment.getAgentId() , attachment.getProxyPort(), ((SocketChannel) channel).socket().getRemoteSocketAddress(), new String(bytes, StandardCharsets.UTF_8));
                    System.out.println("--------------------------------------------------");
                }

                ((SocketChannel) channel).write(buffer);

            } catch (IOException e) {
                if (!attachment.isClosed()) log.warn("RelayWorker-{}(agentId:{}, localPort:{}) Tcp write error;ip:{},exception:{}",relayWorker.getId() ,attachment.getAgentId(), attachment.getProxyPort(), ((SocketChannel) channel).socket().getRemoteSocketAddress(), e.getMessage());
                if (!relayWorker.closeRelayChannel(attachment.getProxyPortId(), attachment.getId())) attachment.close();
                return;
            }

            if (!buffer.hasRemaining()) {
                buffer.clear();
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
//                if (!((wrapper.isIn() && attachment.isOutBufferWriteContinue()) || (!wrapper.isIn() && attachment.isInBufferWriteContinue()))) {
//                    key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
//                }
                return;
            }

            buffer.flip();
        } else if (channel instanceof DatagramChannel) {
            DatagramChannel datagramChannel = (DatagramChannel) channel;
            // 处理端口速度不够时候的缓存
            Object attachment = key.attachment();

            if (attachment instanceof ProxyPortAttachment) {
                ProxyPortAttachment proxyPortAttachment = (ProxyPortAttachment) attachment;

                ProxyPortAttachment.UdpOutWriteTask task = null;

                while((task = proxyPortAttachment.pollUdpOutBuffer()) != null) {
                    try {

                        datagramChannel.send(task.getBuffer(), task.getAddress());

                        if (log.isDebugEnabled()) {
                            log.debug("RelayWorker-{}(agentId:{},localPort:{}) ProxyPort Udp queue write; ip:{},len:{}", relayWorker.getId(), proxyPortAttachment.getAgentId(), proxyPortAttachment.getProxyPort(), task.getAddress().toString(), task.getBuffer().remaining());
                        }

                    } catch (IOException e) {
                        proxyPortAttachment.getOutBufferQueue().offerFirst(task);
                        break;
                    }
                }

                if (proxyPortAttachment.getOutBufferQueue().size() == 0) {
                    key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                }

            } else if (attachment instanceof UdpRelayChannelAttachment) {
                UdpRelayChannelAttachment udpRelayChannelAttachment = (UdpRelayChannelAttachment) attachment;
                // 处理intiQueue
                ByteBuffer buffer = null;

                while((buffer = udpRelayChannelAttachment.getInitialBufferQueue().poll()) != null) {
                    try {
                        buffer.flip();
                        udpRelayChannelAttachment.refresh();
                        datagramChannel.send(buffer, udpRelayChannelAttachment.getProxiedRelayAddress());
                    } catch (IOException e) {
                        buffer.flip();
                        udpRelayChannelAttachment.getInitialBufferQueue().offerFirst(buffer);
                        break;
                    } catch (Exception e) {
                        e.printStackTrace();
                        if (!udpRelayChannelAttachment.shouldClose()) log.warn("RelayWorker-{}(agentId:{},localPort:{}) RelayChannel Udp write error;ip:{},exception:{}", relayWorker.getId(), udpRelayChannelAttachment.getAgentId(), udpRelayChannelAttachment.getProxyPort(), "/127.0.0.1:" + udpRelayChannelAttachment.getProxyPort(), e.getMessage());
                        if (!relayWorker.closeRelayChannel(udpRelayChannelAttachment.getProxyPortId(), udpRelayChannelAttachment.getId())) udpRelayChannelAttachment.close();
                        return;
                    }
                }

                if (buffer == null) {
                    udpRelayChannelAttachment.setInitialBufferQueueEmpty(true);
                }

                try {

                    ByteBuffer inBuffer = udpRelayChannelAttachment.getInBuffer();
                    ByteBuffer outBuffer = udpRelayChannelAttachment.getOutBuffer();

                    try {
                        if (inBuffer != null) {
                            inBuffer.flip();
                            udpRelayChannelAttachment.refresh();
                            datagramChannel.send(inBuffer, new InetSocketAddress("127.0.0.1", udpRelayChannelAttachment.getProxyPort()));
                        }
                        if (outBuffer != null) {
                            outBuffer.flip();
                            udpRelayChannelAttachment.refresh();
                            datagramChannel.send(outBuffer, udpRelayChannelAttachment.getProxiedRelayAddress());
                        }
                    } catch (IOException e) {
                    } catch (Exception e) {
                        e.printStackTrace();
                        if (!udpRelayChannelAttachment.shouldClose())
                            log.warn("RelayWorker-{}(agentId:{},localPort:{}) RelayChannel Udp write error;ip:{},exception:{}", relayWorker.getId(), udpRelayChannelAttachment.getAgentId(), udpRelayChannelAttachment.getProxyPort(), "双向", e.getMessage());
                        if (!relayWorker.closeRelayChannel(udpRelayChannelAttachment.getProxyPortId(), udpRelayChannelAttachment.getId()))
                            udpRelayChannelAttachment.close();
                        udpRelayChannelAttachment.setInBuffer(null);
                        udpRelayChannelAttachment.setOutBuffer(null);
                        return;
                    }

                    if (buffer == null && (udpRelayChannelAttachment.getInBuffer() == null || !udpRelayChannelAttachment.getInBuffer().hasRemaining()) && (udpRelayChannelAttachment.getOutBuffer() == null || !udpRelayChannelAttachment.getOutBuffer().hasRemaining())) {
                        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                    } else {
                        if (udpRelayChannelAttachment.getInBuffer() != null) inBuffer.flip();
                        if (udpRelayChannelAttachment.getOutBuffer() != null) outBuffer.flip();
                    }

                    if (udpRelayChannelAttachment.getInBuffer() != null && !udpRelayChannelAttachment.getInBuffer().hasRemaining()) udpRelayChannelAttachment.setInBuffer(null);
                    if (udpRelayChannelAttachment.getOutBuffer() != null && !udpRelayChannelAttachment.getOutBuffer().hasRemaining()) udpRelayChannelAttachment.setOutBuffer(null);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }

        } else {
        }
    }

    // 处理任务队列
    private void processTask(RelayTask relayTask, RelayWorker relayWorker) {

        if (log.isDebugEnabled()) {
            ProxyPortAttachment proxyPortAttachment = relayTask.getProxyPortAttachment();
            if (proxyPortAttachment != null) {
                log.debug("RelayWorker-{}(agentId:{},localPort:{}) process task;\ntask:{}", relayWorker.getId(), relayTask.getProxyPortAttachment().getAgentId(), relayTask.getProxyPortAttachment().getProxyPort(), relayTask);
            } else if (relayTask.getRelayChannelPairAttachment() != null) {
                RelayChannelAttachment relayChannelPairAttachment = relayTask.getRelayChannelPairAttachment();
                log.debug("RelayWorker-{}(agentId:{},localPort:{},remotePort:{}) process task;\ntask:{}", relayWorker.getId(), relayChannelPairAttachment.getAgentId(), relayChannelPairAttachment.getProxyPort(), relayChannelPairAttachment.getProxiedPort(), relayTask);
            } else if (relayTask.getTcpRelayChannelPairAttachmentWrapper() != null) {
                TcpRelayChannelPairAttachmentWrapper tcpRelayChannelPairAttachmentWrapper = relayTask.getTcpRelayChannelPairAttachmentWrapper();
                log.debug("RelayWorker-{}(agentId:{},localPort:{},remotePort:{}) process task;\ntask:{}", relayWorker.getId(), tcpRelayChannelPairAttachmentWrapper.getAttachment().getAgentId(), tcpRelayChannelPairAttachmentWrapper.getAttachment().getProxyPort(), tcpRelayChannelPairAttachmentWrapper.getAttachment().getProxiedPort(), relayTask);
            }

        }

        switch (relayTask.getTaskType()) {
            case PROXY_PORT_REGISTER:
                ProxyPortAttachment proxyPortAttachment = relayTask.getProxyPortAttachment();
                try {
                    switch (proxyPortAttachment.getProtocol()) {
                        case TCP:
                            ServerSocketChannel tcpChannel = (ServerSocketChannel) proxyPortAttachment.getProxyChannel();
                            tcpChannel.configureBlocking(false);
                            proxyPortAttachment.setSelectionKey(tcpChannel.register(relayWorker.getSelector(), SelectionKey.OP_ACCEPT, proxyPortAttachment));
                            break;
                        case UDP:
                            DatagramChannel udpChannel = (DatagramChannel) proxyPortAttachment.getProxyChannel();
                            udpChannel.configureBlocking(false);
                            proxyPortAttachment.setSelectionKey(udpChannel.register(relayWorker.getSelector(), SelectionKey.OP_READ, proxyPortAttachment));

                            proxyPortAttachment.initUdpBuffer();
                            break;
                        default:
                            proxyPortAttachment.close();
                            break;
                    }

                    String key = genNavigationKey(proxyPortAttachment.getAgentId(), proxyPortAttachment.getProtocol(), proxyPortAttachment.getProxiedPort());
                    relayWorker.registerProxyPort(key, proxyPortAttachment);
                    navigationTable.put(key, new WeakReference<>(relayWorker));

                } catch (IOException e) {
                    if (relayWorker.isRunning())
                        log.warn("RelayWorker-{}(agentId:{}, localPort:{}) register proxyPort error; exception:{}", relayWorker.getId(), proxyPortAttachment.getAgentId(), proxyPortAttachment.getProxyPort(), e.getMessage());
                    proxyPortAttachment.close();
                    return;
                }
                break;
            case OUT_CHANNEL_REGISTER:
                RelayChannelAttachment relayChannelPairAttachment = relayTask.getRelayChannelPairAttachment();

                try {
                    switch (relayChannelPairAttachment.getProtocol()) {
                        case TCP:
                            TcpRelayChannelPairAttachment tcpAttachment = (TcpRelayChannelPairAttachment) relayChannelPairAttachment;
                            SocketChannel outSocketChannel = tcpAttachment.getTcpRelayChannel();
                            // fixme
                            outSocketChannel.socket().setTcpNoDelay(true);
                            outSocketChannel.configureBlocking(false);
                            TcpRelayChannelPairAttachmentWrapper outWrapper = new TcpRelayChannelPairAttachmentWrapper(false, tcpAttachment);
                            outSocketChannel.register(relayWorker.getSelector(), SelectionKey.OP_READ, outWrapper);

                            SocketChannel inSocketChannel = tcpAttachment.getTcpRequestChannel();
                            // fixme
                            inSocketChannel.socket().setTcpNoDelay(true);
                            inSocketChannel.configureBlocking(false);
                            TcpRelayChannelPairAttachmentWrapper inWrapper = new TcpRelayChannelPairAttachmentWrapper(true, tcpAttachment);
                            inSocketChannel.register(relayWorker.getSelector(), SelectionKey.OP_READ, inWrapper);

                            ProxyPortAttachment attachment = relayWorker.getProxyPort(relayChannelPairAttachment.getProxyPortId());
                            attachment.registerTcpRelayChannel(tcpAttachment);
                            break;
                        case UDP:
                            UdpRelayChannelAttachment udpAttachment = (UdpRelayChannelAttachment) relayChannelPairAttachment;
                            DatagramChannel udpChannel = udpAttachment.getDatagramChannel();
                            udpChannel.configureBlocking(false);

                            udpChannel.register(relayWorker.getSelector(), SelectionKey.OP_READ | SelectionKey.OP_WRITE, udpAttachment);
                        default:
                            break;
                    }
                } catch (IOException e) {
                    if (relayWorker.isRunning())
                        log.warn("RelayWorker-{}(agentId:{}, localPort:{}) register outChannel error; exception:{}", relayWorker.getId(), relayChannelPairAttachment.getAgentId(), relayChannelPairAttachment.getProxyPort(), e.getMessage());
                    return;
                }
                break;
            case TCP_INTERSET_EVENT:
                TcpRelayChannelPairAttachmentWrapper wrapper = relayTask.getTcpRelayChannelPairAttachmentWrapper();
                TcpRelayChannelPairAttachment attachment = wrapper.getAttachment();
                if (wrapper.isIn() && !attachment.isClosed()) {
                    SocketChannel writeChannel = attachment.getTcpRelayChannel();
                    try {
                        writeChannel.register(relayWorker.getSelector(), SelectionKey.OP_READ | SelectionKey.OP_WRITE, new TcpRelayChannelPairAttachmentWrapper(false, attachment));
                    } catch (ClosedChannelException e) {
                        if (relayWorker.isRunning()) log.warn("RelayWorker-{}(agentId:{}, localPort:{}) register inChannel error; exception:{}", relayWorker.getId(), attachment.getAgentId(), attachment.getProxyPort(), e.getMessage());
                    }
                }else if (!wrapper.isIn() && !attachment.isClosed()){
                    SocketChannel writeChannel = attachment.getTcpRequestChannel();
                    try {
                        writeChannel.register(relayWorker.getSelector(), SelectionKey.OP_READ | SelectionKey.OP_WRITE, new TcpRelayChannelPairAttachmentWrapper(true, attachment));
                    } catch (ClosedChannelException e) {
                        if (relayWorker.isRunning()) log.warn("RelayWorker-{}(agentId:{}, localPort:{}) register outChannel error; exception:{}", relayWorker.getId(), attachment.getAgentId(), attachment.getProxyPort(), e.getMessage());
                    }
                }
            }
    }

    public void registerProxyPort(String agentId, Integer proxiedPort, Channel channel, TransportLayerProtocol protocol) {
        String key = genNavigationKey(agentId, protocol, proxiedPort);

        relayWorkers.sort(Comparator.comparingLong(RelayWorker::getAverageBlockingTime).reversed());
        RelayWorker relayWorker = relayWorkers.get(0);

        ProxyPortAttachment attachment = ProxyPortAttachment.builder()
                .id(key)
                .agentId(agentId)
                .protocol(protocol)
                .proxiedPort(proxiedPort)
                .proxyChannel(channel)
                .build();

        if (protocol.equals(TransportLayerProtocol.TCP)) {
            attachment.setProxyPort(((ServerSocketChannel) channel).socket().getLocalPort());
        } else if (protocol.equals(TransportLayerProtocol.UDP)) {
            attachment.setProxyPort(((DatagramChannel) channel).socket().getLocalPort());
        } else {
            closeChannel(channel);
            return;
        }

        RelayTask task = new RelayTask(RelayTaskType.PROXY_PORT_REGISTER, attachment);
        relayWorker.submitTask(task);
    }

    public void registerTcpRelayChannel(String agentId, Integer proxiedPort, String tempId, SocketChannel channel) {
        String key = genNavigationKey(agentId, TransportLayerProtocol.TCP, proxiedPort);
        RelayWorker relayWorker = navigationTable.get(key) == null ? null : navigationTable.get(key).get();

        if (relayWorker != null) {

            TcpRelayChannelPairAttachment attachment = relayWorker.getTempCache(tempId);
            attachment.setRelayChannel(channel);

            RelayTask task = new RelayTask(RelayTaskType.OUT_CHANNEL_REGISTER, attachment);
            relayWorker.submitTask(task);
        } else {
            navigationTable.remove(key);
        }
    }

    public void registerUdpRelayChannel(String agentId, Integer proxiedPort, String id) {
            String key = genNavigationKey(agentId, TransportLayerProtocol.UDP, proxiedPort);

            RelayWorker relayWorker = navigationTable.get(key) == null ? null : navigationTable.get(key).get();

            if (relayWorker != null) {
                ProxyPortAttachment proxyPortAttachment = relayWorker.getProxyPort(key);
                if (proxyPortAttachment != null) {
                    UdpRelayChannelAttachment udpRelayChannelAttachment = proxyPortAttachment.getUdpRelayChannel(id);

                    RelayTask task = new RelayTask(RelayTaskType.OUT_CHANNEL_REGISTER, udpRelayChannelAttachment);
                    relayWorker.submitTask(task);
                } else {
                    relayWorker.closeProxyPort(key);
                }
            } else {
                navigationTable.remove(key);
            }
    }

    public static String genNavigationKey(String agentId, TransportLayerProtocol protocol, Integer proxiedPort) {
        return agentId + "-" + protocol + "-" + proxiedPort;
    }

    public void shutdown() {
        Iterator<RelayWorker> iterator = relayWorkers.iterator();
        while (iterator.hasNext()) {
            RelayWorker relayWorker = iterator.next();
            relayWorker.close().forEach(proxyPortId -> {
                navigationTable.remove(proxyPortId);
            });
            iterator.remove();
        }
    }

    public void closeWorker(Integer id) {
        Iterator<RelayWorker> iterator = relayWorkers.iterator();
        while (iterator.hasNext()) {
            RelayWorker relayWorker = iterator.next();
            if (relayWorker.getId() == id) {
                relayWorker.close().forEach(proxyPortId -> {
                    navigationTable.remove(proxyPortId);
                });
                iterator.remove();
                break;
            }
        }
    }

    public void closeProxyPort(String agentId, TransportLayerProtocol protocol, Integer proxiedPort) {
        String key = genNavigationKey(agentId, protocol, proxiedPort);
        closeProxyPort(key);
    }

    public void closeProxyPort(String key) {
        RelayWorker relayWorker = navigationTable.get(key) == null ? null : navigationTable.get(key).get();
        if (relayWorker != null) {
            navigationTable.remove(key);
            relayWorker.closeProxyPort(key);
        }

    }

    // 关闭逻辑
    private void closeChannel(Channel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
            }
        }
    }



}
