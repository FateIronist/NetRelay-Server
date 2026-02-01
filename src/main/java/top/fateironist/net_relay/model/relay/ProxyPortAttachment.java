package top.fateironist.net_relay.model.relay;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import top.fateironist.net_relay.model.relay.enums.TransportLayerProtocol;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;
import java.util.*;

/**
 * 该类属于历史遗留类，暂时未进行tcp、udp拆分
 */
@Data
@Builder
public class ProxyPortAttachment {
    public static final int DEFAULT_BUFFER_SIZE = 1472;

    private TransportLayerProtocol protocol;
    private String agentId;
    // tcp为uuid，udp为socketAddress
    // 因为tcp时有状态的有连接的没有区分的必要，而udp必须根据接收到的socketAddress进行区分
    private String id;
    private Integer proxiedPort;
    private Integer proxyPort;

    private SelectionKey selectionKey;

    @Builder.Default
    private boolean closed = false;

    private ByteBuffer udpTempBuffer;
    private Deque<UdpOutWriteTask> outBufferQueue;

    private Channel proxyChannel;
    @Builder.Default
    private Map<String, RelayChannelAttachment> relayChannels = new HashMap<>();

    public void initUdpBuffer() {
        this.udpTempBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
        outBufferQueue = new ArrayDeque<>(100);
    }

    @Data
    @AllArgsConstructor
    public static class UdpOutWriteTask {
        private SocketAddress address;
        private ByteBuffer buffer;
    }

    public void addUdpOutBuffer(SocketAddress address, ByteBuffer buffer) {
        UdpOutWriteTask task = new UdpOutWriteTask(address, buffer);
        outBufferQueue.add(task);
    }

    public UdpOutWriteTask pollUdpOutBuffer() {
        return outBufferQueue.poll();
    }

    /**
     * 这里由于最初为了提高性能，直接让中转udpChannel和代理端口udpChannel通信
     * 导致代理端口udpChannel无法区分外部和内部的udpChannel
     * 因此这里规定：
     * 内部udpChannel address 为 回环地址，并且端口号为自由段端口号(三种主流操作系统并集 32768~65535)
     */
    public static boolean isExternalUdpChannel(SocketAddress address) {
        InetSocketAddress socketAddress = (InetSocketAddress) address;
        if (socketAddress.getAddress().isLoopbackAddress() && socketAddress.getPort() > 32767 && socketAddress.getPort() < 65536) {
            return false;
        }
        return true;
    }

    public static Integer extractPort(SocketAddress address) {
        return Integer.valueOf(address.toString().split(":")[1]);
    }
    
    public static String genUdpRelayChannelId(SocketAddress address) {
        return address.toString();
    }

    public boolean isRelayChannelPairClosed(String id) {
        if (protocol.equals(TransportLayerProtocol.TCP)) {
            return relayChannels.get(id) == null || ((TcpRelayChannelPairAttachment)relayChannels.get(id)).isClosed();
        } else {
            return relayChannels.get(id) == null || ((UdpRelayChannelAttachment)relayChannels.get(id)).shouldClose();
        }
    }

    public void registerTcpRelayChannel(RelayChannelAttachment relayChannel) {
        if (protocol.equals(TransportLayerProtocol.TCP)) {
            String id = UUID.randomUUID().toString();
            relayChannel.setId(id);
            relayChannels.put(relayChannel.getId(), relayChannel);
        }
    }

    public void registerUdpRelayChannel(String id,RelayChannelAttachment relayChannel) {
        if (protocol.equals(TransportLayerProtocol.UDP)) {
            relayChannel.setId(id);
            relayChannels.put(relayChannel.getId(), relayChannel);
        }
    }

    public void unregisterRelayChannel(String id) {
        relayChannels.remove(id);
    }

    public TcpRelayChannelPairAttachment getTcpRelayChannel(String id) {
        return (TcpRelayChannelPairAttachment) relayChannels.get(id);
    }

    public UdpRelayChannelAttachment getUdpRelayChannel(String id) {
        return (UdpRelayChannelAttachment) relayChannels.get(id);
    }

    public boolean closeRelayChannel(String id) {
        if (protocol.equals(TransportLayerProtocol.TCP)) {
            TcpRelayChannelPairAttachment relayChannelPair = (TcpRelayChannelPairAttachment) relayChannels.get(id);
            if (relayChannelPair != null) {
                relayChannelPair.close();
                relayChannels.remove(id);
                return true;
            }
            return false;
        } else {
            UdpRelayChannelAttachment relayChannel = (UdpRelayChannelAttachment) relayChannels.get(id);
            if (relayChannel != null) {
                relayChannel.close();
                relayChannels.remove(id);
                relayChannels.remove(relayChannel.getId());
                relayChannels.remove(relayChannel.getInnerId());
                return true;
            }
            return false;
        }
    }

    public void close() {
        this.closed = true;
        try {
            selectionKey.cancel();

            if (protocol.equals(TransportLayerProtocol.TCP)) {
                relayChannels.forEach((id, relayChannelPair) -> {((TcpRelayChannelPairAttachment)relayChannelPair).close();});
            } else {
                relayChannels.forEach((id, relayChannelPair) -> {((UdpRelayChannelAttachment)relayChannelPair).close();});
            }

            relayChannels.clear();
            this.proxyChannel.close();

        } catch (IOException e) {
        }
    }
}
