package top.fateironist.net_relay.model.relay;

import lombok.Data;
import lombok.EqualsAndHashCode;
import top.fateironist.net_relay.model.relay.enums.TransportLayerProtocol;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;

@Data
@EqualsAndHashCode(callSuper = true)
public class UdpRelayChannelAttachment extends RelayChannelAttachment{

    private String innerId;

    private int bindPort;
    private SocketAddress proxiedRelayAddress;
    private SocketAddress remoteAddress;

    private DatagramChannel datagramChannel;
    private SelectionKey selectionKey;

    // udp区别于tcp的流式，是数据包传输的，若缓冲区满则新数据整个丢弃，为了避免初始请求中重要的信息丢失，故使用队列缓存超限的初始请求数据包
    private Deque<ByteBuffer> initialBufferQueue;
    private boolean initialBufferQueueEmpty = true;
    private boolean penetration = false;
    private ByteBuffer buffer;
    private ByteBuffer inBuffer;
    private ByteBuffer outBuffer;

    private long lastActiveTime = System.currentTimeMillis();

    public void refresh() {
        lastActiveTime = System.currentTimeMillis();
    }

    public boolean shouldClose() {
        return isClosed() || System.currentTimeMillis() - lastActiveTime > 1000 * 30;
    }

    public UdpRelayChannelAttachment(String agentId,String proxyPortId, Integer proxiedPort, Integer proxyPort) {
        setProtocol(TransportLayerProtocol.UDP);
        setAgentId(agentId);
        setProxyPortId(proxyPortId);
        setProxiedPort(proxiedPort);
        setProxyPort(proxyPort);

        this.initialBufferQueue = new ArrayDeque<>();
        this.buffer = ByteBuffer.allocateDirect(DEFAULT_UDP_BUFFER_SIZE);
        this.penetration = false;

        setClosed(false);
    }

    public ByteBuffer getInBufferOrCreate() {
        if (this.inBuffer == null) {
            this.inBuffer = ByteBuffer.allocateDirect(DEFAULT_UDP_BUFFER_SIZE * 8);
        }
        return this.inBuffer;
    }

    public ByteBuffer getOutBufferOrCreate() {
        if (this.outBuffer == null) {
            this.outBuffer = ByteBuffer.allocateDirect(DEFAULT_UDP_BUFFER_SIZE * 8);
        }
        return this.outBuffer;
    }

    public void close() {
        setClosed(true);
        if (this.selectionKey != null) {
            this.selectionKey.cancel();
            closeChannel(datagramChannel);
        }
    }

}
