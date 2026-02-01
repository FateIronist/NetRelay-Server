package top.fateironist.net_relay.model.relay;

import lombok.Data;
import lombok.EqualsAndHashCode;
import top.fateironist.net_relay.model.relay.enums.TransportLayerProtocol;

import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

@Data
@EqualsAndHashCode(callSuper = true)
public class TcpRelayChannelPairAttachment extends RelayChannelAttachment {
    public static final long MTU_AGGREGATION_WAIT_TIME = 5;
//    /**
//     * BufferWriteContinue
//     * 这里初衷是为了提高写性能
//     * 当读数据过多，由于写操作总是在读到数据的下一轮才会触发，因此存在一个现象
//     * // 第一轮，第一个读到达，注册写事件。
//     * // 第二轮，第二个数据到达，注册写事件。写事件写完，删除写事件。
//     * 此时存在一个bug，写事件暂时丢失，尽管在LT模式下，在下一轮，仍然会触发读注册发写事件，最终写在第四轮被触发，但这样就存在延迟了。
//     * 并且即使写事件删除在新事件读之前
//     * // 第二轮，写事件写完，删除写事件。第二个数据到达，注册写事件。
//     * // 第三轮，第二个事件的写事件触发
//     * 仍然存在延迟。
//     * 因此，这里增加一个临时变量，用于记录当前轮次是否需要继续写事件。
//     *
//     * todo 但...并未充分测试
//     */
    private ByteBuffer inBuffer;
//    private boolean inBufferWriteContinue;
    private ByteBuffer outBuffer;
//    private boolean outBufferWriteContinue;

    private SocketChannel requestChannel;
    private SelectionKey requestChannelSelectionKey;
    private SocketChannel relayChannel;
    private SelectionKey relayChannelSelectionKey;

    private long inBufferLastWriteTime;
    private long outBufferLastWriteTime;
    private long createTime;

    public TcpRelayChannelPairAttachment(String agentId, Integer proxiedPort, Integer proxyPort, SocketChannel requestChannel) {
        setProtocol(TransportLayerProtocol.TCP);
        setAgentId(agentId);
        setProxiedPort(proxiedPort);
        setProxyPort(proxyPort);

        this.requestChannel = requestChannel;
        this.inBuffer = ByteBuffer.allocateDirect(DEFAULT_TCP_BUFFER_SIZE);
//        this.inBufferWriteContinue = false;
        this.outBuffer = ByteBuffer.allocateDirect(DEFAULT_TCP_BUFFER_SIZE);
//        this.outBufferWriteContinue = false;
        this.inBufferLastWriteTime = System.currentTimeMillis();
        this.outBufferLastWriteTime = System.currentTimeMillis();
        this.createTime = System.currentTimeMillis();
        setClosed(false);
    }

    public SocketChannel getTcpRequestChannel() {
        return this.requestChannel;
    }

    public SocketChannel getTcpRelayChannel() {
        return this.relayChannel;
    }

    public boolean isWriteInTimeout() {
        return System.currentTimeMillis() - inBufferLastWriteTime >= MTU_AGGREGATION_WAIT_TIME;
    }

    public boolean isWriteOutTimeout() {
        return System.currentTimeMillis() - outBufferLastWriteTime >= MTU_AGGREGATION_WAIT_TIME;
    }

    public void close() {
        if (!isClosed()) {
            setClosed(true);

            if (requestChannelSelectionKey != null) requestChannelSelectionKey.cancel();
            if (relayChannelSelectionKey != null) relayChannelSelectionKey.cancel();

            closeChannel(requestChannel);
            closeChannel(relayChannel);

        }
    }

}
