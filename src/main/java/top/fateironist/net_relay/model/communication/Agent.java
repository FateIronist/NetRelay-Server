package top.fateironist.net_relay.model.communication;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

@Data
@Builder
public class Agent {
    private String agentId;
    private String ip;
    private Integer port;

    @Builder.Default
    private boolean running = true;

    private SelectionKey selectionKey;

    private SocketChannel communicationSocketChannel;
    private CommunicationChannelAttachment attachment;

    @Builder.Default
    private long lastActiveTime = System.currentTimeMillis();

    @Builder.Default
    private Map<Integer, Integer> tcpProxyPort = new HashMap<>();
    @Builder.Default
    private Map<Integer, Integer> udpProxyPort = new HashMap<>();

    public void close() {
        // 关闭自己的，委托上层关闭周边服务
        running = false;

        try {
            communicationSocketChannel.close();
        } catch (IOException e) {
        }

        if (selectionKey != null) selectionKey.cancel();
    }

    public boolean shouldClose() {
        return !running || !isActive();
    }

    public boolean isActive() {
        return System.currentTimeMillis() - lastActiveTime < 1000 * 60 * 5;
    }

    public void addTcpProxyPort(int localPort, int remotePort) {
        tcpProxyPort.put(localPort, remotePort);
    }

    public void addUdpProxyPort(int localPort, int remotePort) {
        udpProxyPort.put(localPort, remotePort);
    }
}
