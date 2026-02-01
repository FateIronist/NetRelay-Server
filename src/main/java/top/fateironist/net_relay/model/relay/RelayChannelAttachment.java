package top.fateironist.net_relay.model.relay;

import lombok.Data;
import lombok.NoArgsConstructor;
import top.fateironist.net_relay.model.relay.enums.TransportLayerProtocol;

import java.nio.channels.Channel;

@Data
@NoArgsConstructor
public class RelayChannelAttachment {
    public static final int DEFAULT_UDP_BUFFER_SIZE = 1472;
    public static final int DEFAULT_TCP_BUFFER_SIZE = 1460;

    private TransportLayerProtocol protocol;
    private String agentId;

    private String id;

    private String proxyPortId;

    private Integer proxiedPort;
    private Integer proxyPort;

    private boolean isClosed;

    public RelayChannelAttachment(TransportLayerProtocol protocol, String agentId, Integer proxiedPort, Integer proxyPort) {
        this.protocol = protocol;
        this.agentId = agentId;
        this.proxiedPort = proxiedPort;
        this.proxyPort = proxyPort;
    }

    public void closeChannel(Channel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception e) {
            }
        }
    }
}
