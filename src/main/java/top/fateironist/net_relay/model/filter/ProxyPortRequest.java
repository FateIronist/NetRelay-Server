package top.fateironist.net_relay.model.filter;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import top.fateironist.net_relay.model.relay.ProxyPortAttachment;
import top.fateironist.net_relay.model.relay.enums.TransportLayerProtocol;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.Channel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ProxyPortRequest extends Request{
    private String agentId;
    private int proxyPort;
    private int proxiedPort;

    public static ProxyPortRequest buildTcpRequest(ProxyPortAttachment attachment, Channel channel) {
        ProxyPortRequest request = null;
        if (attachment.getProtocol() == TransportLayerProtocol.TCP) {
            request = new ProxyPortRequest();
            SocketChannel socketChannel = (SocketChannel) channel;
            request.setIp(socketChannel.socket().getInetAddress().getHostAddress());
            request.setPort(socketChannel.socket().getPort());
            request.setChannel(channel);
            request.agentId = attachment.getAgentId();
            request.proxyPort = attachment.getProxyPort();
            request.proxiedPort = attachment.getProxiedPort();
        }

        return request;
    }

    public static ProxyPortRequest buildUdpRequest(ProxyPortAttachment attachment, SocketAddress address) {
        ProxyPortRequest request = null;
        if (attachment.getProtocol() == TransportLayerProtocol.UDP) {
            request = new ProxyPortRequest();
            InetSocketAddress socketAddress = (InetSocketAddress) address;
            request.setIp(socketAddress.getAddress().getHostAddress());
            request.setPort(socketAddress.getPort());
            request.agentId = attachment.getAgentId();
            request.proxyPort = attachment.getProxyPort();
            request.proxiedPort = attachment.getProxiedPort();
        }

        return request;
    }
}
