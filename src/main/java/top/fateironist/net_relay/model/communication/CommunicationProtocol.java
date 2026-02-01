package top.fateironist.net_relay.model.communication;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CommunicationProtocol {
    public static final int MAX_MSG_SIZE = 256; // bytes
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    public static final String HEADER = "F-RELAY/1\r\n";
    public static final String BEGIN = "[[BEGIN]]\r\n";
    public static final String END = "[[END]]\r\n";

    public static final String HEADER_WITHOUT_CRLF = "F-RELAY/1";
    public static final String BEGIN_WITHOUT_CRLF = "[[BEGIN]]";
    public static final String END_WITHOUT_CRLF = "[[END]]";

    public static final String BODY_ORDER_PREFIX = "ORDER";

    public static final String BODY_AGENT_ID_PREFIX = "AGENT_ID";
    public static final String BODY_REQUEST_PREFIX = "REQUEST";

    // message format: msgCode=[arg...]
    public static final String BODY_REGISTER_PING_MSG = "000";

    // args=null
    public static final String BODY_REGISTER_COMMUNICATION_CHANNEL_MSG = "001";
    // args=[1,agentId]/[0]
    public static final String BODY_REGISTER_COMMUNICATION_CHANNEL_RESPONSE_MSG = "002";
    // args=[8080,8081,8082...]
    public static final String BODY_REGISTER_TCP_PROXY_MSG = "003";
    // args=[9090,0,9092...]  0 means failed
    public static final String BODY_REGISTER_TCP_PROXY_RESPONSE_MSG = "004";
    // args=[8080,8081,8082...]
    public static final String BODY_REGISTER_UDP_PROXY_MSG = "005";
    // args=[9090,0,9092...]  0 means failed
    public static final String BODY_REGISTER_UDP_PROXY_RESPONSE_MSG = "006";

    // args=[8080,tempId]
    public static final String BODY_REQUIRE_TCP_RELAY_CHANNEL_MSG = "007";
    // args=[8080,tempId]
    public static final String BODY_REQUIRE_TCP_RELAY_CHANNEL_RESPONSE_MSG = "008";

    // args=[8080,60078,12700000000161110]——[proxiedPort,udpRelayBindPort,udpRelayChannelId]
    public static final String BODY_REQUIRE_UDP_RELAY_CHANNEL_MSG = "009";
    // args=[8080,12700000000161110]——[proxiedPort,udpRelayChannelId]
    public static final String BODY_REQUIRE_UDP_RELAY_CHANNEL_RESPONSE_MSG = "010";

    public static final String BODY_UDP_PENETRATION_MEG = "011";
    public static final String BODY_UDP_PENETRATION_RESPONSE_MSG = "012";

    public static final String BODY_SHUTDOWN_MSG = "999";


    public static String buildStrMessage(Map<String, String> body) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(HEADER);
        stringBuilder.append(BEGIN);
        for (Map.Entry<String, String> entry : body.entrySet()) {
            stringBuilder.append(entry.getKey()).append(":").append(entry.getValue()).append("\r\n");
        }
        stringBuilder.append(END);
        return stringBuilder.toString();
    }

    public static String buildStrMessage(String body) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(HEADER).append(BEGIN).append(body).append(END);
        return stringBuilder.toString();
    }

    public static byte[] buildBytesMessage(Map<String, String> body) {
        return buildStrMessage(body).getBytes(CHARSET);
    }

    public static byte[] buildBytesMessage(String body) {
        return buildStrMessage(body).getBytes(CHARSET);
    }


}
