package top.fateironist.net_relay.model.communication;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@NoArgsConstructor
public class CommunicationMsg {
    private String agentId;
    private Method order;
    private Method request;

    public static int findMsgEnd(byte[] bytes) {
        byte[] endBytes = CommunicationProtocol.END_WITHOUT_CRLF.getBytes(CommunicationProtocol.CHARSET);
        int endIndex = endBytes.length - 1;
        for (int i = 0; i < bytes.length - 1; i++) {
            if (bytes[i] == '\r' && bytes[i + 1] == '\n') {
                endIndex = endBytes.length - 1;
                for (int j = i-1; j >= 0; j--) {
                    if (endIndex >= 0 && bytes[j] == endBytes[endIndex--]) {
                        continue;
                    } else {
                        break;
                    }
                }
                if (endIndex < 0) {
                    return i + 1;
                }
            }
        }
        return -1;
    }

    public static CommunicationMsg parse(byte[] bytes) {
        return parse(new String(bytes, CommunicationProtocol.CHARSET));
    }

    public static CommunicationMsg parse(String completeMsg) {
        String[] lines = completeMsg.split("\r\n");
        if (lines.length < 4) {
            return null;
        }
        if (!lines[0].equals(CommunicationProtocol.HEADER_WITHOUT_CRLF)) {
            return null;
        }
        if (!lines[1].equals(CommunicationProtocol.BEGIN_WITHOUT_CRLF)) {
            return null;
        }
        if (!lines[lines.length - 1].equals(CommunicationProtocol.END_WITHOUT_CRLF)) {
            return null;
        }
        CommunicationMsg communicationMsg = new CommunicationMsg();
        for (int i = 2; i < lines.length - 1; i++) {
            String[] msgLines = lines[i].split(":::");
            switch (msgLines[0]) {
                case CommunicationProtocol.BODY_AGENT_ID_PREFIX:
                    communicationMsg.agentId = msgLines[1];
                    break;
                case CommunicationProtocol.BODY_ORDER_PREFIX:
                    String[] orderMethodLines = msgLines[1].split("=");
                    Method orderMethod = null;
                    if (orderMethodLines.length > 1) {
                        orderMethod = new Method(orderMethodLines[0], orderMethodLines[1].substring(1, orderMethodLines[1].length() - 1).split(","));
                    } else {
                        orderMethod = new Method(orderMethodLines[0], null);
                    }
                    communicationMsg.order = orderMethod;
                    break;
                case CommunicationProtocol.BODY_REQUEST_PREFIX:
                    String[] requestMethodLines = msgLines[1].split("=");
                    Method requestMethod = null;
                    if (requestMethodLines.length > 1) {
                        requestMethod = new Method(requestMethodLines[0], requestMethodLines[1].substring(1, requestMethodLines[1].length() - 1).split(","));
                    } else {
                        requestMethod = new Method(requestMethodLines[0], null);
                    }
                    communicationMsg.request = requestMethod;
                    break;
            }
        }

        return communicationMsg;
    }

    public String buildStrOrderMessage() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(CommunicationProtocol.BODY_ORDER_PREFIX).append(":::").append(order.name);
        if (order.hasArgs()) {
            stringBuilder.append("=[");
            for (String arg : order.args) {
                stringBuilder.append(arg).append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            stringBuilder.append("]");
        }
        stringBuilder.append("\r\n");
        return CommunicationProtocol.buildStrMessage(stringBuilder.toString());
    }

    public String buildStrRequestMessage() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(CommunicationProtocol.BODY_AGENT_ID_PREFIX).append(":::").append(agentId).append("\r\n");
        stringBuilder.append(CommunicationProtocol.BODY_REQUEST_PREFIX).append(":::").append(request.name);
        if (request.hasArgs()) {
            stringBuilder.append("=[");
            for (String arg : request.args) {
                stringBuilder.append(arg).append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            stringBuilder.append("]");
        }
        stringBuilder.append("\r\n");
        return CommunicationProtocol.buildStrMessage(stringBuilder.toString());
    }

    public byte[] buildBytesOrderMessage() {
        return buildStrOrderMessage().getBytes(CommunicationProtocol.CHARSET);
    }

    public byte[] buildBytesRequestMessage() {
        return buildStrRequestMessage().getBytes(CommunicationProtocol.CHARSET);
    }

    @Data
    @AllArgsConstructor
    public static class Method {
        private String name;
        private String[] args;

        public boolean hasArgs() {
            return args != null && args.length > 0;
        }
    }
}
