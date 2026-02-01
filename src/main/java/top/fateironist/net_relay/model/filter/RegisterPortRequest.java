package top.fateironist.net_relay.model.filter;

import lombok.*;
import top.fateironist.net_relay.model.communication.CommunicationMsg;
import top.fateironist.net_relay.model.filter.enums.RegisterPortRequestTypeEnum;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class RegisterPortRequest extends Request {
    private RegisterPortRequestTypeEnum type;
    private SelectionKey selectionKey;
    private CommunicationMsg communicationMsg;

    public RegisterPortRequest(String ip, int port, SocketChannel socketChannel, CommunicationMsg communicationMsg) {
        super(ip, port, socketChannel);
        this.type = RegisterPortRequestTypeEnum.REQUEST;
        this.communicationMsg = communicationMsg;
    }

    public RegisterPortRequest(String ip, int port, SocketChannel socketChannel, RegisterPortRequestTypeEnum type) {
        super(ip, port, socketChannel);
        this.type = type;
    }

    public RegisterPortRequest(String ip, int port, SocketChannel socketChannel) {
        super(ip, port, socketChannel);
    }

    public SocketChannel getSocketChannel() {
        return (SocketChannel) getChannel();
    }
}
