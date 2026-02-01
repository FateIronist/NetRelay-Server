package top.fateironist.net_relay.model.relay;

import lombok.Data;

@Data
public class TcpRelayChannelPairAttachmentWrapper {
    private boolean in;
    private TcpRelayChannelPairAttachment attachment;

    public TcpRelayChannelPairAttachmentWrapper(boolean in, TcpRelayChannelPairAttachment attachment) {
        this.in = in;
        this.attachment = attachment;
    }
}
