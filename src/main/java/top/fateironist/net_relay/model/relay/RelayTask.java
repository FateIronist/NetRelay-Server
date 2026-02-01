package top.fateironist.net_relay.model.relay;

import lombok.Data;
import top.fateironist.net_relay.model.relay.enums.RelayTaskType;

@Data
public class RelayTask {
    private RelayTaskType taskType;

    private ProxyPortAttachment proxyPortAttachment;

    private RelayChannelAttachment RelayChannelPairAttachment;

    private TcpRelayChannelPairAttachmentWrapper tcpRelayChannelPairAttachmentWrapper;

    public RelayTask(RelayTaskType taskType, ProxyPortAttachment attachment) {
        this.taskType = taskType;
        this.proxyPortAttachment = attachment;
    }

    public RelayTask(RelayTaskType taskType, RelayChannelAttachment attachment) {
        this.taskType = taskType;
        this.RelayChannelPairAttachment = attachment;
    }

    public RelayTask(RelayTaskType taskType, TcpRelayChannelPairAttachmentWrapper attachment) {
        this.taskType = taskType;
        this.tcpRelayChannelPairAttachmentWrapper = attachment;
    }
}
