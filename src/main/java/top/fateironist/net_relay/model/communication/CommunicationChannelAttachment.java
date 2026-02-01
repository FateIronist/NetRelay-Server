package top.fateironist.net_relay.model.communication;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

@Data
@NoArgsConstructor
public class CommunicationChannelAttachment {
    private String agentId;
    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;

    public CommunicationChannelAttachment(String agentId) {
        this.agentId = agentId;
    }
}
