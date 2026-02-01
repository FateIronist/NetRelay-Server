package top.fateironist.net_relay.model.communication;

import lombok.Data;
import top.fateironist.net_relay.model.communication.enums.CommunicationTaskType;
import top.fateironist.net_relay.model.filter.RegisterPortRequest;


@Data
public class CommunicationTask {
    private CommunicationTaskType type;
    private String agentId;

    private CommunicationMsg communicationMsg;
    private RegisterPortRequest registerPortRequest;

    public static CommunicationTask buildOrderTask(String agentId, CommunicationMsg communicationMsg) {
        CommunicationTask communicationTask = new CommunicationTask();
        communicationTask.type = CommunicationTaskType.ORDER;
        communicationTask.agentId = agentId;
        communicationTask.communicationMsg = communicationMsg;
        return communicationTask;
    }

    public static CommunicationTask buildRequestTask(RegisterPortRequest registerPortRequest) {
        CommunicationTask communicationTask = new CommunicationTask();
        communicationTask.type = CommunicationTaskType.REQUEST;
        communicationTask.agentId = registerPortRequest.getCommunicationMsg().getAgentId();
        communicationTask.registerPortRequest = registerPortRequest;
        return communicationTask;
    }
}
