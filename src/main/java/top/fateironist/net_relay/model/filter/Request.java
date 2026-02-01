package top.fateironist.net_relay.model.filter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.channels.Channel;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Request {
    private String ip;
    private int port;
    private Channel channel;
}
