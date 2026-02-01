package top.fateironist.net_relay;

import de.codecentric.boot.admin.server.config.EnableAdminServer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 只有两个模块
 * -CommunicationManager 负责与代理服务器通信
 * -RelayManager 负责做被代理端口与代理服务器中转Channel的消息转发
 */
@SpringBootApplication
public class NetRelayApplication {

	public static void main(String[] args) {
		SpringApplication.run(NetRelayApplication.class, args);
	}

}
