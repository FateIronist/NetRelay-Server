package top.fateironist.net_relay;

import org.springframework.stereotype.Component;

import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

@Component
public class TestMain {
    public static void main(String[] args) throws Exception {
        Selector selector = Selector.open();
        DatagramChannel datagramChannel = DatagramChannel.open();
        datagramChannel.configureBlocking(false);
        datagramChannel.register(selector, SelectionKey.OP_READ);

        while (true) {
            selector.select();
            System.out.println("select");
            Thread.sleep(500);
        }
    }


}
