package top.fateironist.net_relay.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 实测，在可用端口占比越小的情况下，线性探测效率相比于随机探测效率越高。可用端口占比越多，效率越接近。
 * 对全部自由端口遍历searchTcpPort和searchUdpPort方法各一遍，大概需要1.35s
 */
@Slf4j
public class PortUtil {
    private static final Integer MIN_FREE_PORT = 49152;
    private static final Integer MAX_FREE_PORT = 60999;
    private static final Random random = new Random();
    private static final float THRESHOLD = 0.9f;

    // 0: free, 1: used
    // The bitmap has a delay when maintaining, so it can only be used for reference
    private static final AtomicInteger lastIndex = new AtomicInteger(0); // optimize queries using the previous index
    private static final ConcurrentBitMap bitMap = new ConcurrentBitMap(MAX_FREE_PORT - MIN_FREE_PORT);

    private static boolean autoSearch = false;
    private static final LinkedBlockingQueue<Integer> portQueue = new LinkedBlockingQueue<>(5);

    public static Integer getPort() {
        Integer result = null;
        if (autoSearch) {
            try {
                 result = portQueue.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {

            }

            if (result != null) {
                return result;
            }

            result = searchPort();
        }

        return result;
    }


    public static Integer searchPort() {
        if (bitMap.getCardinality() / (float) bitMap.getCapacity() < THRESHOLD) {
            int steps = 0;
            while (steps++ < bitMap.getCapacity()) {
                lastIndex.compareAndSet(bitMap.getCapacity() - 1, 0);
                int i = lastIndex.getAndIncrement();
                if (bitMap.setIfNotExist(i)) {
                    Integer port = i + MIN_FREE_PORT;
                    if (checkPort(port)) {
                        return port;
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            }
        } else {
            int steps = 0;
            while (steps++ < bitMap.getCapacity()) {
                lastIndex.compareAndSet(bitMap.getCapacity() - 1, 0);
                int i = lastIndex.getAndIncrement();
                Integer port = i + MIN_FREE_PORT;
                if (checkPort(port)) {
                    return port;
                } else {
                    continue;
                }
            }
        }
        log.error("No free ports available!!!");
        return null;
    }


    /**
     * It is recommended to register in virtual thread pool
     * @param executor
     */
    public static void registerAutoSearchQueue(ExecutorService executor) {
        executor.submit(() -> {
            while (true) {
                try {
                    Integer port = searchPort();
                    if (port != null) {
                        portQueue.put(port);
                    } else {
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException e) {
                    continue;
                }
            }
        });

        autoSearch = true;
    }

    /**
     * Try to get a free tcp port randomly;
     * Tip: portBitmap is unmaintained within this series of methods.
     * (Method series: method name ends with Randomly)
     * @return
     */
    public static Integer getPortRandomly() {
        return getPortRandomly(MAX_FREE_PORT - MIN_FREE_PORT, MIN_FREE_PORT, MAX_FREE_PORT);
    }

    public static Integer getPortRandomly(Integer tryTimes) {
        return getPortRandomly(tryTimes, MIN_FREE_PORT, MAX_FREE_PORT);
    }

    public static Integer getPortRandomly(Integer tryTimes, Integer minPort, Integer maxPort) {
        Integer size = maxPort - minPort;
        for (int i = 0; i < tryTimes; ++i) {
            Integer port = MIN_FREE_PORT + (int)(size * random.nextDouble(0,1));
            if (checkPort(port)) {
                return port;
            } else {
                continue;
            }
        }

        return null;
    }

    public static boolean checkPort(Integer port) {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket();
            socket.setReuseAddress(false);
            socket.bind(new InetSocketAddress(port));

            socket.close();
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                }
            }
        }
    }

    public static boolean markPort(Integer port) {
        if (MIN_FREE_PORT <= port && port <= MAX_FREE_PORT) {
            return bitMap.set(port - MIN_FREE_PORT);
        }
        return false;
    }
    public static boolean unmarkPort(Integer port) {
        if (MIN_FREE_PORT <= port && port <= MAX_FREE_PORT) {
            return bitMap.del(port - MIN_FREE_PORT);
        }
        return false;
    }

}
