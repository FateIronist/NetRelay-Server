package top.fateironist.net_relay.model.relay;

import lombok.Data;
import top.fateironist.net_relay.model.common.enums.WorkingStatusEnum;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

@Data
public class RelayWorker {
    private int id;

    private WorkingStatusEnum workingStatus;

    private Selector selector;

    private Map<String, ProxyPortAttachment> proxyPortRegisterTable;
    private ConcurrentLinkedQueue<RelayTask> relayTaskQueue;

    private Map<String, TcpRelayChannelPairAttachment> tempCache;

    private ReentrantLock lock;
    private int index;
    private long[] blockingTime;


    public RelayWorker(int id) {
        this.id = id;
        this.workingStatus = WorkingStatusEnum.STARTING;
        this.proxyPortRegisterTable = new HashMap<>();
        this.relayTaskQueue = new ConcurrentLinkedQueue<>();
        this.tempCache = new HashMap<>();
        this.lock = new ReentrantLock();
        this.index = 0;
        this.blockingTime = new long[100];
    }

    public void updateBlockingTime(long time) {
        this.lock.lock();
        this.blockingTime[this.index] = time;
        this.lock.unlock();
        this.index = (++this.index) % this.blockingTime.length;
    }

    public long[] getBlockingTime() {
        long[] temp = new long[this.blockingTime.length];
        this.lock.lock();
        System.arraycopy(this.blockingTime, 0, temp, 0, this.blockingTime.length);
        this.lock.unlock();
        return temp;
    }

    public long getAverageBlockingTime() {
        long[] temp = this.getBlockingTime();
        long sum = 0;
        for (long l : temp) {
            sum += l;
        }
        return sum / temp.length;
    }

    public ProxyPortAttachment getProxyPort(String id) {
        return this.proxyPortRegisterTable.get(id);
    }

    public boolean isProxyPortClosed(String id) {
        return !this.proxyPortRegisterTable.containsKey(id) || this.proxyPortRegisterTable.get(id).isClosed();
    }

    public boolean shouldRelayChannelClose(String proxyPortId, String id) {
        ProxyPortAttachment proxyPortAttachment = this.getProxyPort(proxyPortId);
        if (proxyPortAttachment != null) {
            if (proxyPortAttachment.isClosed()) {
                return true;
            } else {
                return proxyPortAttachment.isRelayChannelPairClosed(id);
            }
        }
        return true;
    }

    public void registerProxyPort(String id, ProxyPortAttachment attachment) {
        this.proxyPortRegisterTable.put(id, attachment);
    }

    public void unregisterProxyPort(String id) {
        this.proxyPortRegisterTable.remove(id);
    }

    public void putTempCache(String key, TcpRelayChannelPairAttachment attachment) {
        this.tempCache.put(key, attachment);
    }

    public TcpRelayChannelPairAttachment getTempCache(String key) {
        return this.tempCache.remove(key);
    }

    public void submitTask(RelayTask task) {
        if (isRunning()) {
            this.relayTaskQueue.add(task);
            this.selector.wakeup();
        }
    }

    public RelayTask pollTask() {
        return this.relayTaskQueue.poll();
    }

    public void closeProxyPort(String key) {
        ProxyPortAttachment proxyPortAttachment = this.getProxyPort(key);
        if (proxyPortAttachment != null) {
            proxyPortAttachment.close();
            proxyPortRegisterTable.remove(key);
        }
    }

    public boolean closeRelayChannel(String proxyPortId,String id) {
        ProxyPortAttachment proxyPortAttachment = this.getProxyPort(proxyPortId);
        if (proxyPortAttachment != null) {
            return proxyPortAttachment.closeRelayChannel(id);
        }
        return false;
    }
    public List<String> close() {
        this.workingStatus = WorkingStatusEnum.STOPPING;

        List<String> proxyPortIds = new ArrayList<>();

        proxyPortRegisterTable.forEach((id, attachment) -> {
            proxyPortIds.add(id);
            attachment.close();
        });

        tempCache.forEach((id, attachment) -> {attachment.close();});

        relayTaskQueue.clear();
        tempCache.clear();

        try {
            selector.close();
        } catch (IOException e) {
        }

        this.workingStatus = WorkingStatusEnum.STOPPED;
        return proxyPortIds;
    }


    public boolean isRunning() {
        return workingStatus.getCode() <= WorkingStatusEnum.WORKING.getCode();
    }
}
