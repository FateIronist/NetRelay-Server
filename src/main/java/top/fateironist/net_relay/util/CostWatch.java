package top.fateironist.net_relay.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * concurrent-safe task cost watching tool.
 * this is a non-reusable class after invoking beautyPrint().
 * but you can call beautyPrint() repeatedly.
 * <code>
 * <pre>
 *     CostWatch timeWatch = new CostWatch();
 *     timeWatch.watch("title1");
 *     timeWatch.watch("title2");
 *     ...
 *     timeWatch.stop("title2");
 *     timeWatch.stop("title1");
 *     String result = timeWatch.beautyPrint();
 * </pre>
 * </code>
 */
public class CostWatch {

    //Pair --- left: thread id ; right: cost
    private final Map<String,Pair<Long, Long> > cost = new LinkedHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private int count = 0;
    private boolean shutdown = false;
    private final TimeUnit timeUnit;

    /**
     * default time unit is milliseconds
     */
    public CostWatch() {
        timeUnit = TimeUnit.MILLISECONDS;
    }

    public CostWatch(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    private class Pair<L, R> {
        public L left;
        public R right;

        public Pair(L left, R right) {
            this.left = left;
            this.right = right;
        }

    }

    /**
     * start a watch task
     * record the start time
     * @param title
     */
    public void watch(String title) {
        lock.lock();
        if (shutdown) {
            lock.unlock();
            return;
        }
        // avoid recording the same title
        if (!cost.containsKey(title)) {
            count++;
            cost.put(title, new Pair<>(Thread.currentThread().getId(), System.nanoTime()));
        }

        lock.unlock();
    }

    /**
     * stop a watch task
     * record the end time and calculate the cost
     * @param title
     */
    public void stop(String title) {
        lock.lock();
        if (shutdown && count == 0) {
            lock.unlock();
            return;
        }

        cost.compute(title, (k, v) -> {
            if (v == null) {
                return null;
            }

            // avoid stopping the same title
            if (v.left != null) {
                v.left = null;
                v.right = System.nanoTime() - v.right;
                count--;
            }
            return v;
        });

        // awake all waiting threads
        if (shutdown && count == 0) {
            synchronized (this) {
                this.notifyAll();
            }
        }

        lock.unlock();
    }

    public String  beautyPrint() {
        lock.lock();

        // prevent threads from hanging indefinitely caused by invoking stop() after beautyPrint().
        for (Map.Entry<String, Pair<Long, Long>> entry : cost.entrySet()) {
            if (entry.getValue().left != null && entry.getValue().left.longValue() == Thread.currentThread().getId()) {
                lock.unlock();
                return "You can't stop (" + entry.getKey() + ") after beautyPrint!!!";
            }
        }

        boolean single = true;
        shutdown = true;

        // wait for all titles to stop
        if (count != 0) {
            single = false;
            lock.unlock();
            try {
                synchronized (this) {
                    this.wait(60000);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if (single) lock.unlock();

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Pair<Long, Long>> entry : cost.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(timeUnit.convert(entry.getValue().right, TimeUnit.NANOSECONDS)).append(" " + timeUnit.name().toLowerCase() + "\n");
        }
        return sb.toString();
    }

}
