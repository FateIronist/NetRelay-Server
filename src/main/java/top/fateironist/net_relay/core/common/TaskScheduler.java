package top.fateironist.net_relay.core.common;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TaskScheduler {
    private static final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);

    public static void scheduleWithFixedRate(Runnable task, long delay, long period, TimeUnit unit) {
        scheduler.scheduleAtFixedRate(task, delay, period, unit);
    }

    public static void schedule(Runnable task, long delay, TimeUnit unit) {
        scheduler.schedule(task, delay, unit);
    }
}
