package top.fateironist.net_relay.core.common;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Theoretically, virtual threads do not need pooling;
 * however, for better structural organization, we perform formal pooling here.
 */
@Slf4j
public class AsyncIoThreadPool {
    private static final ThreadFactory threadFactory = new ThreadFactory() {
        private static final AtomicInteger threadNumber = new AtomicInteger(0);
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = Thread.ofVirtual()
                    .name("AsyncIoThreadPool-" + threadNumber.getAndIncrement())
                    .inheritInheritableThreadLocals(true)
                    .uncaughtExceptionHandler((t, e) -> {
                        log.error("AsyncIoThreadPool execute error: {}", e.getMessage());
                    })
                    .unstarted(r);
            return thread;
        }
    };

    private static final ExecutorService executorService = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors() * 2,
            1,
            TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(100),
            threadFactory,
            new ThreadPoolExecutor.CallerRunsPolicy()
    );


    public static void executeWithTimeoutIgnoreException(Runnable runnable, long timeout, TimeUnit timeUnit, Consumer<Exception> errorCallback) {
        Future<?> future = executorService.submit(runnable);
        executorService.execute(() -> {
            try {
                future.get(timeout, timeUnit);
            } catch (Exception e) {
                errorCallback.accept(e);
            }
        });
    }

    public static void executeWithTimeout(Runnable runnable, long timeout, TimeUnit timeUnit) throws Exception {
        Future<?> future = executorService.submit(runnable);
        future.get(timeout, timeUnit);
    }

    public static ExecutorService getExecutorService() {
        return executorService;
    }

    public static void execute(Runnable runnable) {
        executorService.execute(runnable);
    }
}
