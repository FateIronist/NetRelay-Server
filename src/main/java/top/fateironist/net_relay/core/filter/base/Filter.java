package top.fateironist.net_relay.core.filter.base;

import top.fateironist.net_relay.model.filter.RegisterPortRequest;
import top.fateironist.net_relay.model.filter.Request;

public interface Filter {
    /**
     * <p>
     * Time-consuming IO operations should be performed in asynchronous mode;
     * Otherwise, it will block the connection thread;
     * </p>
     * <p>example:</p>
     *
     * <blockquote>
     * <pre>
     *     //do some pre-request processing
     *     filterChain.continueChain(connectionRequest);
     *     //do some post-request processing
     * </pre>
     * </blockquote>
     * @param request
     * @param filterChain
     */
    void doFilter(Request request, FilterChain filterChain);

    /**
     * filter with higher priority will be executed first
     * @return priority
     */
    int getPriority();
}
