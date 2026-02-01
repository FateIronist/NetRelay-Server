package top.fateironist.net_relay.core.filter.base;

import top.fateironist.net_relay.model.filter.RegisterPortRequest;
import top.fateironist.net_relay.model.filter.Request;

import java.util.function.Consumer;

public interface FilterChain {
    void startChain(Request request, Consumer<Request> handler);

    void continueChain(Request request);
}
