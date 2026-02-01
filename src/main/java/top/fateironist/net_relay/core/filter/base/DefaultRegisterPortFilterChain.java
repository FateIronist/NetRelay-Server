package top.fateironist.net_relay.core.filter.base;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import top.fateironist.net_relay.model.filter.Request;

import java.util.List;
import java.util.function.Consumer;

@Component
public class DefaultRegisterPortFilterChain implements RegisterPortFilterChain{
    private int index = 0;
    private final List<RegisterPortFilter> filters;
    private Consumer<Request> handler;

    @Autowired
    public DefaultRegisterPortFilterChain(List<RegisterPortFilter> filters) {
        List<RegisterPortFilter> sortedFilters = filters.stream().sorted((o1, o2) -> o1.getPriority() - o2.getPriority()).toList();
        this.filters = sortedFilters;
        index = sortedFilters.size() - 1;
    }

    public DefaultRegisterPortFilterChain(List<RegisterPortFilter> filters, Consumer<Request> handler) {
        this.filters = filters;
        index = filters.size() - 1;
        this.handler = handler;
    }


    public int getIndexAndDecrement() {
        return index--;
    }

    private DefaultRegisterPortFilterChain copy(Consumer<Request> handler) {
        DefaultRegisterPortFilterChain filterChain = new DefaultRegisterPortFilterChain(filters,  handler);
        return filterChain;
    }

    @Override
    public void startChain(Request request, Consumer<Request> handler) {
        if (filters.isEmpty()) {
            handler.accept(request);
        } else {
            DefaultRegisterPortFilterChain filterChain = copy(handler);
            filters.get(filterChain.getIndexAndDecrement()).doFilter(request, filterChain);
        }
    }

    @Override
    public void continueChain(Request request) {
        if (index >= 0) {
            filters.get(getIndexAndDecrement()).doFilter(request, this);
        } else {
            handler.accept(request);
        }
    }
}
