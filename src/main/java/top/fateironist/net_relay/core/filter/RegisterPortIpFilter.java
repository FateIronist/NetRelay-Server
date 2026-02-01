package top.fateironist.net_relay.core.filter;

import org.springframework.stereotype.Component;
import top.fateironist.net_relay.core.filter.base.FilterChain;
import top.fateironist.net_relay.core.filter.base.RegisterPortFilter;
import top.fateironist.net_relay.model.filter.RegisterPortRequest;
import top.fateironist.net_relay.model.filter.Request;
import top.fateironist.net_relay.model.filter.enums.RegisterPortRequestTypeEnum;

@Component
public class RegisterPortIpFilter implements RegisterPortFilter {
    @Override
    public void doFilter(Request request, FilterChain filterChain) {
        RegisterPortRequest registerPortRequest = (RegisterPortRequest) request;
        if (registerPortRequest.getType() == RegisterPortRequestTypeEnum.CONNECTION) {

        }
        filterChain.continueChain(request);
    }

    @Override
    public int getPriority() {
        return 0;
    }
}
