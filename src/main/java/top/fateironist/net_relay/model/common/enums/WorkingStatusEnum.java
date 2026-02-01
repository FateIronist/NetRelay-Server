package top.fateironist.net_relay.model.common.enums;

public enum WorkingStatusEnum {
    STARTING(0),
    WORKING(1),
    STOPPING(2),
    STOPPED(3);

    Integer code;
    WorkingStatusEnum(Integer code) {
        this.code = code;
    }
    public Integer getCode() {
        return code;
    }
}
