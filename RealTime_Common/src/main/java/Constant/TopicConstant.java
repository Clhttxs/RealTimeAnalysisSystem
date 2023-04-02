package Constant;

//常量(static final修饰)一般声明在接口中。
//        无需实例化对象，直接用类名.变量就可以获取常量。
//        就需要把类设置为太监类(抽象类接口)

public interface TopicConstant {
    String ORIGINAL_LOG = "base_log";

    String STARTUP_LOG = "REALTIME_STARTUP_LOG";
    String ACTION_LOG = "REALTIME_ACTION_LOG";

    String ORDER_INFO = "REALTIME_DB_ORDER_INFO";
    String ORDER_DETAIL = "REALTIME_DB_ORDER_DETAIL";
}
