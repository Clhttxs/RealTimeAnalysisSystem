package CommonUtils;

import java.util.ResourceBundle;

/**
 * 读取配置文件中指定Key的value
 */
public class PropertiesUtil {
    /**
     * ResourceBundle.getBundle():读取类路径下xxx.properties文件,输入参数xxx
     * 将读取的配置文件封装成Properties(map)
     */
    private static ResourceBundle props = ResourceBundle.getBundle("config");

    public static String getProperty(String name){
      return  props.getString(name);
    }
}