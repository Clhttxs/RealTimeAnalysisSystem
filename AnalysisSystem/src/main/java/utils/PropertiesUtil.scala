package utils

import java.util.ResourceBundle

/**
定义了一个名为 PropertiesUtil的单例对象，该对象包含一个 getValue 方法，用于获取名为xxx的配置文件中指定 key 的值。
 */
object PropertiesUtil {
    // 绑定配置文件
    // ResourceBundle专门用于读取配置文件，所以读取时，不需要增加扩展名
    val resourceFile: ResourceBundle = ResourceBundle.getBundle("db")

    def getValue( key : String ): String = {
        resourceFile.getString(key)
    }
}
