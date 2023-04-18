package RealTimeUtils

import CommonUtils.PropertiesUtil

import java.sql.Connection
import com.alibaba.druid.pool.DruidDataSourceFactory

import javax.sql.DataSource

/**
 * Created by VULCAN on 2020/11/3
 */
object JDBCUtil {
    // 创建连接池对象
    var dataSource:DataSource = init()

    // 连接池的初始化
    def init():DataSource = {
        val paramMap = new java.util.HashMap[String, String]()
        paramMap.put("driverClassName", PropertiesUtil.getProperty("jdbc.driver.name"))
        paramMap.put("url", PropertiesUtil.getProperty("jdbc.url"))
        paramMap.put("username", PropertiesUtil.getProperty("jdbc.user"))
        paramMap.put("password", PropertiesUtil.getProperty("jdbc.password"))
        paramMap.put("maxActive", PropertiesUtil.getProperty("jdbc.datasource.size"))
        // 使用Druid连接池对象
        DruidDataSourceFactory.createDataSource(paramMap)
    }

    // 从连接池中获取连接对象
    def getConnection(): Connection = {
        dataSource.getConnection
    }

}
