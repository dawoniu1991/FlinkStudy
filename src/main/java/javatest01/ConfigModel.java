package javatest01;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author jiangfan
 * @date 2022/8/16 16:01
 */
public class ConfigModel {
//    public static void fun() throws IOException {
//        Properties properties = new Properties();
//        InputStream resourceAsStream = ConfigModel.class.getClassLoader().getResourceAsStream("debug.properties");
//        properties.load(resourceAsStream);
//        String mysalPassword = properties.getProperty("mysalPassword");
//        System.out.println(mysalPassword);
//        System.out.println(properties);
//    }

    public static void fun01() throws IOException {
        InputStream resourceAsStream = ConfigModel.class.getClassLoader().getResourceAsStream("debug.properties");
        ParameterTool config = ParameterTool.fromPropertiesFile(resourceAsStream);
        String mysalPassword = config.get("mysalPassword");
        System.out.println(mysalPassword);
//        System.out.println(config.get("mysqlUrl"));
    }
}
