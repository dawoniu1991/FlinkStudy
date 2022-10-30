package javatest01;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * @author jiangfan
 * @date 2022/8/31 15:54
 */
public class JavaTest002 {
//    protected static final Logger logger = LoggerFactory.getLogger(JavaTest002.class);

    public static void main(String[] args) {
         Logger logger = LoggerFactory.getLogger(JavaTest002.class);
        int days=900;
        int month = 9;
        switch (month)//值必须是整型或者字符型
        {
            case 2:
                days=28;
                break;
            case 4:
            case 6:
            case 9:
            case 11:
                days = 30;
                break;
            default:
                days = 31;
        }
        System.out.println(month + "月份共" + days + "天");
        System.err.println(month + "月份共" + days + "天");
        System.err.println("qqqqqqq天");
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

        logger.info("aaasss===ww=");
    }
}
