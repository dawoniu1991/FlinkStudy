package javatest01;

/**
 * @author jiangfan
 * @date 2022/8/16 15:40
 */

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

import javax.management.MBeanServer;

import com.sun.management.HotSpotDiagnosticMXBean;

public class MavenSpringApp
{
    public static void main( String[] args )
    {
        testManagementFactory();

    }

    private static void testManagementFactory() {
//      1获取JVM输入参数
        List<String> list=ManagementFactory.getRuntimeMXBean().getInputArguments();
        for(String args:list){
            System.out.println(args);
            System.out.println("=======");
        }
//      2.获取当前JVM进程的PID
//        String name = ManagementFactory.getRuntimeMXBean().getName();
//        String pid = name.split("@")[0];
//      3.获取当前系统的负载
//        ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
//      4.获取内存相关的
//        ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();//堆内存
//        ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();//堆外内存
//      5.获取堆栈信息相当于jstack
//        ManagementFactory.getThreadMXBean().dumpAllThreads(false, false);
//      ThreadInfo 里有线程的信息

//      进行垃圾回收监控
//      MavenSpringApp.main(new String[]{"-gcutil", "-h5",pid,"1s"});

//      如何获取HotSpotDiagnosticMXBean   ？
//        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
//      HotSpotDiagnosticMXBean   hotspotDiagnosticMXBean = new PlatformMXBeanProxy(server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);

        //获取young GC 和full GC 次数
//        List<GarbageCollectorMXBean> list1=ManagementFactory.getGarbageCollectorMXBeans();
//        for(GarbageCollectorMXBean e:list1){
//            System.out.println(String.format("name=%s,count=%s,time=%s",e.getName(),e.getCollectionCount(),e.getCollectionTime()));
//        }
    }
}