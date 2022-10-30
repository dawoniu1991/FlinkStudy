package javatest01;

/**
 * @author jiangfan
 * @date 2022/9/30 14:37
 */
public class JavaTest004 {
    //静态代码块在一个类以编中可写多个，并且遵循自上而下的顺序依次执行
    static{
        System.out.println("类加载1");
    }

    static{
        System.out.println("类加载2");
    }

    static{
        System.out.println("类加载3");
    }

    public static void main(String[] args) {
        System.out.println("main begin");

    }

}
