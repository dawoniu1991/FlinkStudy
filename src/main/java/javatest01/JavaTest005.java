package javatest01;

/**
 * @author jiangfan
 * @date 2022/9/30 14:38
 */
public class JavaTest005 {
    //构造函数

    public JavaTest005() {
        System.out.println("Test类的缺省构造器执行");
    }

//实例代码块

    static{
        System.out.println(11);
    }

//实例代码块

    static{
        System.out.println(22);
    }

//实例代码块

    static{
        System.out.println(33);
    }

    public static void main(String[] args) {
        System.out.println("main begin");
        new JavaTest005();
        System.out.println("-----------");
        new JavaTest005();

    }
}
