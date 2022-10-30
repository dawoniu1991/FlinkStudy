package javatest01;

/**
 * @author jiangfan
 * @date 2022/10/24 17:48
 */
public class JavaTest007 {
    public static void main(String[] args) {
        System.out.println("aaaa");
        System.out.println("bbbb");
        if(0==0){
            System.out.println("qqq");
            throw new RuntimeException("回滚事物异常");}
        System.out.println("ccc");
    }
}
