package javatest01;

import java.util.Collections;

/**
 * @author jiangfan
 * @date 2022/10/19 12:21
 */
public class JavaTest006 {
    public static void main(String[] args) {
        String a = String.format("%0" + 50 + "d", 0);
//        String a = String.format("%0" + 50 + "d", 0).replace("0", "a");
        System.out.println(a);
        String q = String.join("", Collections.nCopies(2174343, "q"));
        System.out.println(q);
    }
}
