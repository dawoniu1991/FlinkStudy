package javatest01;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @author jiangfan
 * @date 2022/8/11 17:05
 */
public class JSON2JavaTest{
    public static void main(String[] args) {

//        String stuString = "{\"age\":2,\"name\":\"公众号编程大aaa道\",\"sex\":\"m\"}";
        String stuString = "{\"age\":2,\"name\":\"公众号编程大aaa道\"}";
        JSONObject jsonObject1 = JSON.parseObject(stuString);
        Student student = JSONObject.toJavaObject(jsonObject1, Student.class);
        System.out.println("JSON对象转换成Java对象\n" + student);//Student{name='公众号编程大道', sex='m', age=2}

//        Student stu = new Student("公众号编程大道", "m", 2);
//        先转成JSON对象
//        JSONObject jsonObject = (JSONObject) JSONObject.toJSON(stu);
//
//        JSON对象转换成Java对象
//        Student student = JSONObject.toJavaObject(jsonObject, Student.class);
//        System.out.println("JSON对象转换成Java对象\n" + student);//Student{name='公众号编程大道', sex='m', age=2}
    }
}
