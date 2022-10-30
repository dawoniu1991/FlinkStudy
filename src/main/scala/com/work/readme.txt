第一个代码是求解异常检测中的温度过高    
        其中输入文件路径src\main\resources\job01.csv  内容为温度传感器的检测内容
第二个代码是求解异常检测中的酸度、粘稠度、含水量，任意指标连续两次以上都比前一个值高10%   
        其中输入文件路径src\main\resources\job02.csv  内容为润滑油质量检测传感器的检测内容
第三份代码是求解每天的温度平均值   
        其中输入文件路径src\main\resources\job01.csv  内容为温度传感器的检测内容


上述代码也可以用env.socketTextStream("localhost", 7777) 的方式发送数据，可以在windows本地cmd，然后bash，然后 nc -lk 7777 开启端口发送数据进行访问