# 基于hadoop的朴素贝叶斯实现
自己学习hadoop时做的小练习
> 运行环境

1. hadoop-2.2
2. java-1.8
3. CentOS6.5

> 使用说明

1. 进入[该网站](https://github.com/zbs02355/Bayes_hadoop) 下载代码
2. 新建java工程，导入hadoop2.2的相关jar包
3. 在'WordCounter_Driver'类中的main()里对输入文件、输出文件进行修改，将路径换成自己的集群环境的地址
4. 创建'input、input_test、output_result'这三个文件夹
5. 在eclipse中导出成jar包，上传到namenode节点中
6. 输入(hadoop jar jar包名称 类名.主函数名)命令运行

>注意事项

该程序目前有一个bug，就是MapReduce和普通Java代码不能同时运行，也就是说在利用MapReduce生成模型文件后，后面的Java代码不能生成结果文件。暂时的解决办法：将' WordCounter_Driver.java '中的'208行'注释掉再运行，此时不要删除' output_model '文件夹的内容
