lab1: 实现一个分布式mapreduce

1. 首先跑一下顺序执行的mapreduce，看一下要做出什么效果
   
   阅读mrapps/wc.go  这是一个统计单词的程序，Map生成(word，1)的数组，reduce是返回[]string的长度也就是单词数量
   
   main/mrsequential.go是顺序执行的mapreduce，作用就是读取文本文件，调用wc.go中的俩函数，最后输出一个(单词, 个数)的文件，原理很简单就是排序去重

   在main文件夹下执行`go build -race -buildmode=plugin ../mrapps/wc.go`，生成wc.so文件（动态库）
   
   然后执行`go run -race mrsequential.go wc.so pg*.txt`运行，最后生成mr-out-0文件


2. 要做的任务是实现一个分布式mapreduce，需要实现`mr/coordinator.go, mr/worker.go, mr/rpc.go`这三个文件
      在一个窗口执行执行 `go run -race mrcoordinator.go pg-*.txt` 
      在另一窗口执行 `go run -race mrworker.go wc.so`
      然后会生成mr-out-*文件（运行前先删除这个文件）

3. 测试

   运行：`bash test-mr.sh`就会进行测试，阅读这份代码可以知道运行时开启一个mrcoordinator进程三个mrworker进程，最后输出mr-out-*文件，然后将这些文件合并与顺序运行的结果进行比较即可