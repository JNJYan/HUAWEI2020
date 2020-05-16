<!--
 * @Author: JNJYan
 * @LastEditors: JNJYan
 * @Email: jjy20140825@gmail.com
 * @Date: 2020-04-27 16:01:49
 * @LastEditTime: 2020-05-16 20:42:12
 * @Description: Modify here please
 * @FilePath: \HUAWEI2020\README.md
--> 
# 2020 Code Craft
## 热身赛

- `warmup.cpp` (0.4936)

## 初赛

- `0419_2.cpp`

    **递归DFS，6+1**

- `0426_multi.cpp`

    **多线程非递归DFS，5+2**

- `0428_4.cpp` (0.3399)

    **(hack data)多线程for循环逆序查找，5+2**

## 复赛

- `05165.cpp`

    **不整理了，累了**

## 总结

1. **版本管理！版本管理！版本管理！**，历史版本混乱，想找一个方便更改的版本，成功地凭借印象翻到了一个有bug的版本，复赛翻车。

2. 合理设计结构避免多线程加锁，数据队列多线程抢占，避免线程间负载不均衡。

3. mmap的优势在于多线程读写文件，生成环时统计字节数，以计算多线程写文件的偏移量。

4. double精度导致翻车，int64不香嘛。

5. 以上所述，代码里都没有。