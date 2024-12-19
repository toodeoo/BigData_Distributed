## 大规模数据处理期末项目 - 探求影响 MapReduce 性能的因素

#### 郑勤 成涵吟 方蕴仪 许瑞琪

## 实验目的

考察影响 MapReduce 性能的因素

## 设计思路

1. 在 MapReduce 中，map 结果需要经过 shuffle 阶段，这个过程会涉及大量的磁盘读写。通过在 shuffle 之前加入 Combine 操作，将键相同的键值对先进行归并，再写入磁盘，可以有效减少磁盘 IO 操作，从而提升性能。
2. 在 MapReduce 的 shuffle 阶段，通过对 map 结果进行压缩后再存储到磁盘，可以减少写入和读取的数据量，从而提高性能。
3. 调整 Hadoop 配置参数，适当设置 Map 和 Reduce 任务的数量。过多的任务会增加调度和启动的开销，而过少的任务会导致单个任务负载过高，选择合适的参数可以提高整体性能。
4. 通过在分布式环境中部署 MapReduce，研究其在分布式环境下下的性能表现，并分析分布式部署对 MapReduce 性能的影响。

## 实验设置

| 名称         | 设置                 |
| ------------ | -------------------- |
| 本机操作系统 | Windows 10           |
| IDEA         | IntelliJ IDEA 2022.1 |
| JDK          | JDK 1.8              |
| 云端总节点数 | 4                    |
| Hadoop 版本  | 2.10.1               |
| 数据集       | pd.train             |
| 数据集大小   | 2.02GB               |

## 实验过程

#### 单机 + 原始版本 WordCount

**Map 阶段**：将输入数据集分割成多个小块，并对每个小块进行处理。每个小块中的每一行数据都会被拆分成单词，并为每个单词生成一个键值对 (word, 1)。**Shuffle 阶段**：将所有 map 任务的输出结果进行重排，根据键值对中的键对数据进行排序和分组。相同的键会被分配到同一个 reduce 任务中。 **Reduce 阶段**：对每个分组中的键值对进行处理，将相同键的值累加，得到每个单词在数据集中出现的总次数。

**性能分析**：记录单机环境下运行原始版本 WordCount 程序的执行时间和资源消耗情况，作为基准性能数据。统计得到运行时间为 335s。运行结果截图如下：
![单机+WordCount](img\SimWordCount.png)

#### 单机 + Combine

与原始版本的 MapReduce 相比，加入了 combine 的 MapReduce 就是在 map 计算出中间文件前做一个简单的合并重复 key 值的操作。由于数据量较大，每一个 map 都可能会产生大量的本地输出，Combiner 的作用就是对 map 端的输出先做一次合并，以减少在 map 和 reduce 节点之间的数据传输量，以提高网络 IO 性能。

**代码实现**：我们需要新建一个 `WordCountCombiner` 方法，继承 Reducer 方法，对每个具有相同键值对的值进行计数，输出类型为 Text 和 IntWritable 键值对。

**性能分析**：加入了 Combine 的 WordCount 运行时间为 217s。运行结果截图如下：
![单机+Combine](img\SimCombine.png)
可以看出，使用 Combine 后运行时长约为原始版本的 65%，其运行效率得到了显著的提升。
![单机+Combine对比](img\Sim1.png)

#### 单机 + Compress

使用 Compress 方法对 MapReduce 性能进行优化也是一种常用的手段，通过对 map 结果进行压缩后再存储到磁盘，可以减少写入和读取的数据量。但 Compress 方法的缺点也很明显：这种方法极大增加了 CPU 的开销（频繁计算带来的频繁压缩与解压缩）。

**代码实现**：只需要在 `WordCount` 主方法中添加压缩的部分，在这里为了便于本地运行，选择了 hadoop 自带的 bzip2 压缩格式。

**性能分析**：加入了 Compress 的 WordCount 运行时间为 2309s，运行结果如下：
![单机+Compress](img\SimCompress.png)
在 shuffle 数量方面，原始版本的 shuffle 量为 28239821，而加入 Compress 之后 shuffle 数量变为 16343504，约为原始版本的 58%，大大减少了磁盘 IO 以及 shuffle 过程中的网络 IO ，从而提升了性能。
![单机+Combine对比](img\Sim2.png)
而在运行时间方面，由于频繁计算带来的频繁压缩与解压缩操作导致 CPU 开销加大，运行时间也极大地增加。
![单机+Combine对比](img\Sim3.png)

#### 单机 + 参数调整

适当设置 Map 和 Reduce 任务的数量也可以提升 MapReduce 性能。过多的任务会增加调度和启动的开销，而过少的任务会导致单个任务负载过高，选择合适的参数可以提高整体性能。
**代码实现**：增加 map 和 reduce 的工作节点数量这两个参数，统计分别遍历二者取值为 1~8 时的运行时间结果，

#### 分布式部署

部署四台虚拟机，其中 ecnu01 为主节点，ecnu02、ecnu03 为从节点，ecnu04 为客户端。在 HDFS 的 input 文件夹中存储 `pd.train` 数据集，主节点上启动 HDFS 和 yarn 服务，并由客户端提交 jar 包运行应用。在 http://ecnu01:19888/ 中可以查询到运行结果如图：
![分布式+WordCount](img\DisOrigin.png)
![分布式+combine](img\DisCombine.png)
![分布式+compress](img\DisComp.png)
本机与分布式运行时间对比如下：
![分布式](img\dis.png)
可以看出，使用分布式部署之后不同方法的运行时间都有了显著下降，其中尤其是使用 Compress 方法的 WordCount ，运行时间仅为单机部署的 37.5%，总之，分布式部署可以有效缩短 MapReduce 的时间开销。
