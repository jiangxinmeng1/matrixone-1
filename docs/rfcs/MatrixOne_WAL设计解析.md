# WAL设计解析

# 引入
WAL是全局共享的，保证acid：在事务提交之前刷盘，写memtable后写WAL。写wal是提交过程中最耗时的部分，这篇文章主要讲优化wal性能。

# 流程：
    事务验证确定不会回滚之后写完memtable，就写wal。等wal提交完才能正式提交事务。
    1. marshal打包entry，
    2. 拿lsn。
        串行拿lsn。这个lsn和事务的committs是一致的。提交顺序，事务可见性和replay顺序依赖这个。
    3. 打包成logservicerecord。
    4. 拿client，marshal record，
    5. 写logservice，
    6. 持久化后通知上层
    拿lsn之后，可以并发地写入memtable，写logtail entry。
# 并发：backend支持并发写
    写入logservicedriver是最耗时的部分。为了提高效率，并发写入logservice。
    只有打包record和拿client是串行的。
# 恢复的时候读乱序的lsn
    并发会导致乱序，重启的时候要按上层的lsn给Tn。
    上层的lsn写入logservice record，读的时候先读一批，

    ckp是按照上层lsn的切的，由于上层lsn和logservicelsn不完全一致，开头会有些零散的lsn要跳过
    然后以其中最小且连续的作为开头，(能确保和ckp一致)

    由于网络问题，可能导致有重复写入
    只处理哪些能与前面的lsn连起来的，丢弃重复写入

    最后丢弃末尾不连续的lsn，（这些lsn一定没提交）

# 合并日志
    为了节省资源？？，对很小的日志，会合并成一个logservice record，在打包成logservice record的时候，会看一下有没有同时提交的，如果有而且大小合适，会打包成一个record。

# 拆分日志，marshal的时候，把每个write op marshal出来同时计算累计的大小，可以贴一下entry的结构
    为了？？，会控制大小，在marshal的时候拆分过大的日志。entry的结构是：[头，write op1，write op2，...]，marshal的时候会一个一个地写entry，并计算大小，如果超过，就会拆分。

# 重复使用entry和logservice record。
    有一个pool，从里面拿entry和record。

# 展望：把marshal放到并发
    marshal entry也占了很多时间，为了提高并发。写wal的时候entry已经定好了不会改变，在定lsn前后marshal效果是一致的，将来会把marshal entry放到写logservice record之前来提高并发，提高性能。写memtable和wal可以并行
    流程图