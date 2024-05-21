# WAL设计解析

# WAL是全局共享的，保证acid：在事务提交之前刷盘，写memtable和写WAL同时进行提高效率。

# 流程：事务验证确定不会回滚之后，marshal打包entry，拿lsn，打包成logservicerecord，写logservicedriver，持久化后通知上层，然后事务正式提交，向cn推logtail
拿lsn之后，可以并发地写入memtable，写logtail entry。
# 并发：backend支持并发写
# 恢复的时候读乱序的lsn
# 拆分日志，marshal的时候，把每个write op marshal出来同时计算累计的大小，可以贴一下entry的结构
# 控制窗口（上次写过）
# 展望：把marshal放到并发
