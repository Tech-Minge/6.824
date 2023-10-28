**1. plugin编译**

https://github.com/microsoft/vscode-go/issues/3048

https://stackoverflow.com/questions/70642618/cannot-load-plugin-when-debugging-golang-file-in-vscode

https://zhuanlan.zhihu.com/p/496015711

**2. 大小写问题**

https://stackoverflow.com/questions/40256161/exported-and-unexported-fields-in-go-language

exported

**3. rpc**

return error

调用流程

**4. worker 等待**

一开始写成了所有任务均为非idle时，就可以退出，但是不正确，万一其他worker fail了

**5. parition file lock问题**

归属权，lock的顺序

**6. rename, atomic考虑**

os.create语义，如果已存在会如何

**7. defer unlock**

**8. 空接口**

interface{}

**9. atmoic**

atomic load & sotre 与 lock

**10. 2A的第二个测试超时问题**

没有启动另一个协程来startElection，导致选举时如果分票就会无限阻塞

**11. 投票后收到leader heartbeat将votefor置为-1**

**12. hold lock when rpc**

会导致dead lock

**13. candidate时request vote，但是得到大多数票时，已经是follower状态**

可能出现2个leader

**14. atomic mutex race检测**

**15. 空接口与struct**

struct内部有空接口

**16. voteFor = -1**

需要在更改term时设置，比如在send HeartBeat时发现term过时了，更新了term但是votefor不为-1，之后别人request vote就会失败

**17. figure 2细节**

比如If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate

only commit current term log

**18. rf.log获取subslice有race**

RPC的args有这个rf.log的读，后续有rf.log的写

**19. new(bytes.Buffer)**

gob.NewEncoder 具体流程

persister 原理

write crash 一致性 checksum

**20. nextIndex 优化原理**

leader re-init

https://blog.csdn.net/ouyangyiwen/article/details/119902194

**21. 收到old term的回复如何处理**

**22. go func() curr_term**

意义 是否需要实时check

check term 当reply success时，对rf.nextIndex等数据结构更改时

**23. batch 对test 2B的影响**

batch=1 40s左右

batch=20 >60s

**24. 选举和be leader的race**

861111 INFO S0 is kicked off at time 2023-10-02 15:35:50.247261106 +0800 CST m=+86.113856459

861113 LEAD S0 get Append RPC reply from S4 in failure due to timeout

861117 INFO S0 is about to sleep 516 ms at time 2023-10-02 15:35:50.247839229 +0800 CST m=+86.114434543

861117 INFO S0 get vote from S2

861119 LEAD S0 be leader at term 43 at time 2023-10-02 15:35:50.248067171 +0800 CST m=+86.114662466

861121 LEAD S0 with current term 43, prepare to send Append RPC at time 2023-10-02 15:35:50.248240321 +0800 CST m=+86.114835610

861124 ERRO S0 is leader at previous term 43, but start election at next term 44

**25. 不同log len对log commit的影响**

下面的例子导致commit到了49

620674 LEAD S0 with current term 9, prepare to send Append RPC at time 2023-10-02 17:33:40.400626114 +0800 CST m=+62.070150017

620676 LEAD S0 leader send Append RPC to S1 with log start from 7 to 48

620679 LEAD S0 leader send Append RPC to S4 with log start from 30 to 48

620686 LEAD S0 leader send Append RPC to S3 with log start from 30 to 48

620687 LEAD S0 get command at log index 49 with current term 9

620696 LEAD S0 leader send Append RPC to S2 with log start from 2 to 49

620817 LEAD S0 append log from 7 to 48 to follower S1 in success

620982 LEAD S0 append log from 2 to 49 to follower S2 in success

620982 LEAD S0 leader incremnt log commit index from 6 to 49

另外copy(log, rf.log[prev_log_index+1:log_len]) 设置log len

104010 LEAD S2 increment next index of S3 from 87 to 90

104011 LEAD S2 before sending Append RPC check term ok, current term 3

104012 LEAD S2 want to send log entry to S1 from prev log index 51(not include) to log len 91, current term 3 current total log len 91

104013 LEAD S2 leader send Append RPC to S1 with log start from 52 to 90

104014 LEAD S4 get Append RPC reply from S0 in failure due to timeout

104015 LEAD S2 before sending Append RPC check term ok, current term 3

104016 LEAD S2 want to send log entry to S3 from prev log index 89(not include) to log len 91, current term 3 current total log len 91

104016 LEAD S2 leader send Append RPC to S3 with log start from 90 to 90

104021 LEAD S2 before sending Append RPC check term ok, current term 3

104022 LEAD S2 want to send log entry to S4 from prev log index 1(not include) to log len 88, current term 3 current total log len 91

104022 LEAD S2 leader send Append RPC to S4 with log start from 2 to 87

104033 INFO S3 receive Append RPC normally from S2, heart beat at time 2023-10-03 12:35:20.43122001 +0800 CST m=+10.406158718

104035 INFO S3 receive Append RPC with previous index 89 and previous term 3, leader commit 51, args log len 1

104036 INFO S3 find no conflict with leader at log, my log len 90

104037 INFO S3 append leader log due to leader is longer, my log current len 91

104040 LEAD S2 before sending Append RPC check term ok, current term 3

104041 LEAD S2 want to send log entry to S3 from prev log index 89(not include) to log len 88, current term 3 current total log len 91

panic: runtime error: makeslice: len out of range

**26. chan和mutex混用导致死锁**

sendCommitedLog 在持有rf.mu时向无buffer的channel发送数据，另一方面applier需要获取rf.mu来snapshot，这之后才会从channel读取数据

applierSnap 函数

**27. clerk time sleep时长**

**28. no-log if no other request**

**29. clerk id, request id persist**

**30. clerk request的先后顺序**

**31. check重复请求**

收到request时check，apply committed时chec

**32. leader被隔离到小部分**

收到request后，始终等待log被复制到大部分follower，但是回归到整体后，仍旧卡住

**33. leader 收到新请求**

leader0收到最新的请求，然后被隔离，client被卡死在RPC，leader0没有可以apply的新op，不会broadcast

即便后来隔离恢复，client由于被卡死，不会再次发送请求，导致该请求不会被响应

新增notify，原始每隔100 ms提醒，但是仍有问题

比如leader收到新请求，replicate到几个follower，发现自己old term，因为之前被隔离的follower会快速增长term，于是leader重新选举，再次成为leader

即便notify，仍会将client的RPC卡死

notify需要check term

**34. labgob.Register(Op{})**

**35. instant commit log**

unlock 之后向channel发送applymsg，此时可能out of order

即62-64 unlock - 65 unlock - 65 chan - 62 chan

**36. atomic compare swap**

https://stackoverflow.com/questions/75122412/what-does-it-mean-for-gos-compareandswap-to-return-false

**37. raft sendcommittedlog**

需要传入rf.lastSnapshotIndex，不能在内部获取，即便加上了lock

**38. persit细究**

raft和KV server关系