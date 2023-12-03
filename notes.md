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

收到request时check，apply committed时check

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

**39. query config是否需要raft日志**

**40. golang map key/value 排序**

https://stackoverflow.com/questions/18695346/how-can-i-sort-a-mapstringint-by-its-values

**41. query previous**

查询以往的config，如果leader崩溃，是否能够保证previous config一定能查到呢？

假设leader收到一个最新的config

1.复制到大多数节点，leader崩溃，没有回复；clerk重发config到新的leader

2.没有复制到大多数，clerk重发config到新leader

3.leader回复后崩溃，查询旧config，但是新leader没有commit这个config，因此借助raft来commit

timeout设置ck.leader=-1，否则一直尝试leader

**42. allow more than one用处**

**43. shardkv为什么需要make_end**

与之前不同

有什么作用

**44. lock for clerk's leader id**

是否需要

**45. wrong group check**

**46. transfer设计**

process re-configurations one at a time, in order

是否需要所有server来transfer，还是leader就可以

如果需要所有server来，那么是否需要raft log commit之前所有在这个shard上的operation

**47. crash recovery后的config**

是否需要持久化config相关数据，例如lastCatchUp等

**48. db变为[Nshards]map[string][string]**

是否对之前的代码有影响

目前仅修改了rebuildStatus中database的定义

**49. race raft log persist**

If you put a map or a slice in a Raft log entry, and your key/value server subsequently sees the entry on the applyCh and saves a reference to the map/slice in your key/value server's state, you may have a race. Make a copy of the map/slice, and store the copy in your key/value server's state. The race is between your key/value server modifying the map/slice and Raft reading it while persisting its log.

two db slice copy

**50. wait when higher config transfer**

G0 -> config1 all G0 -> config2 transfer last 5 to G1

G1 current config 0 recieve transfer RPC with G0 target config2

**51. crash recovery**

no-op, previous config log can't replay

apply previous config relevant log too fast, can't catch up config

**52. wait shard 阻塞**

之前的设计如下

    pendingRequestCount++
    for !kv.allWaitOK(to_be_wait)
        kv.condForApply.Wait()
        cmd_index, _, leader := kv.rf.Start(no-op)
    pendingRequestCount--

考虑如下情况

034868 INFO G101 SK1 wait for agreement on wait shard with config num 2

040715 INFO G101 SK1 has pending request count 1, leader false, current term 3, previous check term 2, notify it to reply

040797 INFO G101 SK1 fail to try no-op at index 0 (no leader)

040900 LEAD S1 be leader at term 3

之后S1是leader，且term不变，就不会notifier了

no-op是否可以简化pending notify的处理

如下的逻辑是错误的

    if leader && curr_term != term
        kv.rf.Start(no-op)

可能term刚改变，但不是leader，之后变为leader就不会no-op

**53. slice of map 初始化**

https://doc.yonyoucloud.com/doc/wiki/project/the-way-to-go/08.4.html

**54. transfer时需要确保相关的操作都完成**

比如append时，正在transfer，则会迁移出一个value不正确的key

024903 INFO G101 SK0 get shard 2 Key 4 and Value 3X0blM_y6WLARPk

024904 INFO G101 SK0 get shard 3 Key 5 and Value Ik8DmY0wKX

起初允许在状态为transfer的shard，继续接收clerk的RPC

后来不允许，原因见59

**55. snapshot时的config, shard state**

如果不记录config，那么snapshot时只有last apply和db，等到恢复时config从1开始，实际上replay了很多重复的config相关的日志

shard state这个数组也是需要的，如果不包含，当snapshot时有将shard置为transfer状态的日志，还有将其置为others的日志

这时crash，如果没有shard state数组，rebuild时就相当于错过了这些日志，这时直接进入到wait，本应该为others的状态目前为mine

**56. apply wait in case of too ahead config**

**57. wake up lose**

    init_term := 0
    for res_index == -1 {
        if term, leader := kv.rf.GetState(); leader && init_term != term {
            kv.lastShardOpTerm = term
            init_term = term
            kv.transferShard(to_be_transfer, next_config)
        }
        if kv.getRequestResult() != -1 {
            break
        }
        kv.condForApply.Wait()
        res_index = kv.getRequestResult()
    }

如果kv.transferShard之后没有检查，直接wait，可能永久阻塞

一次虚假唤醒后，且第二次transferShard，此时恰好第一次transferShard完成并notify

此时第二次transferShard结束，直接wait，造成阻塞

**58. config log重复**

首先某个group更新到config 11，随后crash并恢复

这时依照旧的log可以更新到config 11，但是注意到比如从config 5更新到6时，需要transfer，这时leader就可能会Start一个重复的op

kv.lastApplyIndex == kv.rf.GetLastLogIndex()是否充分

**59. pending dead lock**

G101 SK2 wait for pending requests of transferred shards with config num 8 to finish

SK2等待pending的PutAppend RPC完成，但是PutAppend的cmd index又在transfer ok这个log之后

因为transfer ok是crash之前就有的

**60. 得到transfer的key-value后被误删**

059089 INFO G100 SK0 pull new config num 10, shard slice [100 100 100 100 100 102 102 102 102 102]

061830 INFO G100 SK0 pull new config num 11, shard slice [100 100 100 100 101 102 102 102 101 101]

064602 INFO G100 SK0 know agreement on transfer shard with config num 11 has reached

065114 INFO G101 SK0 applier get shard 4 Key 6 and Value gYiu4bQlrocGxU-Zw1BnUBvy from G100 config 11

065406 INFO G101 SK1 applier get shard 4 Key 6 and Value gYiu4bQlrocGxU-Zw1BnUBvy from G100 config 11

065584 INFO G101 SK2 applier get shard 4 Key 6 and Value gYiu4bQlrocGxU-Zw1BnUBvy from G100 config 11

067029 INFO G100 SK0 rebuild status with config num 11, shard state [1 1 1 1 0 0 0 0 0 0]

067209 INFO G100 SK0 applier get shard 4 Key 6 and Value gYiu4bQlrocGxU-Zw1BnUBvy from G101 config 12

067210 INFO G100 SK0 know agreement on transfer shard finish OK with config num 11 has reached

delete shards!

067211 INFO G100 SK0 know nothing to wait when catching up to config num 11

067211 INFO G100 SK0 catch up to config num 11, current shard status [1 1 1 1 1 0 0 0 0 0]

**61. kv.lastApplyIndex位置**

之前放在possible wait之前

059926 INFO G101 SK2 ready to apply committed command index 37 with G102 config num 11 and request id 11

059953 INFO G101 SK2 finish snapshot with total size 581 and last snapshot index 37

37这个日志被截断

059954 INFO G101 SK2 wait due to encounter ahead config num 11 from G102 while current config num 8

060175 INFO G101 SK2 finish wait, target config num 11 from G102 and current config num 10

060176 INFO G101 SK2 applier get shard 9 Key 1 and Value HtdeT-L1q5Hv from G102 config 11

060176 INFO G101 SK2 applier set shard [8 9] to MINE with config 11 and install request result len 2

060511 INFO G101 SK2 rebuild status with config 8, shard state [0 0 0 0 0 0 0 0 0 0], last apply index 37

这里的8,9都是0

应该要放在possible wait之后，且注意处理duplicate request的情况

**62. transfer result时的注意点**

不能transfer request id = 0的结果，这是无效的，也会对后续的transfer造成影响

034114 INFO G102 SK0 applier APPEND Key 1 and Value 4d65s from SKC2, request id 17

035629 INFO G100 SK2 transfer shard 0 with clerk id 2, request id 0

035671 INFO G102 SK0 install request result with shard 0, clerk id 2 and request id 0

035889 INFO G102 SK0 transfer shard 0 with clerk id 2, request id 0

gid相关的result需不需要被transfer

**63. install request result check**

检查peer的是否更新，若自己的old，则不install

request result整体逻辑
