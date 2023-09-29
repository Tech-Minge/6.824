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

**18. rf.log获取subslice有race**

RPC的args有这个rf.log的读，后续有rf.log的写
