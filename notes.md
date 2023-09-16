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