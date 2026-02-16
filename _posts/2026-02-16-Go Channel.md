---
title: Go Channel
#author: Zyuer
date: 2026-02-16 15:00:00 +0800
categories: [Golang]
pin: false
tags: [gochannel,goroutine,go]  #	文章标签，用于分类和检索
---

[参考资料](https://www.bilibili.com/video/BV1bScEzRE2v?vd_source=d40955b0ebb31fde7200ca58917cab4e&spm_id_from=333.788.videopod.sections&p=6)

## 数据存储

`go/src/runtime/chan.go`中，chan的底层结构为 `hchan`

```go
// hchan是Channel的底层结构体
type hchan struct {
	qcount   uint           // 队列中元素数量
	dataqsiz uint           // 环形队列容量
	buf      unsafe.Pointer // 环形队列缓冲区
	elemsize uint16         // 元素大小
	closed   uint32         // 是否关闭
	elemtype *_type         // 元素类型信息
	sendx    uint           // 发送操作的指针位置
	recvx    uint           // 接收操作的指针位置
	recvq    waitq          // 等待接收的goroutine队列
	sendq    waitq          // 等待发送的goroutine队列

	// 核心：Channel的互斥锁，保护所有字段
	lock mutex
}
```

* **buf      unsafe.Pointer**：指向实际缓冲区的一个指针，缓冲区是一个数组。并且是**环形数组**。
* **sendx    uint**：发送索引，指明下次发送数据应进缓冲区的哪一位置。
* **recvx    uint**：接收索引，指明下次接受数据应取缓冲区的哪一位置。

下图中s代表 `sendx`发送索引；r代表 `recvx`接收索引。

```tex
buf [][][][]       //初始状态，dataqsiz为4
	  r 
	  s
	  
buf [1][][][]      //ch<-1,    sendx = (sendx+1)%dataqsiz
	  r  s
buf [1][2][][]     //ch<-2,    sendx = (sendx+1)%dataqsiz
	  r      s 
buf [1][2][3][]    //ch<-3,    sendx = (sendx+1)%dataqsiz
	  r         s 
buf [1][2][3][4]   //ch<-4,    sendx = (sendx+1)%dataqsiz      ！阻塞
	  r  
    s
buf [][2][3][4]   // <-ch,取1  recvx = (recvx+1)%dataqsiz
	  s  r  
buf [5][2][3][4]   //ch<-5,    sendx = (sendx+1)%dataqsiz       ！阻塞
	      r 
        s
```

**注意到：recvx==sendx时阻塞。buf全空则读阻塞，全满则写阻塞。**

> 实际上，gochan对于环形数组索引的约束，不采用%取模运算，而是 `if index==dataqsiz {index =0}`

具体每一次存取到底操作哪一地址，由 `chanbuf`函数决定，这个函数很简单只有一行。

```go
func chanbuf(c *hchan, i uint) unsafe.Pointer {
    return add(c.buf, uintptr(i)*uintptr(c.elemsize))
}
```

我们在使用 `ch := make(chan int,4)`时，ch就是一个指向buf的指针，**ch地址值  +  i  * 元素大小** 可得出元素地址值。

---

## 并发安全

```go
func chansend(c *hchan, ep unsafe.Pointer, block bool, callerpc uintptr) bool {
	//....
	lock(&c.lock)
	//... 执行写操作
	unlock(&c.lock)
  
```

`hchan.lock` 的类型为 `mutex`，但这里的 `mutex` 并非标准库 `sync.Mutex`，而是 Go 运行时内部的 `runtime.mutex`（定义在 `src/runtime/mutex.go`），是专门为运行时高频、短持有时间场景定制的极简自旋锁。

`runtime.mutex` 和标准库 `sync.Mutex` 核心逻辑**差异极大**：`runtime.mutex` 是纯自旋锁（无阻塞、无等待队列），而 `sync.Mutex` 是「自旋 + 信号量阻塞」的混合锁；`runtime.mutex` 舍弃了复杂的阻塞唤醒逻辑，仅保留核心的原子操作，以适配运行时无 GC 干扰、锁持有时间极短的场景。核心结构和流程如下：

> #### 1. 核心状态（简化版，贴合 Go 源码）
>
> ```go
> // 定义在 src/runtime/mutex.go
> type mutex struct {
> 	// 仅用 0/1 表示锁状态，无等待者标记/计数：
> 	// 0: 未锁定
> 	// 1: 锁定
> 	key uint32
> }
> ```
>
> #### 2. 加锁（`lock(&c.lock)`）流程
>
> Channel 的 `lock()` 调用最终映射到 `runtime.mutex` 的 `lock()` 方法，核心是 “CAS 抢锁 + 轻量自旋”，无阻塞、无等待队列：
>
> ```go
> func lock(l *mutex) {
> 	// 步骤1：快速尝试 CAS 加锁（无竞争时直接成功）
> 	if atomic.CompareAndSwapUint32(&l.key, 0, 1) {
> 		return
> 	}
> 	// 步骤2：有竞争时轻量自旋（CPU 空转），循环尝试 CAS 抢锁
> 	// 自旋仅执行 PAUSE 指令让出 CPU 周期，不休眠 Goroutine
> 	for {
> 		// procyield(30)：执行 30 次 PAUSE 指令，空转等待锁释放
> 		procyield(30)
> 		// 再次尝试 CAS 抢锁，成功则退出循环
> 		if atomic.CompareAndSwapUint32(&l.key, 0, 1) {
> 			return
> 		}
> 	}
> }
> ```
>
> #### 3. 解锁（`unlock(&c.lock)`）流程
>
> 解锁逻辑极简，仅释放锁，无任何唤醒操作（因为没有等待队列）：
>
> ```go
> func unlock(l *mutex) {
> 	// 直接将锁状态置为未锁定，无需判断等待者（无等待队列）
> 	atomic.StoreUint32(&l.key, 0)
> }
> ```

Channel 的所有读写操作（`ch<-`/`<-ch`）、关闭操作（`close(ch)`）都必须先加锁，核心保护以下临界资源，避免多 Goroutine 并发访问导致的数据竞争：

1. 环形缓冲区的读写指针（`sendx`/`recvx`）：防止多 Goroutine 同时修改指针，导致缓冲区读写位置错乱；
2. 队列元素计数（`qcount`）：保证缓冲区中元素数量统计的准确性；
3. 等待队列（`recvq`/`sendq`）：避免在唤醒等待 Goroutine 时，出现重复唤醒、漏唤醒或队列操作异常；
4. 关闭标记（`closed`）：防止在发送 / 接收数据的同时关闭 Channel，导致程序 panic 或数据读写异常。

## 阻塞机制

```plaintext
┌─────────┐
│    M    │ ----> 绑定 ──>   ┌─────────┐
│  线程   │                  │    P    │
└─────────┘                 │ 逻辑处理器│
    │                       └─────────┘
    ▼                             │ 维护本地可运行G队列
    G0                            ▼
                        ┌───G───G───G───G───┐
                        │  协程1 协程2 协程3  │
                        └────────────────────┘
```

上述为[GMP调度](https://zyuer.cn/posts/GMP/)场景，现在M所绑定执行的协程G0正在读写channel而被阻塞。

| question             | answer                                                        |
| :------------------- | ------------------------------------------------------------- |
| 谁来阻塞Go           | 调度器将G0从runable转化为waiting                              |
| G0阻塞完放在什么地方 | sendq\|recvq保存waitting态的协程                              |
| G0 什么时候被唤醒    | buf不全满｜buf不全空时唤醒                                    |
| 谁来唤醒Go           | 调度器runtime.schedule调用goready()，将G0从waiting转为runable |

## 读取优化

```go
// Channel 初始状态
buf [][][][]  // 缓冲区，dataqsiz=4（容量4）
		r
		s
recvq        // 读协程阻塞队列（存储 sudog 节点）
sendq        // 写协程阻塞队列（本场景暂未用到）
```

1. 协程 G1 执行读操作 `x <- ch`，此时 Channel 无数据、缓冲区为空 → G1 读阻塞。
2. 调度器 P 处理：将 G1 从 **runnable（就绪态）** 转为 **waiting（等待态）**，并存入 `recvq`（读阻塞队列）。
3. 队列节点结构：每个节点不仅存阻塞协程 G，还存协程读写的「目标变量地址」

   示例：（G1 的接收变量 x 是其私有栈空间的局部变量）

   ```
   recvq <— [G1 | &x] <— [...]
   ```

现在举个例子来说明gochannel是如何利用上述recvq的结构实现读写优化的

```go
go func(){ 
  x <-ch //G1. x是局部变量，在G1的栈空间 
}() 
go func(){
  ch<-1 //G2  G2会把栈空间中的1 ，直接值拷贝给G1栈空间的x 
}()
```

##### 正常流程（未优化）

G2 执行 `ch <- 1` → 数据写入 buf → 唤醒 G1 → G1 抢锁 → 从 buf 取值 → G1 转为 runnable。

##### 优化后流程（核心）

G2 执行 `ch <- 1` 时，数据不写入 buf，直接传递给 G1 的 x，避免锁竞争和缓冲区操作：

```plaintext
【G1私有栈】          【Channel】          【G2私有栈】
  x(&x)              recvq:[G1|&x]         1(&1)
    │                   │                   │
1.  │──G1阻塞入队───────>│                   │
    │                   │                   │
2.  │                   │<──G2发起写操作────│
    │                   │                   │
3.  │<──直接拷贝1→&x─────│──值拷贝(&1→&x)──>│
    │                   │                   │
4.  │──G1唤醒(runnable)─>│                   │
    │                   │                   │
5.  │──G1直接用x(值=1)──>│ (无需抢锁/读buf)  │
```

#### 优化实现

1. 协程栈空间特性：G1 和 G2 均有独立私有栈空间，x 属于 G1 栈，1 属于 G2 栈。
2. 数据传递本质：G2 通过 `recvq` 头节点获取 G1 的「x 地址（&x）」，将自身栈中的「1」直接 **值拷贝** 到 G1 栈的 x 地址中。
3. 调度器动作：G2 完成拷贝后，调度器唤醒 G1 → 将 G1 从 waiting 转为 runnable，放入 P 的就绪队列，等待调度执行。
4. 核心优势：G1 无需抢锁、无需读取缓冲区，直接使用已赋值的 x，提升效率。

#### 总结

- 优化核心：利用 `recvq` 存储的「变量地址」，实现跨协程栈的直接数据拷贝，跳过缓冲区和锁竞争。
- 队列节点作用：`recvq` 的 sudog 节点（存 G + 变量地址）是实现该优化的核心数据结构支撑。
- 数据流向：G2 栈的 1 → 直接拷贝 → G1 栈的 x（无需经过 buf）。

#### 源码

`go/src/runtime/chan.go` line 312

```go
func send(c *hchan, sg *sudog, ep unsafe.Pointer, unlockf func(), skip int) {
    // 1. 同步测试校验（synctest 相关，非核心逻辑）
    if c.synctest && sg.g.syncGroup != getg().syncGroup {
       unlockf()
       panic(plainError("send on synctest channel from outside bubble"))
    }

    // 2. 竞态检测（raceenabled 开启时的模拟缓冲区操作，非核心）
    if raceenabled {
       if c.dataqsiz == 0 {
          racesync(c, sg)
       } else {
          // 即使直接拷贝，也模拟缓冲区的 head/tail 偏移（仅竞态检测用）
          racenotify(c, c.recvx, nil)
          racenotify(c, c.recvx, sg)
          c.recvx++
          if c.recvx == c.dataqsiz {
             c.recvx = 0
          }
          c.sendx = c.recvx
       }
    }

    // 3. 【核心重点】直接值拷贝（跳过缓冲区的关键）
    if sg.elem != nil {   // sg.elem 是接收方（如G1）的变量地址（&x）
       // sendDirect：将发送方的 ep（如G2的&1）拷贝到 sg.elem（G1的&x）
       sendDirect(c.elemtype, sg, ep) 
       sg.elem = nil // 拷贝完成后清空，避免重复操作
    }

    // 4. 唤醒接收协程的收尾操作
    gp := sg.g          // 获取阻塞的接收协程（如G1）
    unlockf()           // 解锁 channel（无需操作缓冲区，提前解锁）
    gp.param = unsafe.Pointer(sg) // 设置协程参数
    sg.success = true   // 标记发送成功
    if sg.releasetime != 0 {
       sg.releasetime = cputicks() // 记录唤醒时间（性能统计）
    }
    goready(gp, skip+1) // 唤醒接收协程（G1），转为 runnable 态
}


func sendDirect(t *_type, sg *sudog, src unsafe.Pointer) {
    dst := sg.elem // 取接收方地址（如G1的&x）
    // 内存屏障：保证多CPU下拷贝的原子性/可见性
    typeBitsBulkBarrier(t, uintptr(dst), uintptr(src), t.Size_)
    memmove(dst, src, t.Size_) // 核心：src(G2的&1) → dst(G1的&x) 字节级拷贝
}
```


## Closed Channel

当你尝试向一个已经关闭的通道写入数据时，Go 运行时会直接触发 **panic**，错误信息通常为 `send on closed channel`。这是一种运行时错误，会导致当前协程终止，如果没有被 `recover` 捕获，还会导致整个程序崩溃。

```go
package main

import (
	"fmt"
	"time"
)

func main() {
	ch := make(chan int)
	close(ch)
	go func() {
		defer func() {
			// 捕获 panic 并打印信息
			if err := recover(); err != nil {
				fmt.Printf("协程写入时触发 panic: %v\n", err)
			}
		}()
		ch <- 10 // 向已关闭的通道写数据
	}()
	time.Sleep(100 * time.Millisecond)
	fmt.Println("程序正常退出")
}
```

```plaintext
协程写入时触发 panic: send on closed channel
程序正常退出
```

关键说明：无论通道是有缓冲还是无缓冲，**写入已关闭的通道都会触发 panic**，这是 Go 语言的硬性规则，目的是避免向已终止的通信管道写入无效数据。

从已关闭的通道读取数据不会触发 panic，行为分为两种场景，核心规则是：先读完通道中已缓冲的数据，再读则返回对应类型的零值，同时第二个返回值（ok）为 false。

```go
package main

import "fmt"

func main() {
	ch := make(chan int, 1)
	ch <- 5
	close(ch)

	// 第一个读取：获取通道中已有的数据
	val1, ok1 := <-ch
	fmt.Printf("第一次读取：值=%d, ok=%t\n", val1, ok1) // 输出：值=5, ok=true

	// 第二个读取：通道已空，返回零值和 false
	val2, ok2 := <-ch
	fmt.Printf("第二次读取：值=%d, ok=%t\n", val2, ok2) // 输出：值=0, ok=false

	// 启动协程读取已关闭的空通道
	go func() {
		val, ok := <-ch
		fmt.Printf("协程读取：值=%d, ok=%t\n", val, ok) // 输出：值=0, ok=false
	}()

	// 等待协程执行
	fmt.Scanln()
}
```

**补充**：

- 对于有缓冲通道：关闭后仍能按顺序读取完缓冲区内的所有数据，每一次读取的 `ok` 都是 `true`；当缓冲数据读完后，后续所有读取都会返回零值 + `false`。
- 对于无缓冲通道：如果关闭前没有数据写入，关闭后第一次读取就会返回零值 + `false`。
- 即使是在协程中读取已关闭的通道，行为和主协程完全一致，不会有任何异常。

### 总结

1. 向已关闭的通道写入数据：必然触发 `send on closed channel` panic，需通过提前判断或避免关闭后写入来规避。
2. 从已关闭的通道读取数据：安全且不 panic，先读取通道内剩余数据（ok=true），数据读完后返回对应类型零值（ok=false）。
3. 通道关闭的核心原则：关闭操作应由**写端**发起，读端通过 `ok` 判断通道是否关闭，避免写端误写已关闭通道引发 panic。
