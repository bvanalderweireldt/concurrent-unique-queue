# concurrent-unique-queue
A Java concurrent unique queue backed by a Set (LinkedHashSet), for when you need to have a Queue with unique elements. 
The ConcurrentSetBlockingQueue will behave the same way if the queue is full or if a duplicate entry already exists in the Queue.

**Your element needs to have a proper hash and equals implementations !**

```xml
<dependency>
  <groupId>com.hybhub</groupId>
  <artifactId>concurrent-util</artifactId>
  <version>0.1</version>
</dependency>
```

# Usage

```java
#ConcurrentSetBlockingQueue implements BlockingQueue
BlockingQueue<Integer> queue = new ConcurrentSetBlockingQueue<>();

queue.offer(1); # True
queue.offer(2); # True
queue.offer(2); # False
```