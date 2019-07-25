import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * @author chenwen on 2019-07-25
 * Twitter_Snowflake<br>
 * SnowFlake的结构如下(每部分用-分开):<br>
 * 0 - 0000000000 0000000000 0000000000 0000000000 0 - 00000 - 00000 - 000000000000 <br>
 * 1位标识，由于long基本类型在Java中是带符号的，最高位是符号位，正数是0，负数是1，所以id一般是正数，最高位是0<br>
 * 41位时间截(毫秒级)，注意，41位时间截不是存储当前时间的时间截，而是存储时间截的差值（当前时间截 - 开始时间截)
     * 得到的值），这里的的开始时间截，一般是我们的id生成器开始使用的时间，由我们程序来指定的（如下下面程序IdWorker类的startTime属性）。41位的时间截，可以使用69年，年T = (1L << 41) / (1000L * 60 * 60 * 24 * 365) = 69<br>
 * 10位的数据机器位，可以部署在1024个节点，包括5位datacenterId和5位workerId<br>
 * 12位序列，毫秒内的计数，12位的计数顺序号支持每个节点每毫秒(同一机器，同一时间截)产生4096个ID序号<br>
 * 加起来刚好64位，为一个Long型。<br>
 * SnowFlake的优点是，整体上按照时间自增排序，并且整个分布式系统内不会产生ID碰撞(由数据中心ID和机器ID作区分)，并且效率较高，经测试，SnowFlake每秒能够产生26万ID左右。
 *
 */
public class FastSnowFlake {
  /**
   * 起始的时间戳, 2019-01-01 00:00:00
   */
  private final static long START_STMP = 1546272000000L;

  /**
   * 每一部分占用的位数
   */
  private final static long SEQUENCE_BIT = 12; //序列号占用的位数

  /**
   * 128台机器
   */
  private final static long MACHINE_BIT = 7;   //机器标识占用的位数

  /**
   * 8个数据中心
   */
  private final static long DATACENTER_BIT = 3;//数据中心占用的位数

  /**
   * 每一部分的最大值
   */
  private final static long MAX_DATACENTER_NUM = ~(-1L << DATACENTER_BIT);
  private final static long MAX_MACHINE_NUM = ~(-1L << MACHINE_BIT);
  private final static long MAX_SEQUENCE = ~(-1L << SEQUENCE_BIT);

  /**
   * 每一部分向左的位移
   */
  private final static long MACHINE_LEFT = SEQUENCE_BIT;
  private final static long DATACENTER_LEFT = SEQUENCE_BIT + MACHINE_BIT;
  private final static long TIMESTMP_LEFT = DATACENTER_LEFT + DATACENTER_BIT;

  private long datacenterId;  //数据中心
  private long machineId;     //机器标识
  private long sequence = 0L; //序列号
  private long lastStmp = START_STMP;//上一次时间戳
  private long allowLoadMills; //允许透支的时间
  private ReentrantLock lock;

  public FastSnowFlake(long datacenterId, long machineId, long allowLoadMills) {
    if (datacenterId > MAX_DATACENTER_NUM || datacenterId < 0) {
      throw new IllegalArgumentException("datacenterId can't be greater than MAX_DATACENTER_NUM or less than 0");
    }
    if (machineId > MAX_MACHINE_NUM || machineId < 0) {
      throw new IllegalArgumentException("machineId can't be greater than MAX_MACHINE_NUM or less than 0");
    }
    this.datacenterId = datacenterId;
    this.machineId = machineId;
    this.allowLoadMills = allowLoadMills < 0 ? 0 : allowLoadMills;
    this.lock = new ReentrantLock();
  }

  public FastSnowFlake(long datacenterId, long machineId) {
    this(datacenterId, machineId, 0);
  }

  /**
   * 产生下一个ID
   */
  public long nextId() {
    try {
      lock.lock();
      return unsafeNextId(Long.MAX_VALUE, TimeUnit.SECONDS);
    } finally {
      lock.unlock();
    }
  }

  /**
   * 产生下一个ID
   */
  public long nextId(long timeout, TimeUnit timeUnit) throws Exception {
    boolean locked = false;
    try {
      locked = lock.tryLock(timeout, timeUnit);
      if (locked) {
        return unsafeNextId(timeout, timeUnit);
      }
    } catch (InterruptedException ignore) {
    } finally {
      if (locked) {
        lock.unlock();
      }
    }
    throw new TimeoutException("timeout to get nextId");
  }

  private long unsafeNextId(long timeout, TimeUnit timeUnit) {
    //相同毫秒内，序列号自增
    sequence = (sequence + 1) & MAX_SEQUENCE;
    //同一毫秒的序列数已经达到最大
    if (sequence == 0L) {
      lastStmp = lastStmp + 1;
      ensureNextMill(timeout, timeUnit);
    }
    return (lastStmp - START_STMP) << TIMESTMP_LEFT //时间戳部分
           | datacenterId << DATACENTER_LEFT       //数据中心部分
           | machineId << MACHINE_LEFT             //机器标识部分
           | sequence;                             //序列号部分
  }

  /**
   * 确保透支时间
   */
  private void ensureNextMill(long timeout, TimeUnit timeUnit) {
    final long interval = lastStmp - System.currentTimeMillis();
    if (interval > timeUnit.toMillis(timeout)) {
      throw new RuntimeException("cat not allocation id in " + timeUnit.toMillis(timeout) + "(ms)");
    }
    if (interval > allowLoadMills) {
      try {
        Thread.sleep(interval);
      } catch (InterruptedException ignore) {}
    }
  }

  public static void main(String[] args) {
    int[] threads = new int[] {1, 2, 4, 8, 16, 32, 64, 128};
    final Function<FastSnowFlake, Long> fastSnowFlakeFunction = fastSnowFlake -> {
      try {
        return fastSnowFlake.nextId(10, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e.getMessage());
      }
    };
    for (int thread : threads) {
      perfTests(thread, 0, (1 << 12), fastSnowFlakeFunction, "nextId");
    }
  }

  public static long perfTests(int threads, int allowLoadMills, int count, Function<FastSnowFlake, Long> idGenFunction, String name) {
    long startTime = System.currentTimeMillis();

    List<FastSnowFlake> snowFlakes = new ArrayList<>();
    for (int i = 0; i < (1 << DATACENTER_BIT); i++) {
      for (int j = 0; j < (1 << MACHINE_BIT); j++) {
        FastSnowFlake snowFlake = new FastSnowFlake(i, j, allowLoadMills);
        snowFlakes.add(snowFlake);
      }
    }

    final ExecutorService executorService = Executors.newFixedThreadPool(threads);

    for (FastSnowFlake snowFlake : snowFlakes) {
      for (int i = 0; i < count; i++) {
        executorService.submit(() -> {
          idGenFunction.apply(snowFlake);
        });
      }
    }

    executorService.shutdown();

    try {
      executorService.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    long endTime = System.currentTimeMillis();

    System.out.println("---- name=" + name +"------ threads=" + threads + " ------ allowLoadMill=" + allowLoadMills + " ------ costTime=" + (endTime - startTime));

    return endTime - startTime;
  }
}
