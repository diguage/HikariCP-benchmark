package com.zaxxer.hikari.benchmark;

import com.google.common.base.Joiner;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.benchmark.stubs.StubDriver;
import com.zaxxer.hikari.benchmark.stubs.StubStatement;
import com.zaxxer.hikari.pool.HikariPool;
import com.zaxxer.hikari.pool.HikariPoolAccessor;
import com.zaxxer.hikari.util.UtilityElf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_IN_USE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_NOT_IN_USE;
import static com.zaxxer.hikari.util.UtilityElf.quietlySleep;
import static java.lang.Integer.parseInt;
import static java.lang.System.nanoTime;
import static java.lang.Thread.MAX_PRIORITY;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.*;

public class StressLoadTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(StressLoadTest.class);

    private static final String driver = "com.mysql.jdbc.Driver";
    public static final String jdbcUrl = "jdbc:mysql://localhost:3306/employees?useUnicode=true&characterEncoding=utf-8&autoReconnectForPools=true&autoReconnect=true";
    private static final String username = "root";
    private static final String password = "123456";

    private static final int MIN_POOL_SIZE = 5;
    private static final int MAX_POOL_SIZE = 50;

    private DataSource DS;

    private int requestCount;

    private String pool;

    private HikariPoolAccessor hikariPoolAccessor;

    private AtomicInteger threadsRemaining;

    private AtomicInteger threadsPending;

    private ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private volatile boolean stoped = false;
    private static final String SQL = "SELECT 1";

    public static void main(String[] args) throws InterruptedException {
        StressLoadTest test = new StressLoadTest();

        test.setup(args);

        test.start(parseInt(args[0]));
    }

    private void setup(String[] args) {
        try {
            Class.forName(driver);
            Driver driver = DriverManager.getDriver(jdbcUrl);
            LOGGER.info("Using driver ({}): {}", jdbcUrl, driver);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        pool = args[1];
        IntStream.of(0, 1).forEach(i -> {
            switch (pool) {
                case "hikari":
                    setupHikari();
                    hikariPoolAccessor = new HikariPoolAccessor(getHikariPool(DS));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown connection pool specified");
            }

            if (i == 0) {
                try {
                    LOGGER.info("Warming up pool...");
                    LOGGER.info("Warmup blackhole {}", warmupPool());
                    shutdownPool(DS);
                } catch (InterruptedException e) {
                }
            }
        });

        quietlySleep(SECONDS.toMillis(10));

        this.requestCount = parseInt(args[2]);
    }

    private void start(int connectDelay) throws InterruptedException {
        BlockingDeque<RequestThread> list = new LinkedBlockingDeque<>();
        BlockingDeque<RequestThread> requests = new LinkedBlockingDeque<>();
        for (int i = 0; i < requestCount; i++) {
            RequestThread rt = new RequestThread();
            requests.put(rt);
        }
        // 增加任务
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            executorService.execute(() -> {
                // 如果不停止就一直添加任务
                while (!stoped) {
                    if (requests.size() < requestCount) {
                        RequestThread rt = new RequestThread();
                        try {
                            requests.put(rt);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
//                    System.out.println("add task...");
                }
            });
        }


        StubDriver.setConnectDelayMs(connectDelay);
        StubStatement.setExecuteDelayMs(5L);

        Timer timer = new Timer(true);
        ExecutorService executor = Executors.newFixedThreadPool(requestCount);

        quietlySleep(SECONDS.toMillis(3));

        threadsRemaining = new AtomicInteger(0);
        threadsPending = new AtomicInteger(0);

        LOGGER.info("SpikeLoadTest starting.");

        currentThread().setPriority(MAX_PRIORITY);

        timer.schedule(new TimerTask() {
            public void run() {
                // 只要有请求就不停止
                // FIXME 提前退出
                while (!requests.isEmpty()) {
                    final Runnable runner = requests.pollFirst();
                    if (Objects.nonNull(runner)) {
                        executor.execute(runner);
                        try {
                            list.put((RequestThread) runner);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                System.out.println("stopped...");
            }
        }, 1);

        final long startTime = nanoTime();

        List<PoolStatistics> statsList = new ArrayList<>();
        PoolStatistics poolStatistics;
        while (true) {
            poolStatistics = getPoolStatistics(startTime, threadsPending.get());
            statsList.add(poolStatistics);
            UtilityElf.quietlySleep(1L);
            // 运行 30s 才停止。
            if (nanoTime() - startTime > SECONDS.toNanos(30)) {
                stoped = true;
                System.out.println("set stop=true....");
                break;
            }
        }

        long endTime = nanoTime();

        executorService.shutdown();
        executorService.awaitTermination(30, SECONDS);
        executor.shutdown();
        executor.awaitTermination(30, SECONDS);

        LOGGER.info("SpikeLoadTest completed in {}ms", MILLISECONDS.convert(endTime - startTime, NANOSECONDS));

//        dumpStats(statsList, list);
        dumpStatsFile(statsList, list);
    }

    private void dumpStats(List<PoolStatistics> statsList, BlockingDeque<RequestThread> list) {
        System.out.println(String.format("%10s%8s%8s%8s%8s", "Time", "Total", "Active", "Idle", "Wait"));

        for (PoolStatistics stats : statsList) {
            System.out.println(stats);
        }

        System.out.println("\n" + String.format("%12s%12s%12s%20s", "Total", "Conn", "Query", "Thread"));
        for (RequestThread req : list) {
            System.out.println(req);
        }
        System.out.println("count: " + threadsRemaining.get());
    }

    private void dumpStatsFile(List<PoolStatistics> statsList, BlockingDeque<RequestThread> list) {
        List<String> statsResult = new ArrayList<>(statsList.size() + 1);
        statsResult.add(String.format("%10s%8s%8s%8s%8s", "Time", "Total", "Active", "Idle", "Wait"));
        for (PoolStatistics stats : statsList) {
            statsResult.add(stats.toString());
        }
        file(statsResult, "h-status");

        List<String> threads = new ArrayList<>(list.size() + 1);
        threads.add("\n" + String.format("%12s%12s%12s%20s", "Total", "Conn", "Query", "Thread"));
        for (RequestThread req : list) {
            threads.add(req.toString());
        }
        file(threads, "h-threads");
    }

    private void file(List<String> map, String name) {
        try {
            String join = Joiner.on("\n").join(map);
            Files.write(Paths.get("/Users/lijun695/jd.com/" + name + ".txt"), join.getBytes(StandardCharsets.UTF_8));
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    private class RequestThread extends TimerTask implements Runnable {
        @SuppressWarnings("unused")
        Exception exception;
        String name;
        long startTime;
        long endTime;
        long connectTime;
        long queryTime;

        @Override
        public void run() {
            name = currentThread().getName();

            threadsPending.incrementAndGet();
            startTime = nanoTime();
            try (Connection connection = DS.getConnection()) {
                connectTime = nanoTime();
                threadsPending.decrementAndGet();
                try (Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery(SQL)) {
                } finally {
                    queryTime = nanoTime();
                }
            } catch (SQLException e) {
                exception = e;
            } finally {
                endTime = nanoTime();
                threadsRemaining.decrementAndGet();
            }
        }

        @Override
        public String toString() {
            return String.format("%12d%12d%12d%20s",
                    NANOSECONDS.toMicros(endTime - startTime),
                    NANOSECONDS.toMicros(connectTime - startTime),
                    NANOSECONDS.toMicros(queryTime - connectTime),
                    name);
        }
    }

    private static class PoolStatistics {
        long timestamp = nanoTime();
        int activeConnections;
        int idleConnections;
        int pendingThreads;
        int totalConnections;

        PoolStatistics(final long baseTime) {
            timestamp = nanoTime() - baseTime;
        }

        @Override
        public String toString() {
            return String.format("%10d%8d%8d%8d%8d", NANOSECONDS.toMicros(timestamp), totalConnections, activeConnections, idleConnections, pendingThreads);
        }
    }

    private PoolStatistics getPoolStatistics(final long baseTime, int remaining) {
        PoolStatistics stats = new PoolStatistics(baseTime);

        switch (pool) {
            case "hikari":
                final int[] poolStateCounts = hikariPoolAccessor.getPoolStateCounts();
                stats.activeConnections = poolStateCounts[STATE_IN_USE];
                stats.idleConnections = poolStateCounts[STATE_NOT_IN_USE];
                stats.totalConnections = poolStateCounts[4];
                stats.pendingThreads = remaining;
                break;
        }

        return stats;
    }

    private long warmupPool() throws InterruptedException {
        final LongAdder j = new LongAdder();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int k = 0; k < 10; k++) {
            executor.execute(() -> {
//                for (int i = 0; i < 100_000; i++) {
                for (int i = 0; i < 20_000; i++) {

                    try (Connection connection = DS.getConnection();
                         Statement statement = connection.createStatement();
                         ResultSet resultSet = statement.executeQuery(SQL)) {
                        if (resultSet.next()) {
                            j.add(resultSet.getInt(1));
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(60, SECONDS);

        return j.sum();
    }

    private void setupHikari() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMinimumIdle(MIN_POOL_SIZE);
        config.setMaximumPoolSize(MAX_POOL_SIZE);
        config.setConnectionTimeout(2000);
        config.setAutoCommit(false);

        DS = new HikariDataSource(config);
    }

    private void shutdownPool(DataSource ds) {
        if (ds instanceof AutoCloseable) {
            try {
                ((AutoCloseable) ds).close();
            } catch (Exception e) {
            }
        }
    }

    private static HikariPool getHikariPool(DataSource ds) {
        try {
            Field field = ds.getClass().getDeclaredField("pool");
            field.setAccessible(true);
            return (HikariPool) field.get(ds);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
