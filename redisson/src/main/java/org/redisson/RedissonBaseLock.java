/**
 * Copyright (c) 2013-2021 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.redisson.api.BatchOptions;
import org.redisson.api.BatchResult;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;
import org.redisson.client.protocol.decoder.MapValueDecoder;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.connection.MasterSlaveEntry;
import org.redisson.misc.CompletableFutureWrapper;
import org.redisson.misc.RedissonPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;

/**
 * Base class for implementing distributed locks
 *
 * @author Danila Varatyntsev
 * @author Nikita Koksharov
 */
public abstract class RedissonBaseLock extends RedissonExpirable implements RLock {

    public static class ExpirationEntry {
        /**
         * 对于互斥锁来说，这个map只会有一对数据，key为线程id，value为锁重入的次数 <p/>
         */
        private final Map<Long, Integer> threadIds = new LinkedHashMap<>();
        /**
         * 重置ttl的任务
         */
        private volatile Timeout timeout;

        public ExpirationEntry() {
            super();
        }

        public synchronized void addThreadId(long threadId) {
            Integer counter = threadIds.get(threadId);
            if (counter == null) {
                counter = 1;
            } else {
                counter++;
            }
            threadIds.put(threadId, counter);
        }
        public synchronized boolean hasNoThreads() {
            return threadIds.isEmpty();
        }
        public synchronized Long getFirstThreadId() {
            if (threadIds.isEmpty()) {
                return null;
            }
            return threadIds.keySet().iterator().next();
        }
        public synchronized void removeThreadId(long threadId) {
            Integer counter = threadIds.get(threadId);
            if (counter == null) {
                return;
            }
            counter--;
            if (counter == 0) {
                threadIds.remove(threadId);
            } else {
                threadIds.put(threadId, counter);
            }
        }


        public void setTimeout(Timeout timeout) {
            this.timeout = timeout;
        }
        public Timeout getTimeout() {
            return timeout;
        }

    }

    private static final Logger log = LoggerFactory.getLogger(RedissonBaseLock.class);

    /**
     * 锁占用后的重置ttl对象的缓存 <p/>
     * key为锁名
     */
    private static final ConcurrentMap<String, ExpirationEntry> EXPIRATION_RENEWAL_MAP = new ConcurrentHashMap<>();
    /**
     * 默认30秒
     */
    protected long internalLockLeaseTime;

    final String id;
    /**
     * 格式：id:lock_name<p/>
     * 这个entryName可唯一确定是哪个客户端的哪个线程的锁
     */
    final String entryName;

    final CommandAsyncExecutor commandExecutor;

    public RedissonBaseLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.id = commandExecutor.getConnectionManager().getId();
        this.internalLockLeaseTime = commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout();
        this.entryName = id + ":" + name;
    }

    protected String getEntryName() {
        return entryName;
    }

    protected String getLockName(long threadId) {
        return id + ":" + threadId;
    }

    private void renewExpiration() {
        ExpirationEntry ee = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (ee == null) {
            return;
        }
        
        Timeout task = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                ExpirationEntry ent = EXPIRATION_RENEWAL_MAP.get(getEntryName());
                if (ent == null) {
                    return;
                }
                Long threadId = ent.getFirstThreadId();
                if (threadId == null) {
                    return;
                }
                
                RFuture<Boolean> future = renewExpirationAsync(threadId);
                future.whenComplete((res, e) -> {
                    if (e != null) {
                        log.error("Can't update lock " + getRawName() + " expiration", e);
                        EXPIRATION_RENEWAL_MAP.remove(getEntryName());
                        return;
                    }
                    
                    if (res) { // true代表续约成功，继续定时
                        // reschedule itself
                        renewExpiration();
                    } else { // 续约失败（redis中key丢失），取消续约的定时任务
                        // 传null是bug修复后的结果。避免在锁丢失的情况下，如果获取到互斥锁的线程有重入，则不一定会取消定时任务。而当其他线程设置了锁后，这个定时任务还会发挥作用，造成bug
                        cancelExpirationRenewal(null);
                    }
                });
            }
        }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS);
        
        ee.setTimeout(task);
    }
    
    protected void scheduleExpirationRenewal(long threadId) {
        ExpirationEntry entry = new ExpirationEntry();
        ExpirationEntry oldEntry = EXPIRATION_RENEWAL_MAP.putIfAbsent(getEntryName(), entry);
        if (oldEntry != null) { // 锁重入
            oldEntry.addThreadId(threadId);
        } else { // 第一次加锁
            entry.addThreadId(threadId);
            try {
                renewExpiration();
            } finally {
                if (Thread.currentThread().isInterrupted()) {
                    cancelExpirationRenewal(threadId);
                }
            }
        }
    }

    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return 1; " +
                        "end; " +
                        "return 0;",
                Collections.singletonList(getRawName()),
                internalLockLeaseTime, getLockName(threadId));
    }

    /**
     * @param threadId 传null，则取消这个锁关联的所有定时续约任务。传入线程id，则对当前线程重入次数减1，直到为0才取消这个线程关联的续约任务
     */
    protected void cancelExpirationRenewal(Long threadId) {
        ExpirationEntry task = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (task == null) {
            return;
        }
        
        if (threadId != null) {
            task.removeThreadId(threadId);
        }

        if (threadId == null || task.hasNoThreads()) {
            Timeout timeout = task.getTimeout();
            if (timeout != null) {
                timeout.cancel();
            }
            EXPIRATION_RENEWAL_MAP.remove(getEntryName());
        }
    }

    protected <T> RFuture<T> evalWriteAsync(String key, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object... params) {
        MasterSlaveEntry entry = commandExecutor.getConnectionManager().getEntry(getRawName());
        // 执行当前命令的master节点的从节点数量
        int availableSlaves = entry.getAvailableSlaves();

        // 创建批量命令执行器（主要是为了添加WAIT命令，等待数据同步到从节点。但使用了批量命令执行器，就用不了script cache了）
        CommandBatchService executorService = createCommandBatchService(availableSlaves);
        // 这里并不会直接执行命令，而是将命令保存起来，等待后续添加其他命令（比如WAIT）后再一起执行
        RFuture<T> result = executorService.evalWriteAsync(key, codec, evalCommandType, script, keys, params);
        if (commandExecutor instanceof CommandBatchService) {
            return result;
        }
        // 真正的执行：根据配置再适当添加命令，然后一起执行
        RFuture<BatchResult<?>> future = executorService.executeAsync();

        // 设置一个等待这个future完成（正常结束或异常结束）的listener，用来判断redis实际同步的从节点数量是否是我们指定的从节点数量，并抛出异常
        // fixme 感觉这种实现方式不好，所以最新的版本已经让用户来选择是否抛异常了
        CompletionStage<T> f = future.handle((res, ex) -> {
            if (ex == null && res.getSyncedSlaves() != availableSlaves) {
                throw new CompletionException(new IllegalStateException("Only "
                                                        + res.getSyncedSlaves() + " of " + availableSlaves + " slaves were synced"));
            }

            return result.getNow();
        });
        return new CompletableFutureWrapper<>(f);
    }

    private CommandBatchService createCommandBatchService(int availableSlaves) {
        if (commandExecutor instanceof CommandBatchService) {
            return (CommandBatchService) commandExecutor;
        }

        BatchOptions options = BatchOptions.defaults()
                                            .syncSlaves(availableSlaves, 1, TimeUnit.SECONDS);

        return new CommandBatchService(commandExecutor, options);
    }

    protected void acquireFailed(long waitTime, TimeUnit unit, long threadId) {
        get(acquireFailedAsync(waitTime, unit, threadId));
    }
    
    protected RFuture<Void> acquireFailedAsync(long waitTime, TimeUnit unit, long threadId) {
        return RedissonPromise.newSucceededFuture(null);
    }

    @Override
    public Condition newCondition() {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        return isExists();
    }
    
    @Override
    public RFuture<Boolean> isLockedAsync() {
        return isExistsAsync();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return isHeldByThread(Thread.currentThread().getId());
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        RFuture<Boolean> future = commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getRawName(), getLockName(threadId));
        return get(future);
    }

    private static final RedisCommand<Integer> HGET = new RedisCommand<Integer>("HGET", new MapValueDecoder(), new IntegerReplayConvertor(0));
    
    public RFuture<Integer> getHoldCountAsync() {
        return commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, HGET, getRawName(), getLockName(Thread.currentThread().getId()));
    }
    
    @Override
    public int getHoldCount() {
        return get(getHoldCountAsync());
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return forceUnlockAsync();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        long threadId = Thread.currentThread().getId();
        return unlockAsync(threadId);
    }

    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        RFuture<Boolean> future = unlockInnerAsync(threadId);

        CompletionStage<Void> f = future.handle((opStatus, e) -> {
            // 无论是否解锁成功，都取消锁续约的定时任务
            // 可能出现redis服务器重启导致锁消失，虽然当前线程获取了锁，但锁已不存在，解锁就会失败
            cancelExpirationRenewal(threadId);

            if (e != null) {
                throw new CompletionException(e);
            }
            if (opStatus == null) {
                IllegalMonitorStateException cause = new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                        + id + " thread-id: " + threadId);
                throw new CompletionException(cause);
            }

            return null;
        });

        return new CompletableFutureWrapper<>(f);
    }

    protected abstract RFuture<Boolean> unlockInnerAsync(long threadId);
}
