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
import org.redisson.api.RFuture;
import org.redisson.client.RedisException;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.misc.CompletableFutureWrapper;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.redisson.pubsub.LockPubSub;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>non-fair</b> locking so doesn't guarantees an acquire order.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonLock extends RedissonBaseLock {

    /**
     * 默认30秒
     */
    protected long internalLockLeaseTime;

    protected final LockPubSub pubSub;

    final CommandAsyncExecutor commandExecutor;

    public RedissonLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.internalLockLeaseTime = commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout();
        this.pubSub = commandExecutor.getConnectionManager().getSubscribeService().getLockPubSub();
    }

    String getChannelName() {
        return prefixName("redisson_lock__channel", getRawName());
    }

    @Override
    public void lock() {
        try {
            lock(-1, null, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            lock(leaseTime, unit, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock(-1, null, true);
    }

    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        lock(leaseTime, unit, true);
    }

    private void lock(long leaseTime, TimeUnit unit, boolean interruptibly) throws InterruptedException {
        long threadId = Thread.currentThread().getId();
        // 先尝试获取锁，获取到就可直接返回
        Long ttl = tryAcquire(-1, leaseTime, unit, threadId);
        // lock acquired
        if (ttl == null) { // 加锁成功
            return;
        }

        // 异步订阅频道：redisson_lock__channel:{锁名}
        CompletableFuture<RedissonLockEntry> future = subscribe(threadId);
        // 同步等待订阅成功
        if (interruptibly) {
            commandExecutor.syncSubscriptionInterrupted(future);
        } else {
            commandExecutor.syncSubscription(future);
        }

        try {
            while (true) {  // 走到这，就代表订阅成功。死循环获取锁，直到获取成功或抛异常才跳出循环
                // 再次尝试获取锁
                ttl = tryAcquire(-1, leaseTime, unit, threadId);
                // lock acquired
                if (ttl == null) { // 获取锁成功，直接返回
                    break;
                }

                // waiting for message
                if (ttl >= 0) { // redis的key存在ttl，定时尝试 + publish的通知 两种方法
                    try {
                        // 利用jdk的Semaphore，定时尝试获取锁，时间为ttl。
                        // 因为获取到锁的客户端可能宕机导致不能publish解锁的消息，所以这里也需要定时ttl的时间来尝试加锁来解决这种情况
                        /*
                            这个Semaphore初始值就是0，是会阻塞的。
                            只有等待获取到锁的线程解锁时，向这个订阅的频道发送一条解锁消息，Semaphore的有效值才加一
                            这时，当前客户端阻塞在这等待获取锁的线程会被立即唤醒一个，继续尝试获取锁 */
                        commandExecutor.getNow(future).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        if (interruptibly) {
                            throw e;
                        }
                        commandExecutor.getNow(future).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    }
                } else { // redis的key不存在过期时间，只有等待 publish的通知 这一种方法
                    if (interruptibly) {
                        commandExecutor.getNow(future).getLatch().acquire();
                    } else {
                        commandExecutor.getNow(future).getLatch().acquireUninterruptibly();
                    }
                }
            }
        } finally { // 根据当前客户端是否还有其它线程在阻塞获取锁来决定是否需要取消订阅（引用计数思想）
            unsubscribe(commandExecutor.getNow(future), threadId);
        }
//        get(lockAsync(leaseTime, unit));
    }
    
    private Long tryAcquire(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        return get(tryAcquireAsync(waitTime, leaseTime, unit, threadId));
    }
    
    private RFuture<Boolean> tryAcquireOnceAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        RFuture<Boolean> acquiredFuture;
        if (leaseTime != -1) {
            acquiredFuture = tryLockInnerAsync(waitTime, leaseTime, unit, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        } else {
            acquiredFuture = tryLockInnerAsync(waitTime, internalLockLeaseTime,
                    TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        }

        CompletionStage<Boolean> f = acquiredFuture.thenApply(acquired -> {
            // lock acquired
            if (acquired) { // 加锁成功
                if (leaseTime != -1) {
                    internalLockLeaseTime = unit.toMillis(leaseTime);
                } else { // 设置定时任务续约ttl
                    scheduleExpirationRenewal(threadId);
                }
            }
            return acquired;
        });
        return new CompletableFutureWrapper<>(f);
    }

    private <T> RFuture<Long> tryAcquireAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        RFuture<Long> ttlRemainingFuture;
        if (leaseTime != -1) {
            ttlRemainingFuture = tryLockInnerAsync(waitTime, leaseTime, unit, threadId, RedisCommands.EVAL_LONG);
        } else {
            ttlRemainingFuture = tryLockInnerAsync(waitTime, internalLockLeaseTime,
                    TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_LONG);
        }
        CompletionStage<Long> f = ttlRemainingFuture.thenApply(ttlRemaining -> {
            // lock acquired
            if (ttlRemaining == null) { // key的ttl返回为空，代表加锁成功
                if (leaseTime != -1) {
                    internalLockLeaseTime = unit.toMillis(leaseTime);
                } else {
                    // 需要设置定时任务，在锁占用期间无限制的重置ttl，以免任务还未执行完锁就过期了，达不到互斥的效果
                    scheduleExpirationRenewal(threadId);
                }
            }
            return ttlRemaining;
        });
        return new CompletableFutureWrapper<>(f);
    }

    @Override
    public boolean tryLock() {
        return get(tryLockAsync());
    }

    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                "if (redis.call('exists', KEYS[1]) == 0) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return nil; " +
                        "end; " +
                        "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return nil; " +
                        "end; " +
                        "return redis.call('pttl', KEYS[1]);",
                Collections.singletonList(getRawName()), unit.toMillis(leaseTime), getLockName(threadId));
    }

    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
        long time = unit.toMillis(waitTime);
        long current = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        // 先尝试获取或，获取成功就直接返回
        Long ttl = tryAcquire(waitTime, leaseTime, unit, threadId);
        // lock acquired
        if (ttl == null) {
            return true;
        }
        // 第一次获取锁失败，判断剩余等待时间
        time -= System.currentTimeMillis() - current;
        if (time <= 0) { // 没有可用的等待时间了，直接返回失败
            acquireFailed(waitTime, unit, threadId);
            return false;
        }
        
        current = System.currentTimeMillis();
        CompletableFuture<RedissonLockEntry> subscribeFuture = subscribe(threadId);
        // 定时阻塞等待订阅成功（这个时间就为剩余尝试时间），如果这期间连频道都没订阅成功，那也可以构造失败并返回了
        try {
            subscribeFuture.toCompletableFuture().get(time, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            if (!subscribeFuture.cancel(false)) {
                subscribeFuture.whenComplete((res, ex) -> {
                    if (ex == null) {
                        unsubscribe(res, threadId);
                    }
                });
            }
            acquireFailed(waitTime, unit, threadId);
            return false;
        }

        try {
            // 继续计算剩余尝试时间
            time -= System.currentTimeMillis() - current;
            if (time <= 0) {
                acquireFailed(waitTime, unit, threadId);
                return false;
            }
        
            while (true) {// 到者就代表既没获取锁成功，剩余尝试时间还没用完
                // 在死循环初始流程尝试
                long currentTime = System.currentTimeMillis();
                ttl = tryAcquire(waitTime, leaseTime, unit, threadId);
                // lock acquired
                if (ttl == null) {
                    return true;
                }

                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {  // 没获取到锁，并且剩余尝试时间用完，返回失败
                    acquireFailed(waitTime, unit, threadId);
                    return false;
                }

                // waiting for message
                // 最终还是利用Semaphore的超时等待获取锁来实现的
                currentTime = System.currentTimeMillis();
                if (ttl >= 0 && ttl < time) {
                    commandExecutor.getNow(subscribeFuture).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    commandExecutor.getNow(subscribeFuture).getLatch().tryAcquire(time, TimeUnit.MILLISECONDS);
                }

                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    acquireFailed(waitTime, unit, threadId);
                    return false;
                }
            }
        } finally {
            unsubscribe(commandExecutor.getNow(subscribeFuture), threadId);
        }
//        return get(tryLockAsync(waitTime, leaseTime, unit));
    }

    protected CompletableFuture<RedissonLockEntry> subscribe(long threadId) {
        return pubSub.subscribe(getEntryName(), getChannelName());
    }

    protected void unsubscribe(RedissonLockEntry entry, long threadId) {
        pubSub.unsubscribe(entry, getEntryName(), getChannelName());
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }

    @Override
    public void unlock() {
        try {
            get(unlockAsync(Thread.currentThread().getId()));
        } catch (RedisException e) {
            if (e.getCause() instanceof IllegalMonitorStateException) {
                throw (IllegalMonitorStateException) e.getCause();
            } else {
                throw e;
            }
        }
        
//        Future<Void> future = unlockAsync();
//        future.awaitUninterruptibly();
//        if (future.isSuccess()) {
//            return;
//        }
//        if (future.cause() instanceof IllegalMonitorStateException) {
//            throw (IllegalMonitorStateException)future.cause();
//        }
//        throw commandExecutor.convertException(future);
    }

    @Override
    public boolean forceUnlock() {
        return get(forceUnlockAsync());
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('del', KEYS[1]) == 1) then "
                        + "redis.call('publish', KEYS[2], ARGV[1]); "
                        + "return 1 "
                        + "else "
                        + "return 0 "
                        + "end",
                Arrays.asList(getRawName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE);
    }

    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                        "return nil;" +
                        "end; " +
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        "if (counter > 0) then " +
                        "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                        "return 0; " +
                        "else " +
                        "redis.call('del', KEYS[1]); " +
                        "redis.call('publish', KEYS[2], ARGV[1]); " +
                        "return 1; " +
                        "end; " +
                        "return nil;",
                Arrays.asList(getRawName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId));
    }

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(-1, null);
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return lockAsync(leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Void> lockAsync(long currentThreadId) {
        return lockAsync(-1, null, currentThreadId);
    }
    
    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit, long currentThreadId) {
        RPromise<Void> result = new RedissonPromise<Void>();
        RFuture<Long> ttlFuture = tryAcquireAsync(-1, leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                if (!result.trySuccess(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            CompletableFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            subscribeFuture.whenComplete((res, ex) -> {
                if (ex != null) {
                    result.tryFailure(ex);
                    return;
                }

                lockAsync(leaseTime, unit, res, result, currentThreadId);
            });
        });

        return result;
    }

    private void lockAsync(long leaseTime, TimeUnit unit,
                           RedissonLockEntry entry, RPromise<Void> result, long currentThreadId) {
        RFuture<Long> ttlFuture = tryAcquireAsync(-1, leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            if (e != null) {
                unsubscribe(entry, currentThreadId);
                result.tryFailure(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                unsubscribe(entry, currentThreadId);
                if (!result.trySuccess(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            if (entry.getLatch().tryAcquire()) {
                lockAsync(leaseTime, unit, entry, result, currentThreadId);
            } else {
                // waiting for message
                AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
                Runnable listener = () -> {
                    if (futureRef.get() != null) {
                        futureRef.get().cancel();
                    }
                    lockAsync(leaseTime, unit, entry, result, currentThreadId);
                };

                entry.addListener(listener);

                if (ttl >= 0) {
                    Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                        @Override
                        public void run(Timeout timeout) throws Exception {
                            if (entry.removeListener(listener)) {
                                lockAsync(leaseTime, unit, entry, result, currentThreadId);
                            }
                        }
                    }, ttl, TimeUnit.MILLISECONDS);
                    futureRef.set(scheduledFuture);
                }
            }
        });
    }

    @Override
    public RFuture<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long threadId) {
        return tryAcquireOnceAsync(-1, -1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return tryLockAsync(waitTime, leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit,
            long currentThreadId) {
        RPromise<Boolean> result = new RedissonPromise<Boolean>();

        AtomicLong time = new AtomicLong(unit.toMillis(waitTime));
        long currentTime = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync(waitTime, leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                if (!result.trySuccess(true)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            long el = System.currentTimeMillis() - currentTime;
            time.addAndGet(-el);
            
            if (time.get() <= 0) {
                trySuccessFalse(currentThreadId, result);
                return;
            }
            
            long current = System.currentTimeMillis();
            AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
            CompletableFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            subscribeFuture.whenComplete((r, ex) -> {
                if (ex != null) {
                    result.tryFailure(ex);
                    return;
                }

                if (futureRef.get() != null) {
                    futureRef.get().cancel();
                }

                long elapsed = System.currentTimeMillis() - current;
                time.addAndGet(-elapsed);
                
                tryLockAsync(time, waitTime, leaseTime, unit, r, result, currentThreadId);
            });
            if (!subscribeFuture.isDone()) {
                Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws Exception {
                        if (!subscribeFuture.isDone()) {
                            subscribeFuture.cancel(false);
                            trySuccessFalse(currentThreadId, result);
                        }
                    }
                }, time.get(), TimeUnit.MILLISECONDS);
                futureRef.set(scheduledFuture);
            }
        });


        return result;
    }

    private void trySuccessFalse(long currentThreadId, RPromise<Boolean> result) {
        acquireFailedAsync(-1, null, currentThreadId).onComplete((res, e) -> {
            if (e == null) {
                result.trySuccess(false);
            } else {
                result.tryFailure(e);
            }
        });
    }

    private void tryLockAsync(AtomicLong time, long waitTime, long leaseTime, TimeUnit unit,
                              RedissonLockEntry entry, RPromise<Boolean> result, long currentThreadId) {
        if (result.isDone()) {
            unsubscribe(entry, currentThreadId);
            return;
        }
        
        if (time.get() <= 0) {
            unsubscribe(entry, currentThreadId);
            trySuccessFalse(currentThreadId, result);
            return;
        }
        
        long curr = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync(waitTime, leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
                if (e != null) {
                    unsubscribe(entry, currentThreadId);
                    result.tryFailure(e);
                    return;
                }

                // lock acquired
                if (ttl == null) {
                    unsubscribe(entry, currentThreadId);
                    if (!result.trySuccess(true)) {
                        unlockAsync(currentThreadId);
                    }
                    return;
                }
                
                long el = System.currentTimeMillis() - curr;
                time.addAndGet(-el);
                
                if (time.get() <= 0) {
                    unsubscribe(entry, currentThreadId);
                    trySuccessFalse(currentThreadId, result);
                    return;
                }

                // waiting for message
                long current = System.currentTimeMillis();
                if (entry.getLatch().tryAcquire()) {
                    tryLockAsync(time, waitTime, leaseTime, unit, entry, result, currentThreadId);
                } else {
                    AtomicBoolean executed = new AtomicBoolean();
                    AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
                    Runnable listener = () -> {
                        executed.set(true);
                        if (futureRef.get() != null) {
                            futureRef.get().cancel();
                        }

                        long elapsed = System.currentTimeMillis() - current;
                        time.addAndGet(-elapsed);
                        
                        tryLockAsync(time, waitTime, leaseTime, unit, entry, result, currentThreadId);
                    };
                    entry.addListener(listener);

                    long t = time.get();
                    if (ttl >= 0 && ttl < time.get()) {
                        t = ttl;
                    }
                    if (!executed.get()) {
                        Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                            @Override
                            public void run(Timeout timeout) throws Exception {
                                if (entry.removeListener(listener)) {
                                    long elapsed = System.currentTimeMillis() - current;
                                    time.addAndGet(-elapsed);
                                    
                                    tryLockAsync(time, waitTime, leaseTime, unit, entry, result, currentThreadId);
                                }
                            }
                        }, t, TimeUnit.MILLISECONDS);
                        futureRef.set(scheduledFuture);
                    }
                }
        });
    }


}
