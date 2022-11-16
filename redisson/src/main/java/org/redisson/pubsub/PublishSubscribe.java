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
package org.redisson.pubsub;

import io.netty.util.Timeout;
import org.redisson.PubSubEntry;
import org.redisson.client.BaseRedisPubSubListener;
import org.redisson.client.ChannelName;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.RedisTimeoutException;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.pubsub.PubSubType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author Nikita Koksharov
 *
 */
abstract class PublishSubscribe<E extends PubSubEntry<E>> {

    private final ConcurrentMap<String, E> entries = new ConcurrentHashMap<>();
    private final PublishSubscribeService service;

    PublishSubscribe(PublishSubscribeService service) {
        super();
        this.service = service;
    }

    public void unsubscribe(E entry, String entryName, String channelName) {
        AsyncSemaphore semaphore = service.getSemaphore(new ChannelName(channelName));
        semaphore.acquire(() -> {
            // 减少锁持有的引用计数，如果变为0，代表这个客户端没有线程在阻塞获取这把锁了，就取消订阅
            if (entry.release() == 0) {
                entries.remove(entryName);
                service.unsubscribe(PubSubType.UNSUBSCRIBE, new ChannelName(channelName))
                        .whenComplete((r, e) -> {
                            semaphore.release();
                        });
            } else {
                semaphore.release();
            }
        });

    }

    public CompletableFuture<E> subscribe(String entryName, String channelName) {
        AsyncSemaphore semaphore = service.getSemaphore(new ChannelName(channelName));
        CompletableFuture<E> newPromise = new CompletableFuture<>();

        // 设置一个超时检测的定时任务
        int timeout = service.getConnectionManager().getConfig().getTimeout();
        Timeout lockTimeout = service.getConnectionManager().newTimeout(t -> {
            newPromise.completeExceptionally(new RedisTimeoutException(
                    "Unable to acquire subscription lock after " + timeout + "ms. " +
                            "Increase 'subscriptionsPerConnection' and/or 'subscriptionConnectionPoolSize' parameters."));
        }, timeout, TimeUnit.MILLISECONDS);

        semaphore.acquire(() -> {
            if (!lockTimeout.cancel()) {
                semaphore.release();
                return;
            }

            E entry = entries.get(entryName);
            if (entry != null) { // 已经订阅过，表示当前客户端有其他线程也在获取锁 or 锁重入
                entry.acquire();
                semaphore.release();
                entry.getPromise().whenComplete((r, e) -> {
                    if (e != null) {
                        newPromise.completeExceptionally(e);
                        return;
                    }
                    newPromise.complete(r);
                });
                return;
            }
            // 到这就代表当前客户端是第一次订阅
            E value = createEntry(newPromise);
            value.acquire(); // 引用计数 + 1

            E oldValue = entries.putIfAbsent(entryName, value);
            if (oldValue != null) {
                oldValue.acquire();
                semaphore.release();
                oldValue.getPromise().whenComplete((r, e) -> {
                    if (e != null) {
                        newPromise.completeExceptionally(e);
                        return;
                    }
                    newPromise.complete(r);
                });
                return;
            }
            // 创建这个channel的监听器（当服务端publish时，会通知到这个Listener）
            RedisPubSubListener<Object> listener = createListener(channelName, value);
            // 开始订阅channel
            CompletableFuture<PubSubConnectionEntry> s = service.subscribe(LongCodec.INSTANCE, channelName, semaphore, listener);
            s.whenComplete((r, e) -> {
                if (e != null) {
                    value.getPromise().completeExceptionally(e);
                    return;
                }
                value.getPromise().complete(value);
            });

        });

        return newPromise;
    }

    protected abstract E createEntry(CompletableFuture<E> newPromise);

    protected abstract void onMessage(E value, Long message);

    private RedisPubSubListener<Object> createListener(String channelName, E value) {
        RedisPubSubListener<Object> listener = new BaseRedisPubSubListener() {

            @Override
            public void onMessage(CharSequence channel, Object message) {
                if (!channelName.equals(channel.toString())) {
                    return;
                }

                PublishSubscribe.this.onMessage(value, (Long) message);
            }
        };
        return listener;
    }

}
