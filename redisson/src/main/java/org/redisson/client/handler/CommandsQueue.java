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
package org.redisson.client.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;
import org.redisson.client.WriteRedisConnectionException;
import org.redisson.client.protocol.QueueCommand;
import org.redisson.client.protocol.QueueCommandHolder;
import org.redisson.misc.LogHelper;

import java.net.SocketAddress;
import java.util.Deque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 *
 * @author Nikita Koksharov
 *
 */
public class CommandsQueue extends ChannelDuplexHandler {

    /**
     * 虽然是个队列，但里面只会有一个元素。因为线程会独占一个RedisConnection，直到接收到数据才会归还这个RedisConnection。<br/>
     * 所以，异步callback时从这个队列中取出来的QueueCommand是对称的，可以以此来complete一个Promise
     */
    public static final AttributeKey<Deque<QueueCommandHolder>> COMMANDS_QUEUE = AttributeKey.valueOf("COMMANDS_QUEUE");

    /**
     * 当Channel 触发connect事件时，创建一个ConcurrentLinkedDeque用于保存QueueCommand，用于请求的回调处理
     */
    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        super.connect(ctx, remoteAddress, localAddress, promise);

        ctx.channel().attr(COMMANDS_QUEUE).set(new ConcurrentLinkedDeque<>());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Queue<QueueCommandHolder> queue = ctx.channel().attr(COMMANDS_QUEUE).get();
        Iterator<QueueCommandHolder> iterator = queue.iterator();
        while (iterator.hasNext()) {
            QueueCommandHolder command = iterator.next();
            if (command.getCommand().isBlockingCommand()) {
                continue;
            }

            iterator.remove();
            command.getChannelPromise().tryFailure(
                    new WriteRedisConnectionException("Channel has been closed! Can't write command: "
                                + LogHelper.toString(command.getCommand()) + " to channel: " + ctx.channel()));
        }

        super.channelInactive(ctx);
    }

    private final AtomicBoolean lock = new AtomicBoolean();

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof QueueCommand) {
            QueueCommand data = (QueueCommand) msg;
            QueueCommandHolder holder = new QueueCommandHolder(data, promise);

            Queue<QueueCommandHolder> queue = ctx.channel().attr(COMMANDS_QUEUE).get();

            while (true) {
                if (lock.compareAndSet(false, true)) {
                    try {
                        queue.add(holder);
                        ctx.writeAndFlush(data, holder.getChannelPromise());
                    } finally {
                        lock.set(false);
                    }
                    break;
                }
            }
        } else {
            super.write(ctx, msg, promise);
        }
    }

}
