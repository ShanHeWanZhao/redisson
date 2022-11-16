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
package org.redisson.cache;

import java.lang.ref.ReferenceQueue;

import org.redisson.cache.ReferenceCachedValue.Type;

/**
 * 
 * @author Nikita Koksharov
 *
 * @param <K> key
 * @param <V> value
 */
public class ReferenceCacheMap<K, V> extends AbstractCacheMap<K, V> {

    private final ReferenceQueue<V> queue = new ReferenceQueue<V>();
    
    private final ReferenceCachedValue.Type type;
    
    public static <K, V> ReferenceCacheMap<K, V> weak(long timeToLiveInMillis, long maxIdleInMillis) {
        return new ReferenceCacheMap<K, V>(timeToLiveInMillis, maxIdleInMillis, Type.WEAK);
    }
    
    public static <K, V> ReferenceCacheMap<K, V> soft(long timeToLiveInMillis, long maxIdleInMillis) {
        return new ReferenceCacheMap<K, V>(timeToLiveInMillis, maxIdleInMillis, Type.SOFT);
    }
    
    ReferenceCacheMap(long timeToLiveInMillis, long maxIdleInMillis, ReferenceCachedValue.Type type) {
        super(0, timeToLiveInMillis, maxIdleInMillis);
        this.type = type;
    }

    @Override
    protected CachedValue<K, V> create(K key, V value, long ttl, long maxIdleTime) {
        return new ReferenceCachedValue<K, V>(key, value, ttl, maxIdleTime, queue, type);
    }

    /**
     * 因为不确定软引用什么时候被回收，所以，这里每次添加数据时都是要检验是否被回收了，始终返回true
     * @param key
     * @return
     */
    @Override
    protected boolean isFull(K key) {
        return true;
    }

    @Override
    protected boolean removeExpiredEntries() {
        while (true) {
            // 弹出的引用不为空就代表这个引用保存的对象已经被回收了，这时需要清理map里缓存的对象
            CachedValueReference value = (CachedValueReference) queue.poll();
            if (value == null) {
                break;
            }
            map.remove(value.getOwner().getKey(), value.getOwner());
        }
        return super.removeExpiredEntries();
    }
    
    @Override
    protected void onMapFull() {
    }
    
}
