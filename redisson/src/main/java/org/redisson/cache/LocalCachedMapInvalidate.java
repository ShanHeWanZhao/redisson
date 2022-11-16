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

import java.io.Serializable;

/**
 * LocalCacheMap的删除key的消息
 * @author Nikita Koksharov
 *
 */
@SuppressWarnings("serial")
public class LocalCachedMapInvalidate implements Serializable {

    /**
     * 需要排除的LocalCacheMap实例id（发起这个消息的实例已经处理过，所以就可以排除掉）
     */
    private byte[] excludedId;
    /**
     * map中key的hash，以此定位到具体某个key
     */
    private byte[][] keyHashes;

    public LocalCachedMapInvalidate() {
    }
    
    public LocalCachedMapInvalidate(byte[] excludedId, byte[]... keyHashes) {
        super();
        this.keyHashes = keyHashes;
        this.excludedId = excludedId;
    }
    
    public byte[] getExcludedId() {
        return excludedId;
    }
    
    public byte[][] getKeyHashes() {
        return keyHashes;
    }
    
}
