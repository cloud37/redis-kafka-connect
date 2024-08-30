/*
 * Copyright © 2021 Redis
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redis.kafka.connect.sink;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import com.redis.kafka.connect.common.RedisConfig;
import org.apache.kafka.connect.sink.SinkRecord;

public class RedisSinkConfig extends RedisConfig {

    public enum RedisCommand {
        HSET, JSONSET, JSONMERGE, TSADD, SET, XADD, LPUSH, RPUSH, SADD, ZADD, DEL
    }

    public static final RedisSinkConfigDef CONFIG = new RedisSinkConfigDef();

    private final Charset charset;

    private final RedisCommand command;

    private final String keyspace;

    private final String separator;

    private final boolean multiExec;

    private final int waitReplicas;

    private final Duration waitTimeout;

    private final String jsonPath;

    private final String fixedJsonPath;

    private final boolean deleteJsonPath;

    public RedisSinkConfig(Map<?, ?> originals) {
        super(new RedisSinkConfigDef(), originals);
        String charsetName = getString(RedisSinkConfigDef.CHARSET_CONFIG).trim();
        charset = Charset.forName(charsetName);
        command = RedisCommand.valueOf(getString(RedisSinkConfigDef.COMMAND_CONFIG));
        keyspace = getString(RedisSinkConfigDef.KEY_CONFIG).trim();
        separator = getString(RedisSinkConfigDef.SEPARATOR_CONFIG).trim();
        multiExec = Boolean.TRUE.equals(getBoolean(RedisSinkConfigDef.MULTIEXEC_CONFIG));
        waitReplicas = getInt(RedisSinkConfigDef.WAIT_REPLICAS_CONFIG);
        waitTimeout = Duration.ofMillis(getLong(RedisSinkConfigDef.WAIT_TIMEOUT_CONFIG));

        if (command == RedisCommand.JSONMERGE) {
            jsonPath = getString(RedisSinkConfigDef.JSON_PATH_CONFIG).trim();
            fixedJsonPath = getString(RedisSinkConfigDef.FIXED_JSON_PATH_CONFIG).trim();
            deleteJsonPath = getBoolean(RedisSinkConfigDef.DELETE_JSON_PATH_CONFIG);
        } else {
            jsonPath = null;
            fixedJsonPath = null;
            deleteJsonPath = false;
        }
    }

    public Charset getCharset() {
        return charset;
    }

    public RedisCommand getCommand() {
        return command;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getSeparator() {
        return separator;
    }

    public boolean isMultiExec() {
        return multiExec;
    }

    public int getWaitReplicas() {
        return waitReplicas;
    }

    public Duration getWaitTimeout() {
        return waitTimeout;
    }

    public boolean getDeleteJsonPath() { return deleteJsonPath; }

    public String getFixedJsonPath() {
        return fixedJsonPath;
    }

    public String getJsonPath() { return jsonPath; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(charset, keyspace, separator, multiExec, command, waitReplicas, waitTimeout, deleteJsonPath, fixedJsonPath, jsonPath);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        RedisSinkConfig other = (RedisSinkConfig) obj;
        return Objects.equals(charset, other.charset) && Objects.equals(keyspace, other.keyspace)
                && Objects.equals(separator, other.separator) && multiExec == other.multiExec && command == other.command
                && waitReplicas == other.waitReplicas && waitTimeout == other.waitTimeout && deleteJsonPath == other.deleteJsonPath &&
                Objects.equals(fixedJsonPath, other.fixedJsonPath) && Objects.equals(jsonPath, other.jsonPath);
    }

}
