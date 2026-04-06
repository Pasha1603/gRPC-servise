package com.example.kv;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import java.util.ArrayList;
import java.util.List;

public class TarantoolKVClient {
    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;
    private final String spaceName = "KV";

    public TarantoolKVClient(String host, int port, String username, String password) {
        var mapperFactory = DefaultMessagePackMapperFactory.getInstance();
        this.client = TarantoolClientFactory.createClient()
                .withAddress(host, port)
                .withCredentials(username, password)
                .withMessagePackMapper(mapperFactory.defaultSimpleTypeMapper())
                .build();
    }

    public void put(String key, byte[] value) {
        var tuple = client.getTupleFactory().create(key, value);
        client.space(spaceName).replace(tuple).join();
    }

    public byte[] get(String key) {
        var result = client.space(spaceName).select(key).join();
        if (result.isEmpty()) return null;
        return (byte[]) result.get(0).getObject(1);
    }

    public boolean delete(String key) {
        var result = client.space(spaceName).delete(key).join();
        return !result.isEmpty();
    }

    public List<KeyValue> range(String fromKey, String toKey) {
        var result = client.space(spaceName).select(fromKey, toKey).join();
        List<KeyValue> list = new ArrayList<>();
        for (var tuple : result) {
            list.add(new KeyValue(tuple.getString(0), (byte[]) tuple.getObject(1)));
        }
        return list;
    }

    public long count() {
        return client.space(spaceName).len().join();
    }

    public static class KeyValue {
        public final String key;
        public final byte[] value;
        public KeyValue(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    public void close() {
        client.close();
    }
}