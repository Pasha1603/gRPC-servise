package com.example.kv;

import com.example.kv.KVServiceGrpc.KVServiceImplBase;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.List;

public class KVServiceImpl extends KVServiceImplBase {

    private final TarantoolKVClient tarantoolClient;

    public KVServiceImpl(TarantoolKVClient tarantoolClient) {
        this.tarantoolClient = tarantoolClient;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> observer) {
        try {
            byte[] value = request.hasValue() ? request.getValue().toByteArray() : null;
            tarantoolClient.put(request.getKey(), value);
            observer.onNext(PutResponse.newBuilder().setSuccess(true).build());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> observer) {
        try {
            byte[] value = tarantoolClient.get(request.getKey());
            GetResponse.Builder builder = GetResponse.newBuilder();
            if (value != null) {
                builder.setValue(ByteString.copyFrom(value));
            }
            observer.onNext(builder.build());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> observer) {
        try {
            boolean deleted = tarantoolClient.delete(request.getKey());
            observer.onNext(DeleteResponse.newBuilder().setSuccess(deleted).build());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> observer) {
        try {
            List<TarantoolKVClient.KeyValue> items = tarantoolClient.range(
                    request.getKeySince(),
                    request.getKeyTo()
            );
            for (TarantoolKVClient.KeyValue item : items) {
                observer.onNext(RangeResponse.newBuilder()
                        .setKey(item.key)
                        .setValue(ByteString.copyFrom(item.value))
                        .build());
            }
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void count(EmptyRequest request, StreamObserver<CountResponse> observer) {
        try {
            long count = tarantoolClient.count();
            observer.onNext(CountResponse.newBuilder().setCount(count).build());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }
}