package ru.mail.polis.service.pranova;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

class FutureUtils {
    private static final Logger log = LoggerFactory.getLogger(FutureUtils.class);

    static CompletableFuture<Value> merge(@NotNull final CompletableFuture<List<Value>> future) {
        final CompletableFuture<Value> result = new CompletableFuture<>();
        future.whenCompleteAsync((v, e) -> {
            if (e != null) {
                result.complete(Value.errorValue(Replica.NOT_ENOUGH_REPLICAS));
                return;
            }
            final Comparator<Map.Entry<Value, Integer>> comparator = Comparator.comparingInt(Map.Entry::getValue);
            final Optional<Map.Entry<Value, Integer>> actualResponse = v.stream()
                    .collect(Collectors.toMap(Function.identity(), r -> 1, Integer::sum)).entrySet().stream()
                    .max(comparator.thenComparingLong(v1 -> v1.getKey().getTimestamp()));
            if (actualResponse.isPresent()) {
                result.complete(actualResponse.get().getKey());
            } else {
                result.complete(Value.errorValue(Response.INTERNAL_ERROR));
            }
        }).exceptionally(e -> {
            log.error("merge - ", e);
            return null;
        });
        return result;
    }

    static CompletableFuture<List<Value>> collect(@NotNull final List<CompletableFuture<Value>> values,
                                                  int min) {
        final CompletableFuture<List<Value>> future = new CompletableFuture<>();
        final AtomicInteger errors = new AtomicInteger(0);
        final List<Value> result = new ArrayList<>(min);
        final Lock lock = new ReentrantLock();
        final int maxErrors = values.size() - min + 1;
        for (final CompletableFuture<Value> value :
                values) {
            value.whenComplete((v, e) -> {
                if (e != null) {
                    if (errors.incrementAndGet() == maxErrors) {
                        future.completeExceptionally(new RejectedExecutionException(e));
                    }
                    return;
                }
                lock.lock();
                try {
                    if (result.size() >= min) {
                        return;
                    }
                    result.add(v);
                    if (result.size() == min) {
                        future.complete(result);
                    }
                } finally {
                    lock.unlock();
                }
            }).exceptionally(e -> {
                log.error("Future error - ", e);
                return null;
            });
        }
        return future;
    }

    static List<CompletableFuture<Value>> replication(@NotNull final AsyncService.Action action,
                                                      @NotNull final Request request,
                                                      @NotNull final ByteBuffer key,
                                                      @NotNull final Replicas replicas,
                                                      @NotNull final Topology<String> topology,
                                                      @NotNull final HttpClient client) {
        final Set<String> nodes = topology.primaryFor(key, replicas);
        final List<CompletableFuture<Value>> result = new ArrayList<>(nodes.size());
        for (final String node : nodes) {
            if (topology.isMe(node)) {
                final CompletableFuture<Value> future = CompletableFuture.supplyAsync(() -> {
                    final Response response = action.act();
                    return new Value(response.getStatus(), response.getBody(), getTimestamp(response));
                });
                result.add(future);
            } else {
                final URI uri = URI.create(node
                        + "/v0/entity?id="
                        + StandardCharsets.UTF_8.decode(key).toString()
                        + "&replicas="
                        + replicas.toString());
                key.flip();
                final CompletableFuture<Value> future = client
                        .sendAsync(convertRequest(request, uri), HttpResponse.BodyHandlers.ofByteArray())
                        .thenApply(r -> new Value(r.statusCode(),
                                r.body(),
                                Long.parseLong(r.headers().firstValue(Replica.TIMESTAMP_HEADER).orElse("-1"))));
                result.add(future);
            }
        }
        return result;
    }

    private static HttpRequest convertRequest(@NotNull final Request request, @NotNull final URI uri) {
        final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .timeout(Replica.timeout)
                .uri(uri)
                .headers(AsyncService.PROXY_HEADER_KEY, AsyncService.PROXY_HEADER_VALUE);
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return builder.GET().build();
            case Request.METHOD_PUT:
                return builder.PUT(HttpRequest.BodyPublishers.ofByteArray(request.getBody())).build();
            case Request.METHOD_DELETE:
                return builder.DELETE().build();
            default:
                throw new UnsupportedOperationException(request.getMethod() + "not available");
        }
    }

    private static long getTimestamp(@NotNull final Response response) {
        final String timestamp = response.getHeader(Replica.TIMESTAMP_HEADER + ":");
        return timestamp == null ? -1 : Long.parseLong(timestamp);
    }
}
