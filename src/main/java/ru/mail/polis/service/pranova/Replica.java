package ru.mail.polis.service.pranova;


import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.pranova.Cell;
import ru.mail.polis.dao.pranova.ExtendedDAO;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Set;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

class Replica {
    private static final String TIMESTAMP = "timestamp";
    private static final String IOE_ERR = "IOException on session send error";
    private static final String NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";
    private static final Logger log = LoggerFactory.getLogger(AsyncService.class);
    private final ExtendedDAO dao;
    private final Executor executor;
    private final Topology<String> topology;
    private final HttpClient client;
    private final static Duration timeout = Duration.ofSeconds(1);

    Replica(@NotNull final ExtendedDAO dao,
            @NotNull final Executor executor,
            @NotNull final Topology<String> topology) {
        this.dao = dao;
        this.executor = executor;
        this.topology = topology;
        this.client = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    private Response get(@NotNull final ByteBuffer key) {
        final Cell cell;
        try {
            cell = dao.getCell(key);
            if (cell.getValue().isRemoved()) {
                final Response response = new Response(Response.NOT_FOUND, Response.EMPTY);
                response.addHeader(TIMESTAMP + AsyncService.HEADER_SEP + cell.getValue().getTimeStamp());
                return response;
            }
            final byte[] body = new byte[cell.getValue().getData().remaining()];
            cell.getValue().getData().get(body);
            final Response response = new Response(Response.OK, body);
            response.addHeader(TIMESTAMP + AsyncService.HEADER_SEP + cell.getValue().getTimeStamp());
            return response;
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            log.error("Can't get", e);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response put(@NotNull final ByteBuffer key, @NotNull final Request request) {
        try {
            final ByteBuffer valueBuff = ByteBuffer.wrap(request.getBody());
            dao.upsert(key, valueBuff);
            return new Response(Response.CREATED, Response.EMPTY);
        } catch (IOException ex) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response delete(@NotNull final ByteBuffer key) {
        try {
            dao.remove(key);
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } catch (IOException ex) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    protected void execPut(@NotNull final HttpSession session,
                           @NotNull final Request request,
                           @NotNull final ByteBuffer key,
                           final boolean isProxy,
                           @NotNull final Replicas replicas) {
        if (isProxy) {
            asyncAct(session, () -> put(key, request));
            return;
        }
        List<CompletableFuture<Value>> futures = replication(() -> put(key, request), request, key, replicas);
        schedule(futures, replicas.getAck(), session);
    }

    private void schedule(@NotNull final List<CompletableFuture<Value>> futures,
                          final int min,
                          @NotNull final HttpSession session) {
        CompletableFuture<List<Value>> future = collect(futures, min);
        CompletableFuture<Value> result = merge(future);
        result.thenAccept(v -> {
            try {
                session.sendResponse(v.toResponse());
            } catch (IOException e) {
                try {
                    log.error("schedule", e);
                    session.sendError(Response.INTERNAL_ERROR, "");
                } catch (IOException ex) {
                    log.error(IOE_ERR, e);
                }
            }
        });
    }

    protected void execDelete(@NotNull final HttpSession session,
                              @NotNull final Request request,
                              @NotNull final ByteBuffer key,
                              final boolean isProxy,
                              @NotNull final Replicas replicas) {
        if (isProxy) {
            asyncAct(session, () -> delete(key));
            return;
        }
        List<CompletableFuture<Value>> futures = replication(() -> delete(key), request, key, replicas);
        schedule(futures, replicas.getAck(), session);
    }

    protected void execGet(@NotNull final HttpSession session,
                           @NotNull final Request request,
                           @NotNull final ByteBuffer key,
                           final boolean isProxy,
                           @NotNull final Replicas replicas) {
        if (isProxy) {
            asyncAct(session, () -> get(key));
            return;
        }
        List<CompletableFuture<Value>> futures = replication(() -> get(key), request, key, replicas);
        schedule(futures, replicas.getAck(), session);
    }

    private Response mergeResponses(@NotNull final List<Response> result) {
        final Map<Response, Integer> responses = new TreeMap<>(Comparator.comparing(this::getStatus));
        result.forEach(resp -> {
            final Integer val = responses.get(resp);
            responses.put(resp, val == null ? 0 : val + 1);
        });
        Response finalResult = null;
        int maxCount = -1;
        long time = Long.MIN_VALUE;
        for (final Map.Entry<Response, Integer> entry : responses.entrySet()) {
            if (entry.getValue() >= maxCount && getTimestamp(entry.getKey()) > time) {
                time = getTimestamp(entry.getKey());
                maxCount = entry.getValue();
                finalResult = entry.getKey();
            }
        }
        return finalResult;
    }

    private void asyncAct(@NotNull final HttpSession session,
                          @NotNull final AsyncService.Action action) {
        executor.execute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, "");
                } catch (IOException ex) {
                    log.error(IOE_ERR, e);
                }
            }
        });
    }

    private CompletableFuture<Value> merge(@NotNull final CompletableFuture<List<Value>> future) {
        CompletableFuture<Value> result = new CompletableFuture<>();
        future.whenCompleteAsync((v, e) -> {
            if (e != null) {
                result.complete(Value.errorValue(NOT_ENOUGH_REPLICAS));
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

    private CompletableFuture<List<Value>> collect(@NotNull final List<CompletableFuture<Value>> values,
                                                   int min) {
        CompletableFuture<List<Value>> future = new CompletableFuture<>();
        AtomicInteger errors = new AtomicInteger(0);
        List<Value> result = new ArrayList<>(min);
        Lock lock = new ReentrantLock();
        int maxErrors = values.size() - min + 1;
        for (CompletableFuture<Value> value :
                values) {
            value.whenComplete((v, e) -> {
                if (e != null) {
                    if (errors.incrementAndGet() == maxErrors) {
                        future.completeExceptionally(new RejectedExecutionException(e));
                    }
                    return;
                }
                try {
                    lock.lock();
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

    private List<CompletableFuture<Value>> replication(@NotNull final AsyncService.Action action,
                                                       @NotNull final Request request,
                                                       @NotNull final ByteBuffer key,
                                                       @NotNull final Replicas replicas) {
        final Set<String> nodes = topology.primaryFor(key, replicas);
        final List<CompletableFuture<Value>> result = new ArrayList<>(nodes.size());
        for (final String node : nodes) {
            if (topology.isMe(node)) {
                CompletableFuture<Value> future = CompletableFuture.supplyAsync(() -> {
                    Response response = action.act();
                    Value value = new Value(response.getStatus(), response.getBody(), getTimestamp(response));
                    return value;
                });
                result.add(future);
            } else {
                URI uri = URI.create(node
                        + "/v0/entity?id="
                        + StandardCharsets.UTF_8.decode(key).toString()
                        + "&replicas="
                        + replicas.toString());
                key.flip();
                HttpRequest request1 = convertRequest(request, uri);
                CompletableFuture<Value> future = client
                        .sendAsync(request1, HttpResponse.BodyHandlers.ofByteArray())
                        .thenApply(r -> {
                            Value value = new Value(r.statusCode(),
                                    r.body(),
                                    Long.parseLong(r.headers().firstValue(TIMESTAMP).orElse("-1")));
                            return value;
                        });
                result.add(future);
            }
        }
        return result;
    }

    private HttpRequest convertRequest(@NotNull final Request request, URI uri) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .timeout(timeout)
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


    private String getStatus(@NotNull final Response response) {
        return response.getHeaders()[0];
    }

    /**
     * Return timestamp for response, -1 if no timestamp.
     *
     * @param response input value.
     * @return long value.
     */
    public static long getTimestamp(@NotNull final Response response) {
        final String timestamp = response.getHeader(TIMESTAMP + ":");
        return timestamp == null ? -1 : Long.parseLong(timestamp);
    }
}
