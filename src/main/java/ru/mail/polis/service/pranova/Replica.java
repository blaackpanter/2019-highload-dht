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
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.time.Duration;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

class Replica {
    static final String TIMESTAMP_HEADER = "timestamp";
    private static final String IOE_ERR = "IOException on session send error";
    static final String NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";
    private static final Logger log = LoggerFactory.getLogger(AsyncService.class);
    private final ExtendedDAO dao;
    private final Executor executor;
    private final Topology<String> topology;
    private final HttpClient client;
    final static Duration timeout = Duration.ofSeconds(1);

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
                response.addHeader(TIMESTAMP_HEADER + AsyncService.HEADER_SEP + cell.getValue().getTimeStamp());
                return response;
            }
            final byte[] body = new byte[cell.getValue().getData().remaining()];
            cell.getValue().getData().get(body);
            final Response response = new Response(Response.OK, body);
            response.addHeader(TIMESTAMP_HEADER + AsyncService.HEADER_SEP + cell.getValue().getTimeStamp());
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

    void execPut(@NotNull final Context context,
                 @NotNull final ByteBuffer key) {
        execAct(context,
                key,
                () -> put(key, context.getRequest()));
    }

    void execAct(@NotNull final Context context,
                 @NotNull final ByteBuffer key,
                 @NotNull final AsyncService.Action action) {
        if (context.isProxy()) {
            asyncAct(context.getSession(), action);
            return;
        }
        final List<CompletableFuture<Value>> futures = FutureUtils.replication(action, key, topology, client, context);
        schedule(futures, context.getRf().getAck(), context.getSession());
    }

    private void schedule(@NotNull final List<CompletableFuture<Value>> futures,
                          final int min,
                          @NotNull final HttpSession session) {
        final CompletableFuture<List<Value>> future = FutureUtils.collect(futures, min);
        final CompletableFuture<Value> result = FutureUtils.merge(future);
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
        }).exceptionally(e -> {
            log.error("schedule error - ", e);
            return null;
        });
    }

    void execDelete(@NotNull final Context context,
                    @NotNull final ByteBuffer key) {
        execAct(context, key, () -> delete(key));
    }

    void execGet(@NotNull final Context context,
                 @NotNull final ByteBuffer key) {
        execAct(context, key, () -> get(key));
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
}
