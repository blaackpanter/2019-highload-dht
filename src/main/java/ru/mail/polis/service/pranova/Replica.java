package ru.mail.polis.service.pranova;

import one.nio.http.HttpClient;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.http.Request;
import one.nio.http.HttpException;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.pranova.Cell;
import ru.mail.polis.dao.pranova.ExtendedDAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.Set;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

class Replica {
    private static final String TIMESTAMP = "Timestamp: ";
    private static final String IOE_ERR = "IOException on session send error";
    private static final String NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";
    private static final String PROXY_HEADER = "Is-Proxy: True";
    private static final Logger log = LoggerFactory.getLogger(AsyncService.class);
    private final ExtendedDAO dao;
    private final Executor executor;
    private final Topology<String> topology;
    private final Map<String, HttpClient> clusters;

    Replica(@NotNull final ExtendedDAO dao,
            @NotNull final Executor executor,
            @NotNull final Topology<String> topology,
            @NotNull final Map<String, HttpClient> clusters) {
        this.dao = dao;
        this.executor = executor;
        this.topology = topology;
        this.clusters = clusters;
    }

    private Response get(@NotNull final ByteBuffer key) {
        final Cell cell;
        try {
            cell = dao.getCell(key);
            if (cell.getValue().isRemoved()) {
                final Response response = new Response(Response.NOT_FOUND, Response.EMPTY);
                response.addHeader(TIMESTAMP + cell.getValue().getTimeStamp());
                return response;
            }
            final byte[] body = new byte[cell.getValue().getData().remaining()];
            cell.getValue().getData().get(body);
            final Response response = new Response(Response.OK, body);
            response.addHeader(TIMESTAMP + cell.getValue().getTimeStamp());
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
        executor.execute(() -> {
            final List<Response> result = replication(() -> put(key, request), request, key, replicas);
            int ack = 0;
            for (final Response current : result) {
                if (getStatus(current).equals(Response.CREATED)) {
                    ack++;
                }
            }
            correctReplication(ack, replicas, session, Response.CREATED);
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
        executor.execute(() -> {
            final List<Response> result = replication(() -> delete(key), request, key, replicas);
            int ack = 0;
            for (final Response current : result) {
                if (getStatus(current).equals(Response.ACCEPTED)) {
                    ack++;
                }
            }
            correctReplication(ack, replicas, session, Response.ACCEPTED);
        });
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
        executor.execute(() -> {
            try {
                final List<Response> result = replication(() -> get(key), request, key, replicas).stream()
                        .filter(resp -> getStatus(resp).equals(Response.OK)
                                || getStatus(resp).equals(Response.NOT_FOUND)).collect(Collectors.toList());
                if (result.size() < replicas.getAck()) {
                    session.sendResponse(new Response(NOT_ENOUGH_REPLICAS, Response.EMPTY));
                    return;
                }
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
                session.sendResponse(finalResult);
            } catch (IOException e) {
                try {
                    log.error("get", e);
                    session.sendError(Response.INTERNAL_ERROR, "");
                } catch (IOException ex) {
                    log.error(IOE_ERR, e);
                }
            }
        });
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

    private void correctReplication(@NotNull final int ack,
                                    @NotNull final Replicas replicas,
                                    @NotNull final HttpSession session,
                                    @NotNull final String str) {
        try {
            if (ack < replicas.getAck()) {
                session.sendResponse(new Response(NOT_ENOUGH_REPLICAS, Response.EMPTY));
            } else {
                session.sendResponse(new Response(str, Response.EMPTY));
            }
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, "");
            } catch (IOException ex) {
                log.error(IOE_ERR, e);
            }
        }
    }

    private List<Response> replication(@NotNull final AsyncService.Action action,
                                       @NotNull final Request request,
                                       @NotNull final ByteBuffer key,
                                       @NotNull final Replicas replicas) {
        request.addHeader(PROXY_HEADER);
        final Set<String> nodes = topology.primaryFor(key, replicas);
        final List<Response> result = new ArrayList<>(nodes.size());
        for (final String node : nodes) {
            if (topology.isMe(node)) {
                result.add(action.act());
            } else {
                result.add(proxy(node, request));
            }
        }
        return result;
    }

    private Response proxy(@NotNull final String node, @NotNull final Request request) {
        try {
            return clusters.get(node).invoke(request);
        } catch (InterruptedException | PoolException | HttpException | IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
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
        final String timestamp = response.getHeader(TIMESTAMP);
        return timestamp == null ? -1 : Long.parseLong(timestamp);
    }
}
