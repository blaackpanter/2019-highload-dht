package ru.mail.polis.service.pranova;

import com.google.common.base.Charsets;

import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Response;
import one.nio.http.Param;
import one.nio.http.Request;
import one.nio.http.HttpException;
import one.nio.http.HttpServerConfig;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.pranova.Cell;
import ru.mail.polis.dao.pranova.ExtendedDAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executor;

public class AsyncService extends HttpServer implements Service {
    private static final String PROXY_HEADER = "Is-Proxy: True";
    private static final String TIMESTAMP = "Timestamp: ";
    private static final String NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";
    private final ExtendedDAO dao;
    private final Executor executor;
    private static final Logger log = LoggerFactory.getLogger(AsyncService.class);
    private final Topology<String> topology;
    private final Map<String, HttpClient> clusters;

    /**
     * Async service.
     *
     * @param port     number of a port.
     * @param dao      LSMDao.
     * @param executor is pool of workers.
     * @throws IOException throw exception.
     */
    public AsyncService(final int port,
                        @NotNull final ExtendedDAO dao,
                        @NotNull final Executor executor,
                        @NotNull final Topology<String> topology) throws IOException {
        super(createService(port));
        this.dao = dao;
        this.executor = executor;
        this.topology = topology;
        this.clusters = new HashMap<>();
        for (final String node : topology.all()) {
            if (topology.isMe(node)) {
                continue;
            } else clusters.put(node, new HttpClient(new ConnectionString(node + "?timeout=100")));
        }
    }

    @Override
    public HttpSession createSession(@NotNull final Socket socket) throws RejectedSessionException {
        return new StorageSession(socket, this);
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    /**
     * Here is the main access.
     *
     * @param id      id is analog to the key in dao.
     * @param request the one of request (PUT, GET, DELETE) and the request body.
     */
    @Path("/v0/entity")
    public void entity(@Param("id") final String id, final Request request,
                       @NotNull final HttpSession session,
                       @Param("replicas") final String replicas) throws IOException {
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "Key is NULL");
            return;
        }
        final boolean isProxy = isProxied(request);
        final Replicas replicasFactor = isProxy || replicas == null ? Replicas.quorum(clusters.size() + 1) : Replicas.parser(replicas);
        if (replicasFactor.getAck() > replicasFactor.getFrom() || replicasFactor.getAck() <= 0) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        final var method = request.getMethod();
        switch (method) {
            case Request.METHOD_GET:
                execGet(session, request, key, isProxy, replicasFactor);
                break;
            case Request.METHOD_PUT:
                execPut(session, request, key, isProxy, replicasFactor);
                break;
            case Request.METHOD_DELETE:
                execDelete(session, request, key, isProxy, replicasFactor);
                break;
            default:
                new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
                break;
        }
    }

    private Response proxy(@NotNull final String node, @NotNull final Request request) {
        try {
            return clusters.get(node).invoke(request);
        } catch (InterruptedException | PoolException | HttpException | IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    /**
     * Resource for broadcast values for range.
     *
     * @param request the one of request (PUT, GET, DELETE) and the request body.
     * @param session is HttpSession.
     * @param start   from this first key.
     * @param end     to that last key.
     * @throws IOException throw exception.
     */
    @Path("/v0/entities")
    public void entities(@NotNull final Request request,
                         @NotNull final HttpSession session,
                         @Param("start") final String start,
                         @Param("end") final String end) throws IOException {
        if (start == null || start.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "Start is NULL");
            return;
        }
        if (end != null && end.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "End is NULL");
            return;
        }
        final ByteBuffer startR = ByteBuffer.wrap(start.getBytes(Charsets.UTF_8));
        final ByteBuffer endR = end == null ? null : ByteBuffer.wrap(end.getBytes(Charsets.UTF_8));
        try {
            final Iterator<Record> records = dao.range(startR, endR);
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, "");
            log.error("Exception", e);
        }
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

    private static HttpServerConfig createService(final int port) {
        if (port <= 1024 || port >= 65536) {
            throw new IllegalArgumentException("Invalid port");
        }
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        final HttpServerConfig config = new HttpServerConfig();
        acceptorConfig.port = port;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    @Override
    public void handleDefault(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private void asyncAct(@NotNull final HttpSession session,
                          @NotNull final Action action) {
        executor.execute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, "");
                } catch (IOException ex) {
                    log.error("IOException on session send error", e);
                }
            }
        });
    }

    private boolean isProxied(@NotNull final Request request) {
        return request.getHeader(PROXY_HEADER) != null;
    }

    public static long getTimestamp(@NotNull final Response response) {
        String timestamp = response.getHeader(TIMESTAMP);

        return timestamp == null ? -1 : Long.parseLong(timestamp);
    }

    private List<Response> replication(@NotNull final Action action,
                                       @NotNull final Request request,
                                       @NotNull final ByteBuffer key,
                                       @NotNull final Replicas replicas) {
        request.addHeader(PROXY_HEADER);
        final Set<String> nodes = topology.primaryFor(key, replicas);
        final List<Response> result = new ArrayList<>(nodes.size());
        for (String node : nodes) {
            if (topology.isMe(node)) {
                result.add(action.act());
            } else {
                result.add(proxy(node, request));
            }
        }
        return result;
    }

    private void execPut(@NotNull final HttpSession session,
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
            for (Response current : result) {
                if (getStatus(current).equals(Response.CREATED)) {
                    ack++;
                }
            }
            try {
                if (ack < replicas.getAck()) {
                    session.sendResponse(new Response(NOT_ENOUGH_REPLICAS, Response.EMPTY));
                } else {
                    session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                }
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, "");
                } catch (IOException ex) {
                    log.error("IOException on session send error", e);
                }
            }
        });
    }

    private void execDelete(@NotNull final HttpSession session,
                            @NotNull final Request request,
                            @NotNull final ByteBuffer key,
                            final boolean isProxy,
                            @NotNull final Replicas replicas) {
        if (isProxy) {
            asyncAct(session, () -> delete(key));
            return;
        }
        executor.execute(() -> {
            List<Response> result = replication(() -> delete(key), request, key, replicas);
            int ack = 0;
            for (Response current : result) {
                if (getStatus(current).equals(Response.ACCEPTED)) {
                    ack++;
                }
            }
            try {
                if (ack < replicas.getAck()) {
                    session.sendResponse(new Response(NOT_ENOUGH_REPLICAS, Response.EMPTY));
                } else {
                    session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                }
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, "");
                } catch (IOException ex) {
                    log.error("IOException on session send error", e);
                }
            }
        });
    }

    private void execGet(@NotNull final HttpSession session,
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
                final List<Response> result = replication(() -> get(key), request, key, replicas);
                int ack = 0;
                for (Response resp : result) {
                    if (getStatus(resp).equals(Response.OK) || getStatus(resp).equals(Response.NOT_FOUND)) {
                        ack++;
                    }
                }
                if (ack < replicas.getAck()) {
                    session.sendResponse(new Response(NOT_ENOUGH_REPLICAS, Response.EMPTY));
                    return;
                }
                Map<Response, Integer> responses = new TreeMap<>(Comparator.comparing(this::getStatus));
                result.forEach(resp -> {
                    if (getStatus(resp).equals(Response.OK) || getStatus(resp).equals(Response.NOT_FOUND)) {
                        final Integer val = responses.get(resp);
                        responses.put(resp, val == null ? 0 : val + 1);
                    }
                });
                Response finalResult = null;
                int maxCount = -1;
                long time = Long.MIN_VALUE;
                for (Map.Entry<Response, Integer> entry : responses.entrySet()) {
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
                    log.error("IOException on session send error", e);
                }
            }
        });
    }

    private String getStatus(@NotNull final Response response) {
        return response.getHeaders()[0];
    }

    public interface Action {
        Response act();
    }
}