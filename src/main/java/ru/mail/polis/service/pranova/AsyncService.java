package ru.mail.polis.service.pranova;

import com.google.common.base.Charsets;

import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Response;
import one.nio.http.Param;
import one.nio.http.Request;
import one.nio.http.HttpServerConfig;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.pranova.ExtendedDAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;

public class AsyncService extends HttpServer implements Service {
    private static final String PROXY_HEADER = "Is-Proxy: True";
    private final ExtendedDAO dao;
    private static final Logger log = LoggerFactory.getLogger(AsyncService.class);
    private final Map<String, HttpClient> clusters;
    private final Replica replica;

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
        this.clusters = new HashMap<>();
        for (final String node : topology.all()) {
            if (topology.isMe(node)) {
                continue;
            } else clusters.put(node, new HttpClient(new ConnectionString(node + "?timeout=100")));
        }
        replica = new Replica(this.dao, executor, topology, clusters);
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
        final Replicas replicasFactor = isProxy
                || replicas == null ? Replicas.quorum(clusters.size() + 1) : Replicas.parser(replicas);
        if (replicasFactor.getAck() > replicasFactor.getFrom() || replicasFactor.getAck() <= 0) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        final var method = request.getMethod();
        switch (method) {
            case Request.METHOD_GET:
                replica.execGet(session, request, key, isProxy, replicasFactor);
                break;
            case Request.METHOD_PUT:
                replica.execPut(session, request, key, isProxy, replicasFactor);
                break;
            case Request.METHOD_DELETE:
                replica.execDelete(session, request, key, isProxy, replicasFactor);
                break;
            default:
                new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
                break;
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

    private boolean isProxied(@NotNull final Request request) {
        return request.getHeader(PROXY_HEADER) != null;
    }

    public interface Action {
        Response act();
    }
}
