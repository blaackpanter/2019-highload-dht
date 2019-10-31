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
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

public class AsyncService extends HttpServer implements Service {
    private final DAO dao;
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
                        @NotNull final DAO dao,
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
                       @NotNull final HttpSession session) throws IOException {
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "Key is NULL");
            return;
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        final String node = topology.primaryFor(key);
        if (!topology.isMe(node)) {
            asyncAct(session, () -> proxy(node, request));
            return;
        }
        final var method = request.getMethod();
        switch (method) {
            case Request.METHOD_GET:
                asyncAct(session, () -> get(key));
                break;
            case Request.METHOD_PUT:
                asyncAct(session, () -> put(key, request));
                break;
            case Request.METHOD_DELETE:
                asyncAct(session, () -> delete(key));
                break;
            default:
                new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
                break;
        }
    }

    private Response proxy(@NotNull final String node, @NotNull final Request request) throws IOException {
        try {
            return clusters.get(node).invoke(request);
        } catch (InterruptedException | PoolException | HttpException e) {
            throw new IOException("Can't proxy", e);
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
        try {
            final ByteBuffer value = dao.get(key).duplicate();
            final byte[] body = new byte[value.duplicate().remaining()];
            value.get(body);
            return new Response(Response.OK, body);
        } catch (NoSuchElementException ex) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException ex) {
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

    public interface Action {
        Response act() throws IOException;
    }
}
