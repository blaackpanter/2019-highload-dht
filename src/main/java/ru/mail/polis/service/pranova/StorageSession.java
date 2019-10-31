package ru.mail.polis.service.pranova;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class StorageSession extends HttpSession {
    private static final byte[] CRLF = "\r\n".getBytes(Charsets.UTF_8);
    private static final byte[] NEW_LINE = "\n".getBytes(Charsets.UTF_8);
    private static final byte[] END = "0\r\n\r\n".getBytes(Charsets.UTF_8);
    private Iterator<Record> iterator;

    public StorageSession(@NotNull final Socket socket, @NotNull final HttpServer server) {
        super(socket, server);
    }

    void stream(@NotNull final Iterator<Record> iterator) throws IOException {
        this.iterator = iterator;

        final Response response = new Response(Response.OK);
        response.addHeader("Transfer-Encoding: chunked");
        writeResponse(response, false);
        next();
    }

    private void next() throws IOException {
        while (iterator.hasNext() && queueHead == null) {
            final Record record = iterator.next();
            final byte[] chunk = toArray(record);
            write(chunk, 0, chunk.length);
        }

        if (!iterator.hasNext()) {
            write(END, 0, END.length);

            server.incRequestsProcessed();

            if ((handling = pipeline.pollFirst()) != null) {
                if (handling == FIN) {
                    scheduleClose();
                } else {
                    server.handleRequest(handling, this);
                }
            }
        }
    }

    private byte[] toArray(@NotNull final Record record) {
        final byte[] key = new byte[record.getKey().remaining()];
        record.getKey().duplicate().get(key);
        final byte[] value = new byte[record.getValue().remaining()];
        record.getValue().duplicate().get(value);
        final int payloadLength = key.length + NEW_LINE.length + value.length;
        final String size = Integer.toHexString(payloadLength);
        final int chunkLength = size.length() + CRLF.length + payloadLength + CRLF.length;
        final byte[] chunk = new byte[chunkLength];
        final ByteBuffer chunkBuffer = ByteBuffer.wrap(chunk);
        chunkBuffer.put(size.getBytes(Charsets.UTF_8));
        chunkBuffer.put(CRLF);
        chunkBuffer.put(key);
        chunkBuffer.put(NEW_LINE);
        chunkBuffer.put(value);
        chunkBuffer.put(CRLF);
        return chunk;
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        next();
    }
}
