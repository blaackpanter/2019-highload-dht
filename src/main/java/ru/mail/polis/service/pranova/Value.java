package ru.mail.polis.service.pranova;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Objects;

class Value {
    private final String status;
    private final byte[] body;
    private final long timestamp;
    private static final byte[] EMPTY_VALUE = Response.EMPTY;
    private static final long EMPTY_TIMESTAMP = -1;

    public Value(final int status, final byte[] body, final long timestamp) {
        this(convertStatus(status), body, timestamp);
    }

    public Value(@NotNull final String status, final byte[] body, final long timestamp) {
        this.body = Arrays.copyOf(body, body.length);
        this.timestamp = timestamp;
        this.status = status;
    }

    private static String convertStatus(final int status) {
        switch (status) {
            case 200:
                return Response.OK;
            case 201:
                return Response.CREATED;
            case 202:
                return Response.ACCEPTED;
            case 400:
                return Response.BAD_REQUEST;
            case 404:
                return Response.NOT_FOUND;
            case 500:
                return Response.INTERNAL_ERROR;
            case 504:
                return "504 Not Enough Replicas";
            default:
                throw new UnsupportedOperationException(status + "not available");
        }
    }

    Response toResponse() {
        return new Response(status, body);
    }

    public byte[] getValue() {
        return Arrays.copyOf(body, body.length);
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(@NotNull final Object o) {
        if (this == o) return true;
        if (!(o instanceof Value)) return false;
        final Value value = (Value) o;
        return timestamp == value.timestamp
                && Objects.equals(status, value.status)
                && Arrays.equals(body, value.body);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(status, timestamp);
        result = 31 * result + Arrays.hashCode(body);
        return result;
    }

    public static Value errorValue(@NotNull final String status) {
        return new Value(status, EMPTY_VALUE, EMPTY_TIMESTAMP);
    }
}
