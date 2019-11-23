package ru.mail.polis.tank;

import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public final class GeneratorUtil {
    private static final String string = "\r\n";

    private GeneratorUtil() {
    }

    static String randomKey() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    static byte[] randomValue(final int length) {
        final byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    static void put(@NotNull final OutputStream out,
                    @NotNull final String key,
                    final byte[] value) throws IOException {
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, StandardCharsets.US_ASCII)) {
            writer.write("PUT /v0/entity?id=" + key + " HTTP/1.1\r\n");
            writer.write("Content-Length: " + value.length + string);
            writer.write(string);
        }
        request.write(value);
        out.write(Integer.toString(request.size()).getBytes(StandardCharsets.US_ASCII));
        out.write(" put\n".getBytes(StandardCharsets.US_ASCII));
        request.writeTo(out);
        out.write(string.getBytes(StandardCharsets.US_ASCII));
    }

    static void get(@NotNull final OutputStream out,
                    @NotNull final String key) throws IOException {
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, StandardCharsets.US_ASCII)) {
            writer.write("GET /v0/entity?id=" + key + " HTTP/1.1\r\n");
            writer.write(string);
        }
        out.write(Integer.toString(request.size()).getBytes(StandardCharsets.US_ASCII));
        out.write(" get\n".getBytes(StandardCharsets.US_ASCII));
        request.writeTo(out);
        out.write(string.getBytes(StandardCharsets.US_ASCII));
    }
}
