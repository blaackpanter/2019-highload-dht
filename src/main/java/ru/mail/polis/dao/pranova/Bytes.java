package ru.mail.polis.dao.pranova;

import java.nio.ByteBuffer;

final class Bytes {
    private Bytes() {
    }

    /**
     * Return ByteBuffer from int value.
     *
     * @param value for ByteBuffer
     * @return ByteBuffer
     */
    static ByteBuffer fromInt(final int value) {
        final ByteBuffer result = ByteBuffer.allocate(Integer.BYTES);
        result.putInt(value);
        result.rewind();
        return result;
    }

    /**
     * Return ByteBuffer from long value.
     * *
     * * @param value for ByteBuffer
     * * @return ByteBuffer
     */
    static ByteBuffer fromLong(final long value) {
        final ByteBuffer result = ByteBuffer.allocate(Long.BYTES);
        result.putLong(value);
        result.rewind();
        return result;
    }
}
