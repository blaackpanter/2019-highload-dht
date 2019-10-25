package ru.mail.polis.dao.pranova;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileTable implements Table {
    private final ByteBuffer cells;
    private final int rows;
    private final LongBuffer offsets;
    private final File file;

    /**
     * Create an object for file on disk.
     *
     * @param file to get a table
     * @throws IOException if an I/O error is thrown by a visitor method
     */
    FileTable(@NotNull final File file) throws IOException {
        this.file = file;
        final long fileSize = file.length();
        final ByteBuffer mapped;
        try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            assert file.length() <= Integer.MAX_VALUE;
            mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0L, fileSize).order(ByteOrder.BIG_ENDIAN);
        }

        // Rows
        final long rowsValue = mapped.getLong((int) (fileSize - Long.BYTES));
        assert rowsValue <= Integer.MAX_VALUE;
        this.rows = (int) rowsValue;

        // Offsets
        final ByteBuffer offsetBuffer = mapped.duplicate();
        offsetBuffer.position(mapped.limit() - Long.BYTES * rows - Long.BYTES);
        offsetBuffer.limit(mapped.limit() - Long.BYTES);
        this.offsets = offsetBuffer.slice().asLongBuffer();

        // Cells
        final ByteBuffer cellBuffer = mapped.duplicate();
        cellBuffer.limit(offsetBuffer.position());
        this.cells = cellBuffer.slice();
    }

    static void write(@NotNull final Iterator<Cell> cells, @NotNull final File to) throws IOException {
        try (FileChannel fc = FileChannel.open(to.toPath(), StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            final List<Long> offsets = new ArrayList<>();
            long offset = 0;
            while (cells.hasNext()) {
                final Cell cell = cells.next();
                offsets.add(offset);

                // Key
                offset += writeBuffer(fc, cell.getKey());

                // Value
                final Value value = cell.getValue();

                // Timestamp
                if (value.isRemoved()) {
                    fc.write(Bytes.fromLong(-cell.getValue().getTimeStamp()));
                } else {
                    fc.write(Bytes.fromLong(cell.getValue().getTimeStamp()));
                }
                offset += Long.BYTES;

                // Value
                if (!value.isRemoved()) {
                    offset += writeBuffer(fc, value.getData());
                }
            }

            // Offsets
            for (final Long anOffset : offsets) {
                fc.write(Bytes.fromLong(anOffset));
            }

            // Cells
            fc.write(Bytes.fromLong(offsets.size()));
        }
    }

    private static long writeBuffer(@NotNull final FileChannel fc,
                                    @NotNull final ByteBuffer buffer) throws IOException {
        final int valueSize = buffer.remaining();
        fc.write(Bytes.fromInt(valueSize));
        int offset = Integer.BYTES;
        fc.write(buffer);
        offset += valueSize;
        return offset;
    }

    private ByteBuffer keyAt(final int i) {
        assert 0 <= i && i < rows;
        final long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;
        final int keySize = cells.getInt((int) offset);
        final ByteBuffer key = cells.duplicate();
        key.position((int) (offset + Integer.BYTES));
        key.limit(key.position() + keySize);
        return key.slice();
    }

    private Cell cellAt(final int i) {
        assert 0 <= i && i < rows;
        long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;

        // Key
        final int keySize = cells.getInt((int) offset);
        offset += Integer.BYTES;
        final ByteBuffer key = keyAt(i);
        offset += keySize;

        // Timestamp
        final long timestamp = cells.getLong((int) offset);
        offset += Long.BYTES;

        // Value
        if (timestamp < 0) {
            return new Cell(key.slice(), new Value(-timestamp, null));
        } else {
            final int valueSize = cells.getInt((int) offset);
            offset += Integer.BYTES;
            final ByteBuffer value = cells.duplicate();
            value.position((int) offset);
            value.limit(value.position() + valueSize);
            return new Cell(key.slice(), new Value(timestamp, value.slice()));
        }
    }

    private int position(@NotNull final ByteBuffer from, @NotNull final Order order) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final int cmp = from.compareTo(keyAt(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return order == Order.DIRECT ? left : right;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<Cell>() {
            int next = position(from, Order.DIRECT);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cell next() {
                assert hasNext();
                return cellAt(next++);
            }
        };
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sizeInBytes() {
        throw new UnsupportedOperationException();
    }

    public void deleteFileTable() throws IOException {
        Files.delete(file.toPath());
    }

    @Override
    public Iterator<Cell> decreasingIterator(@NotNull final ByteBuffer from) {
        return new Iterator<Cell>() {
            int next = position(from, Order.REVERSE);

            @Override
            public boolean hasNext() {
                return next >= 0;
            }

            @Override
            public Cell next() {
                assert hasNext();
                return cellAt(next--);
            }
        };
    }

    public enum Order {
        DIRECT,
        REVERSE
    }
}
