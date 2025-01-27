package ru.mail.polis.dao.pranova;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class MemTable implements Table {
    private final NavigableMap<ByteBuffer, Value> map = new ConcurrentSkipListMap<>();
    private final AtomicLong sizeInBytes = new AtomicLong();

    @Override
    public long sizeInBytes() {
        return sizeInBytes.get();
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                e -> new Cell(e.getKey(), e.getValue()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Value previous = map.put(key, Value.of(value));
        if (previous == null) {
            sizeInBytes.addAndGet(key.remaining() + value.remaining());
        } else if (previous.isRemoved()) {
            sizeInBytes.addAndGet(value.remaining());
        } else {
            sizeInBytes.addAndGet(value.remaining() - previous.getData().remaining());
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Value previous = map.put(key, Value.tombstone());
        if (previous == null) {
            sizeInBytes.addAndGet(key.remaining());
        } else if (!previous.isRemoved()) {
            sizeInBytes.addAndGet(-previous.getData().remaining());
        }
    }

    @Override
    public Iterator<Cell> decreasingIterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(
                map.headMap(from, true).descendingMap().entrySet().iterator(),
                e -> new Cell(e.getKey(), e.getValue()));
    }
}
