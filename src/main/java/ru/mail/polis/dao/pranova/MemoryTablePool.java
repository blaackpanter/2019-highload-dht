package ru.mail.polis.dao.pranova;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryTablePool implements Table, Closeable {

    private volatile MemTable current;
    private final NavigableMap<Long, Table> pendingToFlushTables;
    private long generation;
    private final long flushLimit;
    private final BlockingQueue<FlushTable> flushTable;
    private final AtomicBoolean compacting = new AtomicBoolean(false);

    private final AtomicBoolean stop = new AtomicBoolean();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Pool tables.
     *
     * @param startGeneration is base number of table.
     * @param flushLimit      is max size of storage.
     */
    public MemoryTablePool(final long startGeneration, final long flushLimit) {
        this.generation = startGeneration;
        this.flushLimit = flushLimit;
        this.current = new MemTable();
        this.pendingToFlushTables = new TreeMap<>();
        this.flushTable = new ArrayBlockingQueue<>(2);

    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            return current.sizeInBytes();
        } finally {
            lock.readLock().unlock();
        }
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(final @NotNull ByteBuffer from) throws IOException {
        final Collection<Iterator<Cell>> iterators;
        lock.readLock().lock();
        try {

            iterators = new ArrayList<>(pendingToFlushTables.size() + 1);
            for (final Table table : pendingToFlushTables.descendingMap().values()) {
                iterators.add(table.iterator(from));
            }
            iterators.add(current.iterator(from));
        } finally {
            lock.readLock().unlock();
        }
        final Iterator<Cell> mergeIterator = Iterators.mergeSorted(iterators, Cell.COMPARATOR);
        return Iters.collapseEquals(mergeIterator, Cell::getKey);
    }

    @Override
    public void upsert(final @NotNull ByteBuffer key, final @NotNull ByteBuffer value) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped!");
        }
        current.upsert(key, value);
        enqueueFlush();
    }

    @Override
    public void remove(final @NotNull ByteBuffer key) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped!");
        }
        current.remove(key);
        enqueueFlush();
    }

    private void enqueueFlush() {
        FlushTable currentFlushTable = null;
        lock.writeLock().lock();
        try {

            if (current.sizeInBytes() > flushLimit) {
                currentFlushTable = new FlushTable(generation,
                        current.iterator(LSMDao.nullBuffer),
                        false);
                pendingToFlushTables.put(generation, current);
                generation = generation + 1;
                current = new MemTable();
            }
        } finally {
            lock.writeLock().unlock();
        }
        if (currentFlushTable != null) {
            try {
                this.flushTable.put(currentFlushTable);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public FlushTable tableToFlush() throws InterruptedException {
        return flushTable.take();
    }

    /**
     * Flush to disk.
     *
     * @param generation is number of table which was thrown.
     */
    public void flushed(final long generation) {
        lock.writeLock().lock();
        try {
            pendingToFlushTables.remove(generation);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (!stop.compareAndSet(false, true)) {
            return;
        }
        FlushTable flushT;
        lock.writeLock().lock();
        try {
            flushT = new FlushTable(generation, current.iterator(LSMDao.nullBuffer), true, false);
        } finally {
            lock.writeLock().unlock();
        }

        try {
            this.flushTable.put(flushT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void compact() {
        if (!compacting.compareAndSet(false, true)) {
            return;
        }
        FlushTable table = null;
        lock.writeLock().lock();
        try {
            table = new FlushTable(generation,
                    current.iterator(LSMDao.nullBuffer),
                    true, true);
            pendingToFlushTables.put(generation, current);
            current = new MemTable();
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushTable.put(table);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
