package ru.mail.polis.dao.pranova;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

public final class LSMDao implements ExtendedDAO {
    private static final String SUFFIX = ".dat";
    public static final String TEMP = ".tmp";
    private static final String PREFIX = "PRL";
    private final MemoryTablePool memTable;
    private final File base;
    private long generation;
    private final NavigableMap<Long, FileTable> files;
    public static final ByteBuffer nullBuffer = ByteBuffer.allocate(0);
    private final Worker worker;
    private static final Logger log = LoggerFactory.getLogger(LSMDao.class);

    /**
     * LSM storage.
     *
     * @param base           is root directory.
     * @param flushThreshold is max size of storage.
     * @throws IOException if an I/O error is thrown by a visitor method.
     */
    public LSMDao(@NotNull final File base,
                  final long flushThreshold) throws IOException {
        this.base = base;
        assert flushThreshold >= 0L;
        generation = 0;
        files = new ConcurrentSkipListMap<>();
        final EnumSet<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        final int maxDeep = 1;
        Files.walkFileTree(base.toPath(), options, maxDeep, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) throws IOException {
                if (path.getFileName().toString().endsWith(SUFFIX)
                        && path.getFileName().toString().startsWith(PREFIX)) {
                    generation++;
                    files.put(generation, new FileTable(path.toFile()));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        generation = files.size() + 1L;
        this.memTable = new MemoryTablePool(generation, flushThreshold);
        this.worker = new Worker();
        worker.start();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Cell> alive = Iterators.filter(getIterator(from, FileTable.Order.DIRECT),
                cell -> !cell.getValue().isRemoved());
        return Iterators.transform(alive, cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    @Override
    public void compact() throws IOException {
        memTable.compact();
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Iterator<Cell> getCellsIterator(@NotNull final List<Iterator<Cell>> iterators) {
        final Iterator<Cell> mergedCells = Iterators.mergeSorted(iterators, Cell.COMPARATOR);
        return Iters.collapseEquals(mergedCells, Cell::getKey);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
    }

    private void flush(@NotNull final Iterator<Cell> data, final long generation) throws IOException {
        final File tmp = new File(base, PREFIX + generation + TEMP);
        FileTable.write(data, tmp);
        final File dest = new File(base, PREFIX + generation + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        files.put(generation, new FileTable(dest));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        memTable.close();
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Iterator from end.
     *
     * @param from input parameter.
     * @return iterator with record.
     * @throws IOException exception.
     */
    public Iterator<Record> decreasingIterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Cell> allCellsD = getIterator(from, FileTable.Order.REVERSE);
        final Iterator<Cell> aliveD = Iterators.filter(allCellsD, cell -> !cell.getValue().isRemoved());
        return Iterators.transform(aliveD, cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    private Iterator<Cell> getIterator(@NotNull final ByteBuffer from,
                                       @NotNull final FileTable.Order order)
            throws IOException {
        final List<Iterator<Cell>> filesIterators = new ArrayList<>();

        for (final FileTable fileTable : files.values()) {
            filesIterators.add(order == FileTable.Order.DIRECT
                    ? fileTable.iterator(from)
                    : fileTable.decreasingIterator(from));
        }

        filesIterators.add(order == FileTable.Order.DIRECT
                ? memTable.iterator(from)
                : memTable.decreasingIterator(from));
        return getCellsIterator(filesIterators);
    }

    @Override
    public Cell getCell(@NotNull final ByteBuffer key) throws IOException {
        final Iterator<Cell> iter = getIterator(key, FileTable.Order.DIRECT);
        if (!iter.hasNext()) {
            throw new NoSuchElementException("Not found");
        }

        final Cell next = iter.next();
        if (next.getKey().equals(key)) {
            return next;
        } else {
            throw new NoSuchElementException("Not found");
        }
    }

    class Worker extends Thread {
        Worker() {
            super("worker");
        }

        @Override
        public void run() {
            boolean poisoned = false;
            boolean compacting = false;
            while (!poisoned && !isInterrupted()) {
                try {
                    final FlushTable table = memTable.tableToFlush();
                    poisoned = table.isPoisonPills();
                    flush(table.data(), table.getGeneration());
                    compacting = table.isCompactionTable();
                    if (compacting) {
                        compactFiles();
                    }
                    memTable.flushed(table.getGeneration());
                } catch (InterruptedException e) {
                    interrupt();
                } catch (IOException e) {
                    log.error("flushing", e);
                }
            }
        }
    }

    private void compactFiles() throws IOException {
        final List<Iterator<Cell>> filesIterators = new ArrayList<>();

        for (final FileTable fileTable : files.values()) {
            filesIterators.add(fileTable.iterator(ByteBuffer.allocate(0)));
        }

        final Iterator<Cell> alive = getCellsIterator(filesIterators);
        final File tmp = new File(base, PREFIX + 1 + TEMP);
        FileTable.write(alive, tmp);

        for (final FileTable fileTable : files.values()) {
            fileTable.deleteFileTable();
        }

        files.clear();
        final File dest = new File(base, PREFIX + 1 + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        files.put(0L, new FileTable(dest));
    }
}
