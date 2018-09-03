package ru.kolpakov.tasks.sort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static ru.kolpakov.tasks.sort.BigFileSort.MEMORY_USAGE_COEFFICIENT;

class FirstStepSort {
    private static final Logger log = LoggerFactory.getLogger(FirstStepSort.class);
    private final Path inputPath;
    private final Path tmpPath;
    private final BlockingQueue<List<String>> chunksToSort;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final int sizeToDump;
    private final int parallelism;
    private final ExecutorService executorService;
    private final Comparator<String> comparator;
    private volatile boolean isRead = false;

    FirstStepSort(Path inputPath, Path tmpPath, long maxMemory, int parallelism, Comparator<String> comparator) throws IOException {
        this.inputPath = inputPath;
        this.tmpPath = tmpPath;
        this.chunksToSort = new ArrayBlockingQueue<>(2);
        this.comparator = comparator;
        long inputSize = Files.size(inputPath);
        sizeToDump = (int) ((MEMORY_USAGE_COEFFICIENT * maxMemory) / parallelism);
        log.info("Total input size is {}; size to sortAndDump to disk from sorter runnable {}", inputSize, sizeToDump);
        this.parallelism = parallelism;
        this.executorService = Executors.newWorkStealingPool(parallelism + 1);
    }

    void sort() throws IOException {
        try {
            Path firstSortDir = Paths.get(tmpPath.toString(), "first");
            Files.createDirectories(firstSortDir);
            CompletableFuture<?> readFuture = CompletableFuture.runAsync(new ReadRunnable(), executorService);
            CompletableFuture.allOf(IntStream.range(0, parallelism)
                    .mapToObj(i -> CompletableFuture.runAsync(new WriteSortedRunnable(firstSortDir), executorService))
                    .toArray(CompletableFuture[]::new))
                    .get();
            readFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new BigFileSortException(e);
        }
    }

    private class WriteSortedRunnable implements Runnable {
        private final Path firstSortDir;

        private WriteSortedRunnable(Path firstSortDir) {
            this.firstSortDir = firstSortDir;
        }

        @Override
        public void run() {
            try {
                List<String> chunkToSort;
                while ((chunkToSort = chunksToSort.poll(100L, TimeUnit.MILLISECONDS)) != null || !isRead) {
                    if (chunkToSort != null) {
                        sortAndDump(chunkToSort);
                    }
                }
                log.info("Thread {} finished sorting and dumping", Thread.currentThread().getName());
            } catch (InterruptedException e) {
                throw new BigFileSortException(e);
            }
        }

        private void sortAndDump(List<String> linesToSort) {
            String fileName = Integer.toString(counter.getAndIncrement());
            Path pathToWrite = Paths.get(firstSortDir.toString(), fileName);
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(pathToWrite.toFile()))) {
                linesToSort.sort(comparator);
                log.info("writing to {}", fileName);
                for (String line : linesToSort) {
                    bw.write(line);
                    bw.write("\n");
                }
            } catch (IOException e) {
                throw new BigFileSortException(e);
            }
        }
    }

    private class ReadRunnable implements Runnable {
        @Override
        public void run() {
            log.info("reading from {}", inputPath.toAbsolutePath());
            try (BufferedReader br = new BufferedReader(new FileReader(inputPath.toFile()))) {
                String str;
                List<String> readLines = new ArrayList<>();
                int approxSize = 0;
                while ((str = br.readLine()) != null) {
                    readLines.add(str);
                    approxSize += str.length() * 2;
                    if (approxSize > sizeToDump) {
                        chunksToSort.put(readLines);
                        readLines = new ArrayList<>();
                        approxSize = 0;
                    }
                }
                chunksToSort.put(readLines);
            } catch (IOException | InterruptedException e) {
                throw new BigFileSortException(e);
            } finally {
                log.info("finished reading");
                isRead = true;
            }
        }
    }
}
