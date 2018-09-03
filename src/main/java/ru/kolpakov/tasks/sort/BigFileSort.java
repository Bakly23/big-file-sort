package ru.kolpakov.tasks.sort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class BigFileSort {
    static final double MEMORY_USAGE_COEFFICIENT = 0.125;
    private static final Logger log = LoggerFactory.getLogger(BigFileSort.class);
    private final Path inputPath;
    private final Path tmpPath;
    private final Path outputPath;
    private final long maxMemory;
    private final int parallelism;
    private Comparator<String> comparator;

    public BigFileSort(Path inputPath, Path tmpPath, Path outputPath, long maxMemory, int parallelism) {
        this(inputPath, tmpPath, outputPath, maxMemory, parallelism, String::compareTo);
    }

    public BigFileSort(Path inputPath, Path tmpPath, Path outputPath, long maxMemory, int parallelism, Comparator<String> comparator) {
        this.inputPath = inputPath;
        this.tmpPath = tmpPath;
        this.outputPath = outputPath;
        this.maxMemory = maxMemory;
        this.parallelism = parallelism;
        this.comparator = comparator;
    }

    public void sort() throws IOException {
        log.info("Started first step: sort");
        comparator = String::compareTo;
        new FirstStepSort(inputPath, tmpPath, maxMemory, parallelism, comparator).sort();
        log.info("Finished first step: sort");
        log.info("Started second step: merge");
        new SecondStepMerge(tmpPath, outputPath, String::compareTo).sort();
        log.info("Finished second step: merge");
        if (Files.exists(tmpPath)) {
            log.info("Recursively removing {}", tmpPath.toAbsolutePath());
            Files.walk(tmpPath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }
}
