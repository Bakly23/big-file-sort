package ru.kolpakov.tasks.sort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            throw new IllegalArgumentException("there must be 3 arguments: input path, tmp directory and output path");
        }
        Path inputPath = Paths.get(args[0]);
        Path tmpPath = Paths.get(args[1]);
        Path outputPath = Paths.get(args[2]);
        if (!Files.exists(inputPath) || !Files.isRegularFile(inputPath)) {
            throw new IllegalArgumentException(String.format("Input path %s must exist and be a regular file.", inputPath));
        }
        if (Files.exists(tmpPath)) {
            throw new IllegalArgumentException(String.format("Tmp path %s must not exist.", tmpPath));
        }
        if (Files.exists(outputPath)) {
            throw new IllegalArgumentException(String.format("Output path %s must not exist.", outputPath));
        }
        long maxMemory = Runtime.getRuntime().maxMemory();
        int parallelism = Runtime.getRuntime().availableProcessors();
        log.info("Input path is {}; tmp path is {}; output path is {}", inputPath.toAbsolutePath().toString(),
                tmpPath.toAbsolutePath().toString(), outputPath.toAbsolutePath().toString());
        log.info("Max memory is {}; level of parallelism is {}", maxMemory, parallelism);
        new BigFileSort(inputPath, tmpPath, outputPath, maxMemory, parallelism).sort();

    }
}
