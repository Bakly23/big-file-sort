package ru.kolpakov.tasks.sort;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.stream.LongStream;

public class GenerateBigFile {

    private static final Random RANDOM = new Random();
    private static final String SYMBOLS_TO_GENERATE = "1234567890QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm";

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            throw new IllegalArgumentException("There must be 3 arguments: output path, number of lines and average line length");
        }
        Path outputPath = Paths.get(args[0]);
        long numberOfLines = Long.parseLong(args[1]);
        int avgLineLength = Integer.parseInt(args[2]);
        if(numberOfLines < 1) {
            throw new IllegalArgumentException("Number of lines must be positive integer number");
        }
        if(avgLineLength < 2) {
            throw new IllegalArgumentException("average line length must be at least 2");
        }
        Files.write(outputPath, (Iterable<String>) LongStream.range(0, numberOfLines)
                .mapToObj(i -> randomLine(generateRandom(avgLineLength)))::iterator);
    }

    private static int generateRandom(int avg) {
        return Math.max(1, avg + (int) (RANDOM.nextGaussian() * avg / 4));
    }

    private static String randomLine(int size) {
        char[] arr = new char[size];
        for (int i = 0; i < size; i++) {
            arr[i] = SYMBOLS_TO_GENERATE.charAt(RANDOM.nextInt(SYMBOLS_TO_GENERATE.length()));
        }
        return new String(arr);
    }
}
