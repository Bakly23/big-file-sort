package ru.kolpakov.tasks.sort;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BigFileSortTest {

    private Path outputPath = Paths.get("target/output.txt");
    private Path tmpPath = Paths.get("target/tmp");

    @Before
    public void clean() throws IOException {
        if (Files.exists(tmpPath)) {
            Files.walk(tmpPath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        if (Files.exists(outputPath)) {
            Files.delete(outputPath);
        }
    }

    @Test
    public void testSort() throws IOException {
        Path inputPath = Paths.get("src/test/resources/input.txt");
        new BigFileSort(inputPath, tmpPath, outputPath, 100_000, 4).sort();
        List<String> sortedLines = new ArrayList<>(Files.readAllLines(outputPath));
        List<String> expectedSortedLines = Files.readAllLines(inputPath).stream().sorted().collect(Collectors.toCollection(ArrayList::new));
        assertEquals(expectedSortedLines, sortedLines);
    }
}
