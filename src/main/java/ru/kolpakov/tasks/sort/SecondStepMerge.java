package ru.kolpakov.tasks.sort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

class SecondStepMerge {
    private static final Logger log = LoggerFactory.getLogger(SecondStepMerge.class);
    private final Path tmpPath;
    private final Path outputPath;
    private final Comparator<String> comparator;

    SecondStepMerge(Path tmpPath, Path outputPath, Comparator<String> comparator) {
        this.tmpPath = tmpPath;
        this.outputPath = outputPath;
        this.comparator = comparator;
    }

    void sort() throws IOException {
        List<Path> files = Files.list(Paths.get(tmpPath.toAbsolutePath().toString(), "first")).collect(Collectors.toList());
        log.info("Second step would merge {} sorted files", files.size());
        PriorityQueue<BufferAndLastLine> buffersSortedByLastLines = new PriorityQueue<>();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath.toFile()))) {
            for (Path path : files) {
                BufferAndLastLine ball = new BufferAndLastLine(new BufferedReader(new FileReader(path.toFile())));
                if (!ball.isEmpty()) {
                    buffersSortedByLastLines.add(ball);
                }
            }
            while (!buffersSortedByLastLines.isEmpty()) {
                BufferAndLastLine ball = buffersSortedByLastLines.poll();
                bw.write(ball.getLastLine());
                bw.write("\n");
                if (ball.isEmpty()) {
                    ball.close();
                } else {
                    buffersSortedByLastLines.add(ball);
                }
            }
        } finally {
            for (BufferAndLastLine ball : buffersSortedByLastLines) {
                try {
                    ball.close();
                } catch (IOException e) {
                    log.warn("Buffer {} was not closed: error occurred", ball.br, e);
                }
            }

        }
    }

    private class BufferAndLastLine implements Comparable<BufferAndLastLine>, Closeable {
        final BufferedReader br;
        String lastLine;

        private BufferAndLastLine(BufferedReader br) throws IOException {
            this.br = br;
            read();
        }

        boolean isEmpty() {
            return lastLine == null;
        }

        String getLastLine() throws IOException {
            String answer = lastLine;
            read();
            return answer;
        }

        private void read() throws IOException {
            lastLine = br.readLine();
        }


        @Override
        public int compareTo(BufferAndLastLine o) {
            return comparator.compare(lastLine, o.lastLine);
        }

        @Override
        public void close() throws IOException {
            if (br != null) {
                br.close();
            }
        }
    }
}
