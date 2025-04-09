package org.grid;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class GridGenerator {

    private static final char EMPTY_CELL = '\0';
    private static final Random random = new Random();
    private static final int MAX_PLACEMENT_ATTEMPTS = 100;

    private enum Direction {
        HORIZONTAL(0, 1), VERTICAL(1, 0), DIAG_DOWN_RIGHT(1, 1), DIAG_UP_RIGHT(-1, 1),
        INV_HORIZONTAL(0, -1), INV_VERTICAL(-1, 0), DIAG_UP_LEFT(-1, -1), DIAG_DOWN_LEFT(1, -1);

        final int dr, dc;

        Direction(int dr, int dc) {
            this.dr = dr;
            this.dc = dc;
        }

        static Direction getRandomDirection() {
            return values()[random.nextInt(values().length)];
        }
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: java GridGenerator <dictionary_path> <rows> <cols> <output_grid_path>");
            System.err.println("Example: java GridGenerator dictionary.txt 100 100 output_grid.txt");
            System.exit(1);
        }

        Path dictionaryPath;
        int rows;
        int cols;
        Path outputGridPath;

        try {
            dictionaryPath = Paths.get(args[0]);
            rows = Integer.parseInt(args[1]);
            cols = Integer.parseInt(args[2]);
            outputGridPath = Paths.get(args[3]);

            if (rows <= 0 || cols <= 0) {
                throw new NumberFormatException("Rows and columns must be positive.");
            }

            Path outputDir = outputGridPath.getParent();
            if (outputDir != null && (!Files.exists(outputDir) || !Files.isDirectory(outputDir) || !Files.isWritable(outputDir)) ) {
                if (!Files.exists(outputDir)) {
                    Files.createDirectories(outputDir);
                    System.out.println("Created output directory: " + outputDir.toAbsolutePath());
                } else if (!Files.isWritable(outputDir)) {
                    throw new IOException("Output directory is not writable: " + outputDir.toAbsolutePath());
                }
            } else if (outputDir == null) {
                Path currentDir = Paths.get(".");
                if (!Files.isWritable(currentDir)) {
                    throw new IOException("Current directory is not writable.");
                }
            }


        } catch (InvalidPathException | NumberFormatException e) {
            System.err.println("Error parsing arguments: " + e.getMessage());
            System.exit(1);
            return;
        } catch (IOException e) {
            System.err.println("Error checking/creating output directory: " + e.getMessage());
            System.exit(1);
            return;
        }


        try {
            System.out.println("Starting grid generation...");
            System.out.println("Dictionary: " + dictionaryPath.toAbsolutePath());
            System.out.println("Grid size: " + rows + "x" + cols);
            System.out.println("Output file: " + outputGridPath.toAbsolutePath());

            List<String> words = loadAndPrepareDictionary(dictionaryPath, Math.max(rows, cols));
            if (words.isEmpty()) {
                System.err.println("Dictionary is empty or no suitable words found. Exiting.");
                System.exit(1);
            }
            System.out.println("Loaded and prepared " + words.size() + " words.");

            // 2. Создать пустую матрицу
            char[][] grid = new char[rows][cols]; // Инициализируется '\0' (EMPTY_CELL)

            // 3. Попытаться разместить слова
            int wordsPlaced = placeWords(grid, words);
            System.out.println("Attempted to place words. Successfully placed: " + wordsPlaced);

            // 4. Заполнить пустые ячейки
            fillEmptyCells(grid);
            System.out.println("Filled empty cells with random letters.");

            // 5. Записать матрицу в файл
            writeGridToFile(grid, outputGridPath);
            System.out.println("Grid successfully generated and saved to " + outputGridPath.toAbsolutePath());

        } catch (IOException e) {
            System.err.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static List<String> loadAndPrepareDictionary(Path dictionaryPath, int maxWordLength) throws IOException {
        if (!Files.isReadable(dictionaryPath)) {
            throw new IOException("Dictionary file not found or not readable: " + dictionaryPath);
        }
        List<String> words;
        try (BufferedReader reader = Files.newBufferedReader(dictionaryPath, StandardCharsets.UTF_8)) {
            words = reader.lines()
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(String::toUpperCase)
                    .distinct()
                    .collect(Collectors.toList());
        }

        Collections.shuffle(words, random);
        return words;
    }

    private static int placeWords(char[][] grid, List<String> words) {
        int rows = grid.length;
        int cols = grid[0].length;
        int placedCount = 0;

        for (String word : words) {
            boolean placed = false;
            for (int attempt = 0; attempt < MAX_PLACEMENT_ATTEMPTS; attempt++) {
                int r = random.nextInt(rows);
                int c = random.nextInt(cols);
                Direction direction = Direction.getRandomDirection();

                if (canPlaceWord(grid, word, r, c, direction)) {
                    placeWord(grid, word, r, c, direction);
                    placed = true;
                    break; // Переходим к следующему слову
                }
            }
            if (placed) {
                placedCount++;
            } else {
                System.out.println("Warning: Could not place word '" + word + "' after " + MAX_PLACEMENT_ATTEMPTS + " attempts.");
            }
        }
        return placedCount;
    }

    private static boolean canPlaceWord(char[][] grid, String word, int r, int c, Direction direction) {
        int rows = grid.length;
        int cols = grid[0].length;

        for (int i = 0; i < word.length(); i++) {
            int currentR = r + i * direction.dr;
            int currentC = c + i * direction.dc;

            // 1. Проверка выхода за границы
            if (currentR < 0 || currentR >= rows || currentC < 0 || currentC >= cols) {
                return false;
            }

            // 2. Проверка конфликта с существующими буквами
            char existingChar = grid[currentR][currentC];
            char newChar = word.charAt(i);
            if (existingChar != EMPTY_CELL && existingChar != newChar) {
                return false; // Конфликт!
            }
        }
        // Если все проверки пройдены
        return true;
    }

    private static void placeWord(char[][] grid, String word, int r, int c, Direction direction) {
        for (int i = 0; i < word.length(); i++) {
            int currentR = r + i * direction.dr;
            int currentC = c + i * direction.dc;
            grid[currentR][currentC] = word.charAt(i);
        }
        // System.out.println("Placed '" + word + "' at (" + r + "," + c + ") direction " + direction); // Debug
    }

    private static void fillEmptyCells(char[][] grid) {
        int rows = grid.length;
        int cols = grid[0].length;

        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                if (grid[r][c] == EMPTY_CELL) {
                    grid[r][c] = (char) ('A' + random.nextInt(26)); // Случайная буква A-Z
                }
            }
        }
    }

    private static void writeGridToFile(char[][] grid, Path outputGridPath) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(outputGridPath, StandardCharsets.UTF_8)) {
            for (char[] row : grid) {
                writer.write(row); // Записываем массив символов строки
                writer.newLine();   // Добавляем перенос строки
            }
        }
    }
}