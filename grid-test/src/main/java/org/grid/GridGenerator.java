package org.grid;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*; // Импортируем нужные коллекции и утилиты
import java.util.stream.Collectors;
import java.util.stream.Stream; // Для Files.lines

public class GridGenerator {

    private static final char EMPTY_CELL = '\0';
    private static Random random;

    private static final int MAX_TOTAL_PLACEMENT_ATTEMPTS = 30000; // Увеличил значение для большей плотности

    private static class DictionaryData {
        final List<String> originalWords;
        final List<Character> alphabet;

        DictionaryData(List<String> words, Set<Character> alphabetSet) {
            this.originalWords = Collections.unmodifiableList(new ArrayList<>(words)); // Неизменяемая копия
            this.alphabet = new ArrayList<>(alphabetSet);
            Collections.sort(this.alphabet);

            if (this.alphabet.isEmpty()) {
                System.err.println("Warning: Dictionary processing resulted in an empty alphabet. Falling back to default A-Z.");
                for (char c = 'A'; c <= 'Z'; c++) {
                    this.alphabet.add(c);
                }
            }
        }
    }

    private enum Direction {
        HORIZONTAL(0, 1), VERTICAL(1, 0), DIAG_DOWN_RIGHT(1, 1), DIAG_UP_RIGHT(-1, 1),
        INV_HORIZONTAL(0, -1), INV_VERTICAL(-1, 0), DIAG_UP_LEFT(-1, -1), DIAG_DOWN_LEFT(1, -1);

        final int dr, dc;

        Direction(int dr, int dc) {
            this.dr = dr;
            this.dc = dc;
        }

        static Direction getRandomDirection(Random rand) {
            return values()[rand.nextInt(values().length)];
        }
    }

    public static void main(String[] args) {
        if (args.length < 4 || args.length > 5) {
            System.err.println("Usage: java GridGenerator <dictionary_path> <rows> <cols> <output_grid_path> [random_seed]");
            System.err.println("Example: java GridGenerator dictionary.txt 50 50 output_grid.txt");
            System.err.println("Example (deterministic): java GridGenerator dictionary.txt 30 30 grid.txt 12345");
            System.exit(1);
        }

        Path dictionaryPath;
        int rows;
        int cols;
        Path outputGridPath;
        long randomSeed = -1;

        try {
            dictionaryPath = Paths.get(args[0]);
            rows = Integer.parseInt(args[1]);
            cols = Integer.parseInt(args[2]);
            outputGridPath = Paths.get(args[3]);

            if (rows <= 0 || cols <= 0) {
                throw new NumberFormatException("Rows and columns must be positive.");
            }

            if (args.length == 5) {
                try {
                    randomSeed = Long.parseLong(args[4]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid random seed provided. It must be a long integer.");
                    System.exit(1);
                }
            }

            if (randomSeed != -1) {
                random = new Random(randomSeed);
                System.out.println("Using provided random seed: " + randomSeed);
            } else {
                random = new Random(); // Используем системное время
                System.out.println("Using system time for random seed.");
            }

            Path outputDir = outputGridPath.getParent();
            if (outputDir != null) {
                if (!Files.exists(outputDir)) {
                    System.out.println("Output directory does not exist, creating: " + outputDir.toAbsolutePath());
                    Files.createDirectories(outputDir);
                } else if (!Files.isDirectory(outputDir)) {
                    throw new IOException("Output path's parent exists but is not a directory: " + outputDir.toAbsolutePath());
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
        } catch (SecurityException e) {
            System.err.println("Security error accessing paths or creating directories: " + e.getMessage());
            System.exit(1);
            return;
        }

        try {
            System.out.println("Starting grid generation...");
            System.out.println("Dictionary: " + dictionaryPath.toAbsolutePath());
            System.out.println("Grid size: " + rows + "x" + cols);
            System.out.println("Output file: " + outputGridPath.toAbsolutePath());

            DictionaryData dictionaryData = loadAndPrepareDictionary(dictionaryPath, rows, cols);
            if (dictionaryData.originalWords.isEmpty()) {
                System.err.println("Dictionary is empty or no suitable words found (after filtering). Exiting.");
                System.exit(1);
            }
            System.out.println("Loaded and prepared " + dictionaryData.originalWords.size() + " suitable words.");
            System.out.println("Detected alphabet size: " + dictionaryData.alphabet.size());

            List<String> availableWords = dictionaryData.originalWords;
            char[][] grid = new char[rows][cols];

            System.out.println("Attempting to place words repeatedly (max " + MAX_TOTAL_PLACEMENT_ATTEMPTS + " total attempts)...");
            int successfulPlacements = placeWordsRepeatedly(grid, availableWords);
            System.out.println("Successfully placed " + successfulPlacements + " word instances.");

            if (successfulPlacements == 0 && !availableWords.isEmpty()) {
                System.err.println("Warning: Could not place any word instances onto the grid. Check grid size, word lengths, or increase MAX_TOTAL_PLACEMENT_ATTEMPTS.");
            }

            long filledByWords = 0;
            for (char[] rowArray : grid) {
                for (char cell : rowArray) {
                    if (cell != EMPTY_CELL) {
                        filledByWords++;
                    }
                }
            }
            System.out.printf("Cells filled by words: %d (%.2f%% of total %d cells)\n",
                    filledByWords, filledByWords > 0 ? (double) filledByWords * 100 / (rows * cols) : 0.0, rows * cols);

            fillEmptyCells(grid, dictionaryData.alphabet);
            System.out.println("Filled empty cells with random letters from the dictionary's alphabet.");

            writeGridToFile(grid, outputGridPath);
            System.out.println("Grid successfully generated and saved to " + outputGridPath.toAbsolutePath());

        } catch (IOException e) {
            System.err.println("An I/O error occurred during grid generation or writing: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("An unexpected error occurred during grid generation: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static DictionaryData loadAndPrepareDictionary(Path dictionaryPath, int rows, int cols) throws IOException {
        if (!Files.isReadable(dictionaryPath)) {
            throw new IOException("Dictionary file not found or not readable: " + dictionaryPath.toAbsolutePath());
        }

        Set<Character> alphabet = new HashSet<>();
        List<String> words;
        int maxGridDimension = Math.max(rows, cols);

        try (Stream<String> lines = Files.lines(dictionaryPath, StandardCharsets.UTF_8)) {
            words = lines
                    .parallel()
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(String::toUpperCase) // К верхнему регистру
                    .filter(s -> s.length() >= 2 && s.length() <= maxGridDimension)
                    .distinct()
                    .collect(Collectors.toList());

            if (!words.isEmpty()) {
                words.forEach(word -> {
                    for (char c : word.toCharArray()) {
                        alphabet.add(c);
                    }
                });
            }

        } catch (IOException e) {
            throw new IOException("Error reading dictionary file: " + dictionaryPath.toAbsolutePath(), e);
        }

        return new DictionaryData(words, alphabet);
    }

    private static int placeWordsRepeatedly(char[][] grid, List<String> availableWords) {
        if (availableWords.isEmpty()) {
            System.out.println("No words available to place.");
            return 0;
        }

        int rows = grid.length;
        int cols = grid[0].length;
        int successfulPlacementsCount = 0;
        int totalAttemptsMade = 0;

        while (totalAttemptsMade < MAX_TOTAL_PLACEMENT_ATTEMPTS) {
            totalAttemptsMade++;

            String wordToTry = availableWords.get(random.nextInt(availableWords.size()));

            int r = random.nextInt(rows);
            int c = random.nextInt(cols);

            Direction direction = Direction.getRandomDirection(random);

            if (canPlaceWord(grid, wordToTry, r, c, direction)) {
                placeWord(grid, wordToTry, r, c, direction);
                successfulPlacementsCount++;
            }

            if (MAX_TOTAL_PLACEMENT_ATTEMPTS >= 10 && totalAttemptsMade % (MAX_TOTAL_PLACEMENT_ATTEMPTS / 10) == 0) {
                System.out.printf("... Placement attempts made: %d/%d, Successful placements: %d\n",
                        totalAttemptsMade, MAX_TOTAL_PLACEMENT_ATTEMPTS, successfulPlacementsCount);
            }
        }

        System.out.println("Finished placement phase after " + totalAttemptsMade + " total attempts.");
        return successfulPlacementsCount;
    }

    private static boolean canPlaceWord(char[][] grid, String word, int r, int c, Direction direction) {
        int rows = grid.length;
        int cols = grid[0].length;
        int wordLen = word.length();

        int endR = r + (wordLen - 1) * direction.dr;
        int endC = c + (wordLen - 1) * direction.dc;
        if (endR < 0 || endR >= rows || endC < 0 || endC >= cols) {
            return false;
        }

        for (int i = 0; i < wordLen; i++) {
            int currentR = r + i * direction.dr;
            int currentC = c + i * direction.dc;

            if (currentR < 0 || currentR >= rows || currentC < 0 || currentC >= cols) {
                return false;
            }

            char existingChar = grid[currentR][currentC];
            char newChar = word.charAt(i);
            if (existingChar != EMPTY_CELL && existingChar != newChar) {
                return false;
            }
        }
        return true;
    }

    private static void placeWord(char[][] grid, String word, int r, int c, Direction direction) {
        for (int i = 0; i < word.length(); i++) {
            int currentR = r + i * direction.dr;
            int currentC = c + i * direction.dc;
            grid[currentR][currentC] = word.charAt(i);
        }
    }

    private static void fillEmptyCells(char[][] grid, List<Character> alphabet) {
        int rows = grid.length;
        int cols = grid[0].length;

        if (alphabet.isEmpty()) {
            System.err.println("Critical Error: Alphabet is empty, cannot fill empty cells meaningfully. Filling with '?'.");
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    if (grid[r][c] == EMPTY_CELL) {
                        grid[r][c] = '?';
                    }
                }
            }
            return;
        }

        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                if (grid[r][c] == EMPTY_CELL) {
                    grid[r][c] = alphabet.get(random.nextInt(alphabet.size())); // Используем наш Random
                }
            }
        }
    }

    private static void writeGridToFile(char[][] grid, Path outputGridPath) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(outputGridPath, StandardCharsets.UTF_8)) {
            for (char[] row : grid) {
                writer.write(row);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new IOException("Error writing grid to file: " + outputGridPath.toAbsolutePath(), e);
        } catch (SecurityException e) {
            throw new SecurityException("Security error writing grid to file: " + outputGridPath.toAbsolutePath(), e);
        }
    }
}