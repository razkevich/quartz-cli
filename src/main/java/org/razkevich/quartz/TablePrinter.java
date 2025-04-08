package org.razkevich.quartz;

import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Utility for printing tables in a readable format
 */
public class TablePrinter {
    private static final int MAX_COLUMN_WIDTH = 25;  // Reduced from 30
    private static final int MIN_COLUMN_WIDTH = 6;   // Reduced from 8
    private static final int TERMINAL_WIDTH = 56;    // 70% of 80 (standard terminal width)
    private static int horizontalOffset = 0;

    /**
     * Print a table in a formatted way
     */
    public static void printTable(Table table) {
        if (table == null || table.isEmpty()) {
            System.out.println("No data to display");
            return;
        }

        // Calculate column widths
        List<Integer> columnWidths = calculateColumnWidths(table);
        int totalWidth = columnWidths.stream().mapToInt(Integer::intValue).sum() + (columnWidths.size() * 3) - 1;

        // Clear screen and move cursor to top
        System.out.print("\033[H\033[2J");
        
        // Print column headers
        printRow(table.columnNames(), columnWidths, true, horizontalOffset);
        
        // Print separator line
        printSeparator(columnWidths, horizontalOffset);
        
        // Print data rows
        for (int row = 0; row < table.rowCount(); row++) {
            List<String> rowData = new ArrayList<>();
            for (int col = 0; col < table.columnCount(); col++) {
                rowData.add(table.get(row, col).toString());
            }
            printRow(rowData, columnWidths, false, horizontalOffset);
        }

        // If table is wider than terminal, show scroll controls
        if (totalWidth > TERMINAL_WIDTH) {
            System.out.println("\nUse arrow keys to scroll horizontally (← →) or 'q' to quit");
            System.out.println("Total width: " + totalWidth + " characters, Current offset: " + horizontalOffset);
            
            // Handle keyboard input
            handleKeyboardInput(totalWidth);
        }
    }

    private static void handleKeyboardInput(int totalWidth) {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String input = scanner.nextLine().toLowerCase();
            if (input.equals("q")) {
                break;
            } else if (input.equals("\u001B[D")) { // Left arrow
                horizontalOffset = Math.max(0, horizontalOffset - 10);
                break;
            } else if (input.equals("\u001B[C")) { // Right arrow
                horizontalOffset = Math.min(totalWidth - TERMINAL_WIDTH, horizontalOffset + 10);
                break;
            }
        }
    }

    private static List<Integer> calculateColumnWidths(Table table) {
        List<Integer> widths = new ArrayList<>();
        
        // Calculate minimum required width for each column
        for (int i = 0; i < table.columnCount(); i++) {
            Column<?> column = table.column(i);
            int maxWidth = Math.max(
                column.name().length(),
                column.asList().stream()
                    .mapToInt(obj -> obj.toString().length())
                    .max()
                    .orElse(0)
            );
            
            // Apply width constraints
            int width = Math.min(Math.max(maxWidth, MIN_COLUMN_WIDTH), MAX_COLUMN_WIDTH);
            widths.add(width);
        }
        
        return widths;
    }

    private static void printRow(List<String> values, List<Integer> widths, boolean isHeader, int offset) {
        StringBuilder row = new StringBuilder();
        int currentPosition = 0;
        
        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            int width = widths.get(i);
            
            // Skip columns that are before the offset
            if (currentPosition < offset) {
                currentPosition += width + 3;
                continue;
            }
            
            // Stop if we've reached the terminal width
            if (currentPosition - offset >= TERMINAL_WIDTH) {
                break;
            }
            
            // Truncate or pad the value
            String formattedValue;
            if (value.length() > width) {
                formattedValue = value.substring(0, width - 3) + "...";
            } else {
                formattedValue = String.format("%-" + width + "s", value);
            }
            
            row.append("| ").append(formattedValue).append(" ");
            currentPosition += width + 3;
        }
        
        if (row.length() > 0) {
            row.append("|");
            
            // Print the row with appropriate styling
            if (isHeader) {
                System.out.println("\u001B[1m" + row + "\u001B[0m"); // Bold for headers
            } else {
                System.out.println(row);
            }
        }
    }

    private static void printSeparator(List<Integer> widths, int offset) {
        StringBuilder separator = new StringBuilder();
        int currentPosition = 0;
        
        for (int width : widths) {
            // Skip columns that are before the offset
            if (currentPosition < offset) {
                currentPosition += width + 3;
                continue;
            }
            
            // Stop if we've reached the terminal width
            if (currentPosition - offset >= TERMINAL_WIDTH) {
                break;
            }
            
            separator.append("+").append("-".repeat(width + 2));
            currentPosition += width + 3;
        }
        
        if (separator.length() > 0) {
            separator.append("+");
            System.out.println(separator);
        }
    }
} 