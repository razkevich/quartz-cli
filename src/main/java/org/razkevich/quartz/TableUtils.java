package org.razkevich.quartz;

import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Unified utility class for table formatting and printing
 * This class consolidates functionality from TableFormatter and TablePrinter
 */
public class TableUtils {
    private static final int MAX_COLUMN_WIDTH = 25;
    private static final int MIN_COLUMN_WIDTH = 6;
    private static final int DEFAULT_TERMINAL_WIDTH = 80;
    private static int horizontalOffset = 0;
    
    /**
     * Configure table display settings
     */
    public static void configureTableDisplay() {
        // Set max column width to a large number to prevent truncation
        System.setProperty("tablesaw.max.column.width", "1000");
        // Set max rows to a large number to show all rows
        System.setProperty("tablesaw.max.rows", "1000000");
    }
    
    /**
     * Print a table in a formatted way
     */
    public static void printTable(Table table) {
        if (table == null || table.isEmpty()) {
            System.out.println("No data to display");
            return;
        }

        // Configure display settings
        configureTableDisplay();
        
        // Calculate column widths
        List<Integer> columnWidths = calculateColumnWidths(table);
        int totalWidth = columnWidths.stream().mapToInt(Integer::intValue).sum() + (columnWidths.size() * 3) - 1;

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
        int terminalWidth = getTerminalWidth();
        if (totalWidth > terminalWidth) {
            System.out.println("\nUse arrow keys to scroll horizontally (← →) or 'q' to quit");
            System.out.println("Total width: " + totalWidth + " characters, Current offset: " + horizontalOffset);
        }
    }
    
    /**
     * Format a table as a string
     */
    public static String formatTable(Table table) {
        if (table == null || table.isEmpty()) {
            return "No data to display";
        }
        
        // Configure display settings
        configureTableDisplay();
        
        // Get terminal width
        int terminalWidth = getTerminalWidth();
        if (terminalWidth <= 0) {
            terminalWidth = DEFAULT_TERMINAL_WIDTH; // Default fallback
        }
        
        // Calculate column widths
        List<Integer> columnWidths = calculateColumnWidths(table);
        
        // Build the table string
        StringBuilder sb = new StringBuilder();
        
        // Helper method to add horizontal line
        Runnable addHorizontalLine = () -> {
            sb.append("+");
            for (int width : columnWidths) {
                sb.append("-".repeat(width + 2)).append("+");
            }
            sb.append("\n");
        };
        
        // Top border
        addHorizontalLine.run();
        
        // Column names
        sb.append("|");
        for (int i = 0; i < table.columns().size(); i++) {
            String name = table.column(i).name();
            sb.append(" ").append(padOrWrap(name, columnWidths.get(i))).append(" |");
        }
        sb.append("\n");
        
        // Header separator
        addHorizontalLine.run();
        
        // Rows
        for (int row = 0; row < table.rowCount(); row++) {
            sb.append("|");
            for (int col = 0; col < table.columns().size(); col++) {
                String value = table.column(col).getString(row);
                sb.append(" ").append(padOrWrap(value, columnWidths.get(col))).append(" |");
            }
            sb.append("\n");
        }
        
        // Bottom border
        addHorizontalLine.run();
        
        return sb.toString();
    }

    /**
     * Calculate optimal column widths for a table
     */
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

    /**
     * Print a row of the table
     */
    private static void printRow(List<String> values, List<Integer> widths, boolean isHeader, int offset) {
        StringBuilder row = new StringBuilder();
        row.append("|");
        
        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            int width = widths.get(i);
            
            // Truncate or pad the value to fit the column width
            String formattedValue = padOrWrap(value, width);
            row.append(" ").append(formattedValue).append(" |");
        }
        
        System.out.println(row.toString());
    }

    /**
     * Print a separator line
     */
    private static void printSeparator(List<Integer> widths, int offset) {
        StringBuilder separator = new StringBuilder();
        separator.append("+");
        
        for (int width : widths) {
            separator.append("-".repeat(width + 2)).append("+");
        }
        
        System.out.println(separator.toString());
    }

    /**
     * Pad or wrap a string to fit within a maximum length
     */
    private static String padOrWrap(String str, int maxLength) {
        if (str == null) {
            str = "";
        }
        
        if (str.length() <= maxLength) {
            // Pad with spaces to fill the column width
            return str + " ".repeat(maxLength - str.length());
        } else {
            // Truncate and add ellipsis
            return str.substring(0, maxLength - 3) + "...";
        }
    }

    /**
     * Get the terminal width
     */
    private static int getTerminalWidth() {
        try {
            // Try to get terminal width from environment
            String columns = System.getenv("COLUMNS");
            if (columns != null && !columns.isEmpty()) {
                return Integer.parseInt(columns);
            }
            
            // Default fallback
            return DEFAULT_TERMINAL_WIDTH;
        } catch (Exception e) {
            return DEFAULT_TERMINAL_WIDTH;
        }
    }

    /**
     * Wrap a string into multiple lines to fit within a maximum length
     */
    private static List<String> wrapString(String str, int maxLength) {
        List<String> lines = new ArrayList<>();
        
        if (str == null || str.isEmpty()) {
            lines.add("");
            return lines;
        }
        
        if (str.length() <= maxLength) {
            lines.add(str);
            return lines;
        }
        
        // Split the string into words
        String[] words = str.split("\\s+");
        StringBuilder currentLine = new StringBuilder();
        
        for (String word : words) {
            // If adding this word would exceed the max length, start a new line
            if (currentLine.length() + word.length() + 1 > maxLength) {
                if (currentLine.length() > 0) {
                    lines.add(currentLine.toString());
                    currentLine = new StringBuilder();
                }
                
                // If the word itself is longer than maxLength, split it
                if (word.length() > maxLength) {
                    int start = 0;
                    while (start < word.length()) {
                        int end = Math.min(start + maxLength, word.length());
                        lines.add(word.substring(start, end));
                        start = end;
                    }
                } else {
                    currentLine.append(word);
                }
            } else {
                // Add a space before adding the word (if not the first word on the line)
                if (currentLine.length() > 0) {
                    currentLine.append(" ");
                }
                currentLine.append(word);
            }
        }
        
        // Add the last line if it's not empty
        if (currentLine.length() > 0) {
            lines.add(currentLine.toString());
        }
        
        return lines;
    }
}