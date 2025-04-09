package org.razkevich.quartz;

import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import java.util.List;
import java.util.ArrayList;

public class TableFormatter {
    
    public static void configureTableDisplay() {
        // Set max column width to a large number to prevent truncation
        System.setProperty("tablesaw.max.column.width", "1000");
        // Set max rows to a large number to show all rows
        System.setProperty("tablesaw.max.rows", "1000000");
    }
    
    public static String formatTable(Table table) {
        // Configure display settings
        configureTableDisplay();
        
        // Get terminal width
        int terminalWidth = getTerminalWidth();
        if (terminalWidth <= 0) {
            terminalWidth = 80; // Default fallback
        }
        
        // Calculate column widths
        List<Integer> columnWidths = new ArrayList<>();
        int totalWidth = 0;
        
        // First pass: calculate minimum widths based on content
        for (Column<?> column : table.columns()) {
            int width = Math.max(
                column.name().length(),
                column.getString(0).length()
            );
            for (int i = 1; i < table.rowCount(); i++) {
                width = Math.max(width, column.getString(i).length());
            }
            columnWidths.add(width);
            totalWidth += width + 3; // +3 for borders and padding
        }
        
        // If table is wider than terminal, adjust column widths
        if (totalWidth > terminalWidth) {
            int excess = totalWidth - terminalWidth;
            int avgReduction = excess / columnWidths.size();
            
            // Reduce each column width proportionally
            for (int i = 0; i < columnWidths.size(); i++) {
                columnWidths.set(i, Math.max(10, columnWidths.get(i) - avgReduction));
            }
        }
        
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
            List<String> wrappedLines = new ArrayList<>();
            int maxLines = 1;
            
            // First pass: wrap all cells and find max lines
            for (int col = 0; col < table.columns().size(); col++) {
                String value = table.column(col).getString(row);
                List<String> lines = wrapString(value, columnWidths.get(col));
                wrappedLines.addAll(lines);
                maxLines = Math.max(maxLines, lines.size());
            }
            
            // Print each line of wrapped cells
            for (int line = 0; line < maxLines; line++) {
                sb.append("|");
                for (int col = 0; col < table.columns().size(); col++) {
                    List<String> lines = wrapString(table.column(col).getString(row), columnWidths.get(col));
                    String cellLine = line < lines.size() ? lines.get(line) : "";
                    sb.append(" ").append(padOrWrap(cellLine, columnWidths.get(col))).append(" |");
                }
                sb.append("\n");
            }
            
            // Add separator after each row
            addHorizontalLine.run();
        }
        
        return sb.toString();
    }
    
    private static List<String> wrapString(String str, int maxLength) {
        List<String> lines = new ArrayList<>();
        if (str == null || str.isEmpty()) {
            lines.add("");
            return lines;
        }
        
        String[] words = str.split("\\s+");
        StringBuilder currentLine = new StringBuilder();
        
        for (String word : words) {
            if (currentLine.length() + word.length() + 1 <= maxLength) {
                if (currentLine.length() > 0) {
                    currentLine.append(" ");
                }
                currentLine.append(word);
            } else {
                if (currentLine.length() > 0) {
                    lines.add(currentLine.toString());
                    currentLine = new StringBuilder();
                }
                // Handle words longer than maxLength
                while (word.length() > maxLength) {
                    lines.add(word.substring(0, maxLength));
                    word = word.substring(maxLength);
                }
                currentLine.append(word);
            }
        }
        
        if (currentLine.length() > 0) {
            lines.add(currentLine.toString());
        }
        
        return lines;
    }
    
    private static String padOrWrap(String str, int maxLength) {
        if (str == null) {
            return " ".repeat(maxLength);
        }
        if (str.length() <= maxLength) {
            return str + " ".repeat(maxLength - str.length());
        }
        return str.substring(0, maxLength);
    }
    
    private static int getTerminalWidth() {
        try {
            String columns = System.getenv("COLUMNS");
            if (columns != null) {
                return Integer.parseInt(columns);
            }
            return 80; // Default fallback
        } catch (Exception e) {
            return 80; // Default fallback
        }
    }
}