package org.razkevich.quartz.web;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for handling pagination in REST controllers
 * Provides methods to paginate results and format response with pagination metadata
 */
public class PaginationUtils {

    /**
     * Paginate a list of items and return a formatted response with pagination metadata
     * This is applied after server-side filtering has been performed
     *
     * @param allItems The complete list of items to paginate
     * @param page The requested page number (0-based)
     * @param size The requested page size
     * @return A map containing the paginated items and pagination metadata
     */
    public static Map<String, Object> paginateResults(List<Map<String, Object>> allItems, int page, int size) {
        Map<String, Object> result = new HashMap<>();
        int totalItems = allItems.size();
        int totalPages = (int) Math.ceil((double) totalItems / size);
        
        // Ensure page is within bounds
        page = Math.max(0, Math.min(page, totalPages > 0 ? totalPages - 1 : 0));
        
        // Calculate start and end indices
        int startIndex = page * size;
        int endIndex = Math.min(startIndex + size, totalItems);
        
        // Get the items for the current page
        List<Map<String, Object>> items = (startIndex < totalItems) ?
                allItems.subList(startIndex, endIndex) : new ArrayList<>();
        
        // Build the result
        result.put("content", items);
        result.put("totalItems", totalItems);
        result.put("totalPages", totalPages);
        result.put("currentPage", page);
        result.put("pageSize", size);
        
        return result;
    }
    
    /**
     * Check if the requested page is valid for the given total number of items and page size
     *
     * @param totalItems Total number of items
     * @param page Requested page number (0-based)
     * @param size Requested page size
     * @return true if the page is valid, false otherwise
     */
    public static boolean isValidPage(int totalItems, int page, int size) {
        int totalPages = calculateTotalPages(totalItems, size);
        return page >= 0 && (totalPages == 0 || page < totalPages);
    }
    
    /**
     * Calculate the total number of pages for the given total number of items and page size
     *
     * @param totalItems Total number of items
     * @param size Page size
     * @return Total number of pages
     */
    public static int calculateTotalPages(int totalItems, int size) {
        return (int) Math.ceil((double) totalItems / size);
    }
}