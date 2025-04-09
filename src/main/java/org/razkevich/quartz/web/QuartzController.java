package org.razkevich.quartz.web;

import org.razkevich.quartz.QuartzConnectionService;
import org.razkevich.quartz.QuartzDataService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST controller for Quartz operations
 * All filtering operations are performed server-side at the database level
 */
@RestController
@RequestMapping("/api/quartz")
@CrossOrigin(origins = "*")
public class QuartzController {

    private final QuartzDataService quartzDataService;

    public QuartzController() {
        // Get connection parameters from system properties set by QuartzCli
        String jdbcUrl = System.getProperty("quartz.jdbc.url");
        String username = System.getProperty("quartz.jdbc.username");
        String password = System.getProperty("quartz.jdbc.password");
        String driver = System.getProperty("quartz.jdbc.driver");
        String schema = System.getProperty("quartz.jdbc.schema");
        String tablePrefix = System.getProperty("quartz.table.prefix", "qrtz_");
        String schedulerName = System.getProperty("quartz.scheduler.name");
        
        DataSource dataSource = QuartzConnectionService.createDataSource(
            jdbcUrl, username, password, driver, schema);
            
        this.quartzDataService = new QuartzDataService(dataSource, schema, tablePrefix, schedulerName);
    }

    /**
     * List jobs with server-side filtering and pagination
     * 
     * @param group Optional group filter (server-side filtering)
     * @param name Optional name filter (server-side filtering)
     * @param page Page number for pagination
     * @param size Page size for pagination
     * @return Paginated and filtered job list
     */
    @GetMapping("/jobs")
    public ResponseEntity<Map<String, Object>> listJobs(
            @RequestParam(required = false) String group,
            @RequestParam(required = false) String name,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        // Server-side filtering is applied by passing filter parameters directly to the service
        List<Map<String, Object>> allJobs = quartzDataService.listJobs(group, name);
        return ResponseEntity.ok(paginateResults(allJobs, page, size));
    }

    /**
     * List triggers with server-side filtering and pagination
     * 
     * @param group Optional group filter (server-side filtering)
     * @param name Optional name filter (server-side filtering)
     * @param page Page number for pagination
     * @param size Page size for pagination
     * @return Paginated and filtered trigger list
     */
    @GetMapping("/triggers")
    public ResponseEntity<Map<String, Object>> listTriggers(
            @RequestParam(required = false) String group,
            @RequestParam(required = false) String name,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        // Server-side filtering is applied by passing filter parameters directly to the service
        List<Map<String, Object>> allTriggers = quartzDataService.listTriggers(group, name);
        return ResponseEntity.ok(paginateResults(allTriggers, page, size));
    }

    /**
     * List running jobs with server-side filtering and pagination
     * 
     * @param group Optional group filter (server-side filtering)
     * @param name Optional name filter (server-side filtering)
     * @param page Page number for pagination
     * @param size Page size for pagination
     * @return Paginated and filtered running job list
     */
    @GetMapping("/running-jobs")
    public ResponseEntity<Map<String, Object>> listRunningJobs(
            @RequestParam(required = false) String group,
            @RequestParam(required = false) String name,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        // Server-side filtering is applied by passing filter parameters directly to the service
        List<Map<String, Object>> allRunningJobs = quartzDataService.listRunningJobs(group, name);
        return ResponseEntity.ok(paginateResults(allRunningJobs, page, size));
    }

    /**
     * List paused trigger groups with server-side filtering and pagination
     * 
     * @param group Optional group filter (server-side filtering)
     * @param page Page number for pagination
     * @param size Page size for pagination
     * @return Paginated and filtered paused trigger group list
     */
    @GetMapping("/paused-groups")
    public ResponseEntity<Map<String, Object>> listPausedTriggerGroups(
            @RequestParam(required = false) String group,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        // Server-side filtering is applied by passing filter parameters directly to the service
        List<Map<String, Object>> allPausedGroups = quartzDataService.listPausedTriggerGroups(group);
        return ResponseEntity.ok(paginateResults(allPausedGroups, page, size));
    }

    /**
     * List schedulers with pagination
     * 
     * @param page Page number for pagination
     * @param size Page size for pagination
     * @return Paginated scheduler list
     */
    @GetMapping("/schedulers")
    public ResponseEntity<Map<String, Object>> listSchedulers(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        List<Map<String, Object>> allSchedulers = quartzDataService.listSchedulers();
        return ResponseEntity.ok(paginateResults(allSchedulers, page, size));
    }

    /**
     * Get detailed information about a specific job
     * 
     * @param group Job group name
     * @param name Job name
     * @return Job details
     */
    @GetMapping("/jobs/{group}/{name}")
    public ResponseEntity<Map<String, Object>> getJobDetails(
            @PathVariable String group,
            @PathVariable String name) {
        return ResponseEntity.ok(quartzDataService.getJobDetails(group, name));
    }

    /**
     * Get detailed information about a specific trigger
     * 
     * @param group Trigger group name
     * @param name Trigger name
     * @return Trigger details
     */
    @GetMapping("/triggers/{group}/{name}")
    public ResponseEntity<Map<String, Object>> getTriggerDetails(
            @PathVariable String group,
            @PathVariable String name) {
        return ResponseEntity.ok(quartzDataService.getTriggerDetails(group, name));
    }

    /**
     * Delete a job
     * 
     * @param group Job group name
     * @param name Job name
     * @return Success status
     */
    @DeleteMapping("/jobs/{group}/{name}")
    public ResponseEntity<Map<String, Object>> deleteJob(
            @PathVariable String group,
            @PathVariable String name) {
        boolean success = quartzDataService.deleteJob(group, name);
        Map<String, Object> response = new HashMap<>();
        response.put("success", success);
        return ResponseEntity.ok(response);
    }

    /**
     * Delete a trigger
     * 
     * @param group Trigger group name
     * @param name Trigger name
     * @return Success status
     */
    @DeleteMapping("/triggers/{group}/{name}")
    public ResponseEntity<Map<String, Object>> deleteTrigger(
            @PathVariable String group,
            @PathVariable String name) {
        boolean success = quartzDataService.deleteTrigger(group, name);
        Map<String, Object> response = new HashMap<>();
        response.put("success", success);
        return ResponseEntity.ok(response);
    }

    /**
     * Clear all Quartz tables
     * 
     * @return Success status
     */
    @DeleteMapping("/clear")
    public ResponseEntity<Map<String, Object>> clearTables() {
        try {
            quartzDataService.clearTables();
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            return ResponseEntity.ok(response);
        }
    }

    /**
     * Get connection information
     * 
     * @return Connection details
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getConnectionInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("url", System.getProperty("quartz.jdbc.url", "Not configured"));
        info.put("driver", System.getProperty("quartz.jdbc.driver", "Not configured"));
        info.put("schema", System.getProperty("quartz.jdbc.schema", "Default"));
        info.put("tablePrefix", System.getProperty("quartz.table.prefix", "qrtz_"));
        return ResponseEntity.ok(info);
    }
    
    /**
     * Helper method to paginate results
     * This is applied after server-side filtering has been performed
     */
    private Map<String, Object> paginateResults(List<Map<String, Object>> allItems, int page, int size) {
        Map<String, Object> result = new HashMap<>();
        int totalItems = allItems.size();
        int totalPages = (int) Math.ceil((double) totalItems / size);
        
        // Ensure page is within bounds
        page = Math.max(0, Math.min(page, totalPages - 1));
        
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
}