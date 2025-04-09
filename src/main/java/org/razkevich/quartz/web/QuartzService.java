package org.razkevich.quartz.web;

import org.razkevich.quartz.QuartzConnectionService;
import org.razkevich.quartz.QuartzDataService;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service class for Quartz operations in the web application
 */
@Service
public class QuartzService {

    private final QuartzDataService quartzDataService;

    public QuartzService() {
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
     * Get a list of all jobs
     */
    public List<Map<String, Object>> listJobs(String groupFilter, String nameFilter) {
        return quartzDataService.listJobs(groupFilter, nameFilter);
    }
    
    /**
     * Get a paginated list of jobs
     */
    public Map<String, Object> listJobsPaginated(String groupFilter, String nameFilter, int page, int size) {
        List<Map<String, Object>> allJobs = quartzDataService.listJobs(groupFilter, nameFilter);
        return paginateResults(allJobs, page, size);
    }
    
    /**
     * Get a list of all triggers
     */
    public List<Map<String, Object>> listTriggers(String groupFilter, String nameFilter) {
        return quartzDataService.listTriggers(groupFilter, nameFilter);
    }
    
    /**
     * Get a paginated list of triggers
     */
    public Map<String, Object> listTriggersPaginated(String groupFilter, String nameFilter, int page, int size) {
        List<Map<String, Object>> allTriggers = quartzDataService.listTriggers(groupFilter, nameFilter);
        return paginateResults(allTriggers, page, size);
    }
    
    /**
     * Get a list of all running jobs
     */
    public List<Map<String, Object>> listRunningJobs() {
        return quartzDataService.listRunningJobs();
    }
    
    /**
     * Get a paginated list of running jobs
     */
    public Map<String, Object> listRunningJobsPaginated(int page, int size) {
        List<Map<String, Object>> allRunningJobs = quartzDataService.listRunningJobs();
        return paginateResults(allRunningJobs, page, size);
    }
    
    /**
     * Get a list of all paused trigger groups
     */
    public List<Map<String, Object>> listPausedTriggerGroups() {
        return quartzDataService.listPausedTriggerGroups();
    }
    
    /**
     * Get a paginated list of paused trigger groups
     */
    public Map<String, Object> listPausedTriggerGroupsPaginated(int page, int size) {
        List<Map<String, Object>> allPausedGroups = quartzDataService.listPausedTriggerGroups();
        return paginateResults(allPausedGroups, page, size);
    }
    
    /**
     * Get a list of all schedulers
     */
    public List<Map<String, Object>> listSchedulers() {
        return quartzDataService.listSchedulers();
    }
    
    /**
     * Get a paginated list of schedulers
     */
    public Map<String, Object> listSchedulersPaginated(int page, int size) {
        List<Map<String, Object>> allSchedulers = quartzDataService.listSchedulers();
        return paginateResults(allSchedulers, page, size);
    }
    
    /**
     * Helper method to paginate results
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
    
    /**
     * Get detailed information about a job
     */
    public Map<String, Object> getJobDetails(String group, String name) {
        return quartzDataService.getJobDetails(group, name);
    }
    
    /**
     * Get detailed information about a trigger
     */
    public Map<String, Object> getTriggerDetails(String group, String name) {
        return quartzDataService.getTriggerDetails(group, name);
    }
    
    /**
     * Delete a job and its triggers
     */
    public boolean deleteJob(String group, String name) {
        return quartzDataService.deleteJob(group, name);
    }
    
    /**
     * Delete a trigger
     */
    public boolean deleteTrigger(String group, String name) {
        return quartzDataService.deleteTrigger(group, name);
    }
    
    /**
     * Clear all Quartz tables
     */
    public boolean clearTables() {
        quartzDataService.clearTables();
        return true;
    }
}