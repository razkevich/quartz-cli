package org.razkevich.quartz.web;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST controller for Quartz operations
 */
@RestController
@RequestMapping("/api/quartz")
@CrossOrigin(origins = "*")
public class QuartzController {

    private final QuartzService quartzService;

    @Autowired
    public QuartzController(QuartzService quartzService) {
        this.quartzService = quartzService;
    }

    @GetMapping("/jobs")
    public ResponseEntity<Map<String, Object>> listJobs(
            @RequestParam(required = false) String group,
            @RequestParam(required = false) String name,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return ResponseEntity.ok(quartzService.listJobsPaginated(group, name, page, size));
    }

    @GetMapping("/triggers")
    public ResponseEntity<Map<String, Object>> listTriggers(
            @RequestParam(required = false) String group,
            @RequestParam(required = false) String name,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return ResponseEntity.ok(quartzService.listTriggersPaginated(group, name, page, size));
    }

    @GetMapping("/running-jobs")
    public ResponseEntity<Map<String, Object>> listRunningJobs(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return ResponseEntity.ok(quartzService.listRunningJobsPaginated(page, size));
    }

    @GetMapping("/paused-groups")
    public ResponseEntity<Map<String, Object>> listPausedTriggerGroups(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return ResponseEntity.ok(quartzService.listPausedTriggerGroupsPaginated(page, size));
    }

    @GetMapping("/schedulers")
    public ResponseEntity<Map<String, Object>> listSchedulers(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return ResponseEntity.ok(quartzService.listSchedulersPaginated(page, size));
    }

    @GetMapping("/jobs/{group}/{name}")
    public ResponseEntity<Map<String, Object>> getJobDetails(
            @PathVariable String group,
            @PathVariable String name) {
        return ResponseEntity.ok(quartzService.getJobDetails(group, name));
    }

    @GetMapping("/triggers/{group}/{name}")
    public ResponseEntity<Map<String, Object>> getTriggerDetails(
            @PathVariable String group,
            @PathVariable String name) {
        return ResponseEntity.ok(quartzService.getTriggerDetails(group, name));
    }

    @DeleteMapping("/jobs/{group}/{name}")
    public ResponseEntity<Map<String, Object>> deleteJob(
            @PathVariable String group,
            @PathVariable String name) {
        boolean success = quartzService.deleteJob(group, name);
        Map<String, Object> response = new HashMap<>();
        response.put("success", success);
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/triggers/{group}/{name}")
    public ResponseEntity<Map<String, Object>> deleteTrigger(
            @PathVariable String group,
            @PathVariable String name) {
        boolean success = quartzService.deleteTrigger(group, name);
        Map<String, Object> response = new HashMap<>();
        response.put("success", success);
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/clear")
    public ResponseEntity<Map<String, Object>> clearTables() {
        boolean success = quartzService.clearTables();
        Map<String, Object> response = new HashMap<>();
        response.put("success", success);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getConnectionInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("url", System.getProperty("quartz.jdbc.url", "Not configured"));
        info.put("driver", System.getProperty("quartz.jdbc.driver", "Not configured"));
        info.put("schema", System.getProperty("quartz.jdbc.schema", "Default"));
        info.put("tablePrefix", System.getProperty("quartz.table.prefix", "qrtz_"));
        return ResponseEntity.ok(info);
    }
}