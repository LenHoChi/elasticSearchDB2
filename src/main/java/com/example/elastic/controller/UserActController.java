package com.example.elastic.controller;

import com.easyquartz.scheduler.ScheduleService;
import com.example.elastic.ElasticApplication;
import com.example.elastic.model.UserActivity;
import com.example.elastic.model.UserActivityDB;
import com.example.elastic.repository.UserActDBRepository;
import com.example.elastic.repository.UserActRepository;
import com.example.elastic.service.UserActService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;
@RestController
public class UserActController {

    @Autowired
    private UserActService userActService;
    @Autowired
    private UserActRepository userActRepository;
    @Autowired
    private UserActDBRepository userActDBRepository;
    @GetMapping("/find-all")
    public Iterable<UserActivity> findAllUsers() {
        return userActService.findAll();
    }
    @GetMapping("/find-all-db")
    public List<UserActivityDB> findAllUsersDB() {
        return userActDBRepository.findAll();
    }
    @GetMapping("/find-by-url")
    public List<UserActivity> findByUrl(@RequestBody String url) {
        return userActService.findByUrl(url);
    }
    @GetMapping("/group-by-field")
    public List<String> groupByField() throws IOException {
        return userActService.groupByField();
    }
    @GetMapping("find-by-field")
    public List<UserActivity> solve2(@RequestBody String message) throws IOException {
        return userActService.findByField2(userActService.splitHeadTail(message),"2021-03-30","2021-03-31","PC-QUANPHAM$");
    }
    @PostMapping("/saveAll")
    public boolean saveAll() {
        return userActService.saveAll();
    }
    @PostMapping("/pull-into-db")
    public boolean pullDataIntoDB(@RequestBody String time) throws IOException {
        return userActService.mainProcessing(time);
    }
    @PostMapping("/pull-into-db2")
    public boolean pullDataIntoDB2(@RequestBody String time) throws IOException, ParseException {
        return userActService.mainProcessing2(time);
    }
}
