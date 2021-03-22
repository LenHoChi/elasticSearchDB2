package com.example.elastic.controller;

import com.example.elastic.model.UserActivity;
import com.example.elastic.repository.UserActDBRepository;
import com.example.elastic.repository.UserActRepository;
import com.example.elastic.service.UserActService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
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
        return userActRepository.findAll();
    }
    @GetMapping("/pull-into-db")
    public boolean pullDataIntoDB(@RequestBody String dateNow) throws IOException {
        return userActService.mainProcessing(dateNow);
    }
    @GetMapping("/find-by-url")
    public Iterable<UserActivity> findByUrl(@RequestBody String url) {
        return userActRepository.findByUrl(url);
    }
    @GetMapping("/group-by-field")
    public List<String> groupByField() throws IOException {
        return userActService.groupByField();
    }
    @GetMapping("find-by-message-date")
    public List<UserActivity> solve2(@RequestBody String message) {
        return userActService.findByMessageDate(userActService.splitHeadTail(message),"2021-03-19","PC-LenHo");
    }
    @PostMapping("/saveAll")
    public boolean saveAll() {
        return userActService.saveAll();
    }
}
