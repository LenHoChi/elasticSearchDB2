package com.example.elastic.controller;

import com.easyquartz.scheduler.ScheduleService;
import com.example.elastic.ElasticApplication;
import com.example.elastic.model.UserActivity;
import com.example.elastic.model.UserActivityDB;
import com.example.elastic.model.Users;
import com.example.elastic.repository.UserActDBRepository;
import com.example.elastic.repository.UserActRepository;
import com.example.elastic.repository.UsersRepository;
import com.example.elastic.service.UserActService;
import com.example.elastic.service.UserActService2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import static org.hibernate.tool.schema.SchemaToolingLogging.LOGGER;

@RestController
public class UserActController {

    @Autowired
    public UsersRepository usersRepository;
    @Autowired
    private UserActService userActService;
    @Autowired
    private UserActService2 userActService2;
    @Autowired
    private UserActRepository userActRepository;
    @Autowired
    private UserActDBRepository userActDBRepository;
    //----------------------------BEGIN---------------------------------------------------\
    //pb 1: group  {user_id, url}+query {date}--> find(use croll)
    @PostMapping("/pull-into-db")
    public boolean pullDataIntoDB(@RequestBody String time) throws IOException, ParseException {
        return userActService.mainProcess(time);
    }
    //pb 2: source {user_id} agg url & localdate
    @PostMapping("/pull-into-db2")
    public boolean pullDataIntoDB2(@RequestBody String time) throws IOException, ParseException {
        return userActService.mainProcessing(time);
    }
    //pb3 groupby {3 field}
    @PostMapping("/pull-into-db3")
    public boolean pullDataIntoDB3(@RequestBody String time) throws IOException, ParseException {
        return userActService.mainProcessingTerm(time);
    }
    //pb4 soruce {2 field} agg 1 field
    @GetMapping("/pull-into-db4")
    public void pullDataIntoDB4() throws IOException, ParseException {
        userActService.mainProcessing3("");
    }
    //pb5 nt but add for
    @GetMapping("/pull-into-db5")
    public void pullDataIntoDB5() throws IOException {
        userActService2.main2();
    }
}
