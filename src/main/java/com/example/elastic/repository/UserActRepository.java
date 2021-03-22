package com.example.elastic.repository;

import com.example.elastic.model.UserActivity;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface UserActRepository extends ElasticsearchRepository<UserActivity, String>{
    List<UserActivity> findByUrl(String message);
}