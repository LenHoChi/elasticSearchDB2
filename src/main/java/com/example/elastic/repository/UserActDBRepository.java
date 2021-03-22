package com.example.elastic.repository;

import com.example.elastic.model.MyKey;
import com.example.elastic.model.UserActivityDB;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserActDBRepository extends JpaRepository<UserActivityDB, MyKey> {
}
