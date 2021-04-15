package com.example.elastic.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MyKeyTemp implements Serializable {
    private String user_id;
    private String url;
    private String time;
    private float total;
}
