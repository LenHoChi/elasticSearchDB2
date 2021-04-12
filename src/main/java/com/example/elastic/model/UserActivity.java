package com.example.elastic.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;

import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName="network_packet",shards = 2)
@Data
@AllArgsConstructor
@NoArgsConstructor
@RequiredArgsConstructor
public class UserActivity {

    @Id
    @NonNull
    private String id;
    @NonNull
    @Field(name="url")
    @JsonProperty("url")
    //@JsonProperty("message")
    private String url;
    @NonNull
    @Field(name="localdate")
    @JsonProperty("localdate")
    //@JsonProperty("@timestamp")
    //@Field(type = FieldType.Date, format = DateFormat.custom, pattern = "uuuu-MM-dd'T'HH:mm:ss.SSSZZ")
    private String time;
    @NonNull
    @JsonProperty("user_id")
    @Field(name="user_id")
    private String user_id;
    @Field(name="message")
    private String message;
    @Field(name = "@timestamp")
    @JsonProperty("@timestamp")
    private String timestamp;
    @JsonProperty("@version")
    private String version;
}
