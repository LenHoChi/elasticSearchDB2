package com.example.elastic.service;

import com.example.elastic.configuration.DBConfig;
import com.example.elastic.convert.ToDB;
import com.example.elastic.model.*;
import com.example.elastic.repository.UserActDBRepository;
import com.example.elastic.repository.UserActRepository;
import com.example.elastic.repository.UsersRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.catalina.User;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.hibernate.tool.schema.SchemaToolingLogging;
import org.jboss.logging.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hibernate.tool.schema.SchemaToolingLogging.LOGGER;

@Transactional
@Service
public class UserActService implements Job {
    private final ElasticsearchOperations elasticsearchOperations;
    private final String index = "network_packet";

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    @Autowired
    UsersRepository usersRepository;
    @Autowired
    DBConfig dbConfig;
    @Autowired
    UserActRepository userActRepository;
    @Autowired
    UserActDBRepository userActDBRepository;

    @Autowired
    public UserActService(final ElasticsearchOperations elasticsearchOperations) {
        super();
        this.elasticsearchOperations = elasticsearchOperations;
    }


    public float getTotalTime(String pcName, String url, String date) {
        Optional<UserActivityDB> lstUser = userActDBRepository.findById(new MyKey(pcName, url, date));
        List<UserActivityDB> lstUserR = lstUser.stream().collect(Collectors.toList());
        for (int i = 0; i < lstUser.stream().count(); i++) {
            if (lstUserR.get(i).getUrl().equals(url) && lstUserR.get(i).getTime().equals(date)) {
                return lstUserR.get(i).getTotal_time();
            }
        }
        return 0;
    }

    public int getCount(String pcName, String url, String date) {
        Optional<UserActivityDB> lstUser = userActDBRepository.findById(new MyKey(pcName, url, date));
        List<UserActivityDB> lstUserR = lstUser.stream().collect(Collectors.toList());
        for (int i = 0; i < lstUser.stream().count(); i++) {
            if (lstUserR.get(i).getUrl().equals(url) && lstUserR.get(i).getTime().equals(date)) {
                return lstUserR.get(i).getCount();
            }
        }
        return 0;
    }

    public String splitHeadTail(String x) {
        String[] arr = x.split(" ");
        String[] arrX = arr[1].split(":");
        return arrX[0];
    }

    public float getSecondFromTime(String time) {
        float total;
        String[] arr = time.split(":");
        int hour = Integer.parseInt(arr[0]);
        int minute = Integer.parseInt(arr[1]);
        float second = Float.parseFloat(arr[2]);
        total = hour * 60 * 60 + minute * 60 + second;
        return total;
    }

    public String getTimeFromEL(String time) {
        String[] arr = time.split("T");
        String firstTime = arr[1];
        String[] arr2 = firstTime.split("\\+");
        return arr2[0];
    }

    public String getDateFromEL(String time) {
        String[] arr = time.split("T");
        return arr[0];
    }

    public boolean saveAll() {
        ToDB infoToDB = new ToDB();
        Iterable<UserActivity> lstInfo = userActRepository.findAll();
        List<UserActivityDB> lstDBInfo = new ArrayList<>();
        Iterator item = lstInfo.iterator();
        while (item.hasNext()) {
            UserActivityDB dbInfo = infoToDB.convertInfoToDB((UserActivity) item.next());
            lstDBInfo.add(dbInfo);
        }
        userActDBRepository.saveAll(lstDBInfo);
        return true;
    }

    public void saveST() {
        UserActivityDB userActivityDB = new UserActivityDB("pcName", "message", 1, "date", (float) (100 / 60));
        userActDBRepository.save(userActivityDB);
        System.out.println("ok");
    }

    public Iterable<UserActivity> findAll() {
        return userActRepository.findAll();
    }

    public List<UserActivity> findByUrl(String url) {
        return userActRepository.findByUrl(url);
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        Timestamp scheduledFireTime = new Timestamp(jobExecutionContext.getScheduledFireTime().getTime());
        String strScheduledFireTime = sdf.format(scheduledFireTime);
        try {
            mainProcessing(strScheduledFireTime);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    public boolean checkExists(String pcName) {
        Optional<Users> user = usersRepository.findById(pcName);
        if (user.isPresent())
            return true;
        return false;
    }

    //--------------------------------------------------------------------------------------------------------------------------
//dang
    public boolean processAdd(String message, float totalTime, String date, String pcName) {
        final long start = System.currentTimeMillis();
        Optional<UserActivityDB> userDB = findUserByIDDB(pcName, message, date);
        final long end = System.currentTimeMillis();
        findTime += (end - start);
        int count = 0;
        float total = 0;
        if (userDB.isPresent()) {
            count = userDB.get().getCount();
            total = userDB.get().getTotal_time();
        }
        total *= 60;
        totalTime += total;
        UserActivityDB userActivityDB = new UserActivityDB(pcName, message, ++count, date, (totalTime / 60));
        if (checkExists(pcName)) {
            lstDbList.add(userActivityDB);
            return true;
        }
        return false;
    }

    public void addLstKeyTemp(String message, String date, String pcName, float totalTime) {
        MyKey myKey = new MyKey(pcName, message, date);
        mKey.put(myKey,totalTime);
    }

    public void processAddCompare(String message, float totalTime, String date, String pcName) {
        addLstKeyTemp(message, date, pcName, totalTime);
    }

    public void processAddCompare2(Map<MyKey, Float> mKey) {
        Set<MyKey> kSet = mKey.keySet();// all mykey
        List<UserActivityDB> lstResult = userActDBRepository.findAllById(kSet);
        Map<UserActivityDB,Float> mResultExists = new HashMap<>();
        for(int i=0;i<lstResult.size();i++){
            MyKey myKey = new MyKey(lstResult.get(i).getUser_id(), lstResult.get(i).getUrl(), lstResult.get(i).getTime());
            mResultExists.put(lstResult.get(i),mKey.get(myKey));
            mKey.remove(myKey);
        }
        Map<UserActivityDB,Float> mResultNotExists = new HashMap<>();
        Set<MyKey> kSet2 = mKey.keySet();
        for(MyKey myKey : kSet2){
            UserActivityDB userActivityDB = new UserActivityDB(myKey.getUser_id(), myKey.getUrl(), 0, myKey.getTime(),0);
            mResultNotExists.put(userActivityDB,mKey.get(myKey));
        }
        //--got a list not exists--->lstResultNotExists
        addToListExistsAndNot(mResultExists,mResultNotExists);
    }
    public void addToListExistsAndNot(Map<UserActivityDB, Float> mResultExists, Map<UserActivityDB,Float> mResultNotExists) {
        int count = 0;
        float total = 0;
        float totalTime = 0;
        Set<UserActivityDB> set1 = mResultExists.keySet();
        for(UserActivityDB ele : set1){
            count = ele.getCount();
            total = ele.getTotal_time();
            totalTime = mResultExists.get(ele);
            total *= 60;
            totalTime += total;

            UserActivityDB userActivityDB = new UserActivityDB(ele.getUser_id(), ele.getUrl(), ++count, ele.getTime(), (totalTime / 60));
            if (checkExists(ele.getUser_id())) {
                lstDbList.add(userActivityDB);
            }
        }
        Set<UserActivityDB> set2 = mResultNotExists.keySet();
        for(UserActivityDB ele : set2){
            totalTime = mResultNotExists.get(ele);

            UserActivityDB userActivityDB = new UserActivityDB(ele.getUser_id(), ele.getUrl(), 1, ele.getTime(), ((totalTime / 60)));
            if (checkExists(ele.getUser_id())) {
                lstDbList.add(userActivityDB);
            }
        }
    }
    Map<MyKey, Float> mKey = new HashMap<>();
    List<UserActivityDB> lstDbList = new ArrayList<>();
    long findTime = 0;
    long z = 0;
    long saveTime = 0;

    public Optional<UserActivityDB> findUserByIDDB(String pcName, String url, String date) {
        return userActDBRepository.findById(new MyKey(pcName, url, date));
    }

    public boolean checkContain(String url) {
        String[] arr = url.split(" ");
        return arr[0].contains("CONNECT");
    }

    public List<UserActivity> findByField(final String message, String fromDate, String toDate, String pcName) {
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("localdate")
                .gte(fromDate)
                .lte(toDate)).must(QueryBuilders.matchPhraseQuery("user_id", pcName)).must(QueryBuilders.matchPhraseQuery("url", message));
        Query searchQuery = new NativeSearchQueryBuilder()
                .withQuery(queryBuilder)
                .withSort(SortBuilders.fieldSort("localdate").order(SortOrder.ASC))
                .build();

        SearchHits<UserActivity> productHits =
                elasticsearchOperations
                        .search(searchQuery, UserActivity.class,
                                IndexCoordinates.of(index));
        List<UserActivity> productMatches = new ArrayList<>();
        productHits.forEach(srchHit -> productMatches.add(srchHit.getContent()));
        if (productMatches.size() == 0)
            LOGGER.info("size ne:---->:" + productMatches.size());
        return productMatches;
    }

    public List<UserActivity> findByFieldCroll(final String message, String fromDate, String toDate, String pcName) throws IOException {
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("localdate")
                .gte(fromDate)
                .lte(toDate)).must(QueryBuilders.matchPhraseQuery("user_id", pcName)).must(QueryBuilders.matchPhraseQuery("url", message));

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.sort("localdate", SortOrder.ASC);
//        searchSourceBuilder.size(10);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = dbConfig.elasticsearchClient().search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        List<UserActivity> lstUserTemp = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            for (SearchHit hit : searchHits) {
                // Map map = hit.getSourceAsMap();
                //System.out.println(map.toString());
                String source = hit.getSourceAsString();
                UserActivity userActivity = objectMapper.readValue(source, UserActivity.class);
                lstUserTemp.add(userActivity);
            }
            searchResponse = dbConfig.elasticsearchClient().scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = dbConfig.elasticsearchClient().clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
//        boolean succeeded = clearScrollResponse.isSucceeded();
        return lstUserTemp;
    }

    public Terms groupByField(String fromDate, String toDate) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.timeout(new TimeValue(100, TimeUnit.SECONDS));
        searchBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchBuilder.fetchSource(false);

        TermsAggregationBuilder aggregation1 = AggregationBuilders
                .terms("url")
                .field("url" + ".keyword").size(100000);

        TermsAggregationBuilder aggregation = AggregationBuilders
                .terms("id")
                .field("user_id" + ".keyword")
                .size(100000)
                .subAggregation(aggregation1);

        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("localdate").gte(fromDate).lte(toDate));

        searchBuilder.query(queryBuilder);
        searchBuilder.aggregation(aggregation);

        SearchRequest searchRequest = Requests.searchRequest(index).allowPartialSearchResults(true)
                .source(searchBuilder);
        SearchResponse searchResponse = dbConfig.elasticsearchClient().search(searchRequest, RequestOptions.DEFAULT);

        Terms groupedProperties = searchResponse.getAggregations().get("id");
        return groupedProperties;
    }

    public Terms groupByFieldUpdateTerm(String fromDate, String toDate) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.timeout(new TimeValue(100, TimeUnit.SECONDS));
        //searchBuilder.sort(new ScoreSortBuilder().order(SortOrder.ASC));
        searchBuilder.fetchSource(false);

        TermsAggregationBuilder aggregation1 = AggregationBuilders
                .terms("url")
                .field("url" + ".keyword").size(100000);
        TermsAggregationBuilder aggregation2 = AggregationBuilders
                .terms("date")
                .field("localdate").size(100000)
                .order(BucketOrder.aggregation("_key", true));

        TermsAggregationBuilder aggregation = AggregationBuilders
                .terms("id")
                .field("user_id" + ".keyword")
                .size(100000)
                .subAggregation(aggregation1.subAggregation(aggregation2));
        //.subAggregation(aggregation1);

        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("localdate").gte(fromDate).lte(toDate));

        searchBuilder.query(queryBuilder);
        searchBuilder.aggregation(aggregation);
        //searchBuilder.sort("@timestamp",SortOrder.ASC);

        SearchRequest searchRequest = Requests.searchRequest(index).allowPartialSearchResults(true)
                .source(searchBuilder);
        SearchResponse searchResponse = dbConfig.elasticsearchClient().search(searchRequest, RequestOptions.DEFAULT);

        Terms groupedProperties = searchResponse.getAggregations().get("id");
        return groupedProperties;
    }

    public CompositeAggregation groupByFieldUpdateComposite(String fromDate, String toDate) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.timeout(new TimeValue(100, TimeUnit.SECONDS));
        //searchBuilder.sort(new ScoreSortBuilder().order(SortOrder.ASC));
        searchBuilder.fetchSource(false);

        TermsAggregationBuilder aggregation1 = AggregationBuilders
                .terms("url")
                .field("url" + ".keyword").size(100000);
        TermsAggregationBuilder aggregation2 = AggregationBuilders
                .terms("date1")
                .field("localdate").size(100000)
                .order(BucketOrder.aggregation("_key", true));

        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources.add(new TermsValuesSourceBuilder("product").field("user_id.keyword"));
        CompositeAggregationBuilder compositeAggregationBuilder =
                new CompositeAggregationBuilder("byProductAttributes", sources).size(10000);

        compositeAggregationBuilder.subAggregation(aggregation1.subAggregation(aggregation2));

        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("localdate").gte(fromDate).lte(toDate));

        searchBuilder.query(queryBuilder);
        searchBuilder.aggregation(compositeAggregationBuilder);
        //searchBuilder.sort("@timestamp",SortOrder.ASC);

        SearchRequest searchRequest = Requests.searchRequest(index).allowPartialSearchResults(true)
                .source(searchBuilder);
        SearchResponse searchResponse = dbConfig.elasticsearchClient().search(searchRequest, RequestOptions.DEFAULT);

        CompositeAggregation compositeAggregation = searchResponse.getAggregations().get("byProductAttributes");
        return compositeAggregation;
    }

    long y = 0;

    public Boolean mainProcessing(String timeStamp) throws ParseException, IOException {
        final long startTime = System.currentTimeMillis();
        System.out.println(timeStamp);
        String fromDate = "", toDate = "";
        fromDate = "2021-04-12" + "T01+0700";
        toDate = "2021-04-13" + "T23+0700";
        CompositeAggregation lstRoot = groupByFieldUpdateComposite(fromDate, toDate);
        for (CompositeAggregation.Bucket entry : lstRoot.getBuckets()) {
            Terms bucket = entry.getAggregations().get("url");
            String finalPcName = (String) entry.getKey().get("product");
            bucket.getBuckets().forEach(ele -> {

                List<String> lstDate = new ArrayList<>();
                String url = ele.getKeyAsString();
                Terms bucket1 = ele.getAggregations().get("date1");
                for (Terms.Bucket entry1 : bucket1.getBuckets()) {
                    lstDate.add(entry1.getKeyAsString());
                }
                if (!checkContain(url)) {
                    return;
                }
                final long startTime2 = System.currentTimeMillis();
                executeProcess(lstDate, url, finalPcName);
                final long endTime2 = System.currentTimeMillis();
                z += (endTime2 - startTime2);
                //subProcess(lstDate,url,finalPcName);
            });
        }
        final long endTime = System.currentTimeMillis();
        LOGGER.log(Logger.Level.INFO, "Total execution time: " + (endTime - startTime));
        LOGGER.log(Logger.Level.INFO, "find time :" + findTime);
        findTime = 0;
        LOGGER.log(Logger.Level.INFO, "save time all :" + z);
        z = 0;
        LOGGER.log(Logger.Level.INFO, "save time :" + saveTime);
        saveTime = 0;
        return true;
    }

    public Boolean mainProcessingTerm(String timeStamp) throws ParseException, IOException {
        final long startTime = System.currentTimeMillis();
        System.out.println(timeStamp);
        String fromDate = "", toDate = "";
        fromDate = "2021-04-12" + "T01+0700";
        toDate = "2021-04-13" + "T23+0700";
        Terms lstRoot = groupByFieldUpdateTerm(fromDate, toDate);
        for (Terms.Bucket entry : lstRoot.getBuckets()) {
            Terms bucket = entry.getAggregations().get("url");
            String finalPcName = entry.getKeyAsString();
            bucket.getBuckets().forEach(ele -> {
                List<String> lstDate = new ArrayList<>();
                String url = ele.getKeyAsString();
                Terms bucket1 = ele.getAggregations().get("date");
                for (Terms.Bucket entry1 : bucket1.getBuckets()) {
                    lstDate.add(entry1.getKeyAsString());
                }
                if (!checkContain(url)) {
                    return;
                }
                executeProcess(lstDate, url, finalPcName);
                //subProcess(lstDate,url,finalPcName);
            });
        }
        final long endTime = System.currentTimeMillis();
        LOGGER.log(Logger.Level.INFO, "Total execution time4: " + (endTime - startTime));
        return true;
    }

    public void executeProcess(List<String> lstDate, String url, String finalPcName) {
        List<String> lstDate2 = new ArrayList<>();
        for (int i = 0; i < lstDate.size() - 1; i++) {
            if (getDateFromEL(lstDate.get(i)).equalsIgnoreCase(getDateFromEL(lstDate.get(i + 1)))) {
                lstDate2.add(lstDate.get(i));
            } else {
                lstDate2.add(lstDate.get(i));
                subProcess(lstDate2, url, finalPcName);
                lstDate2 = new ArrayList<>();
            }
            if (i == lstDate.size() - 2) {
                lstDate2.add(lstDate.get(i + 1));
                subProcess(lstDate2, url, finalPcName);
            }
        }
        if (lstDate.size() == 1)
            subProcess(lstDate, url, finalPcName);
        //xl
        //prepare list
        //processAddCompare2(mKey);
        //add to db
        final long start3 = System.currentTimeMillis();
        userActDBRepository.saveAll(lstDbList);
        final long end3 = System.currentTimeMillis();
        saveTime += (end3 - start3);
        lstDbList = new ArrayList<>();
    }

    public Boolean mainProcessing2(String timeStamp) throws ParseException, IOException {
        final long startTime = System.currentTimeMillis();
        System.out.println(timeStamp);
        String fromDate = "", toDate = "";
        Date dateFire = sdf.parse(timeStamp);
        int hour = dateFire.getHours();
        String[] temp = timeStamp.split(" ");
        String date = temp[0];

        fromDate = "2021-04-12" + "T01+0700";
        toDate = "2021-04-12" + "T23+0700";

//        if (hour < 12) {
//            fromDate = date + "T08+0700";
//            toDate = date + "T11:30+0700";
//        } else {
//            fromDate = date + "T11:31+0700";
//            toDate = date + "T15:30+0700";
//        }
        Terms lstRoot = groupByField(fromDate, toDate);
        for (Terms.Bucket entry : lstRoot.getBuckets()) {
            Terms bucket = entry.getAggregations().get("xx");
            String finalFromDate = fromDate;
            String finalToDate = toDate;
            String finalPcName = entry.getKeyAsString();
            bucket.getBuckets().forEach(bucket1 -> {
                String url = bucket1.getKeyAsString();
                if (!checkContain(url)) {
                    return;
                }
                List<UserActivity> lstUserRS = null;
                try {
                    lstUserRS = findByFieldCroll(splitHeadTail(url), finalFromDate, finalToDate, finalPcName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                subProcess2(lstUserRS, url, finalPcName);
            });
        }
        final long endTime = System.currentTimeMillis();
        LOGGER.log(Logger.Level.INFO, "Total execution time: " + (endTime - startTime));
        return true;
    }

    //dang
    public void subProcess(List<String> lstUserRS, String url, String finalPcName) {
        AtomicReference<String> dateDB = new AtomicReference<>("");
        dateDB.set(getDateFromEL(lstUserRS.get(0)));
        float totalTime = 0;
        for (int i = 0; i < lstUserRS.size() - 1; i++) {
            String timeRootF = getTimeFromEL2(lstUserRS.get(i));
            float secondTimeF = getSecondFromTime2(timeRootF);
            String timeRootS = getTimeFromEL2(lstUserRS.get(i + 1));
            float secondTimeS = getSecondFromTime2(timeRootS);
            float timeUsed = secondTimeS - secondTimeF;
            //time between two surf bigger than 3 minutes--->solve // break time =3m
            if (timeUsed >= 180) {
                if (totalTime == 0)
                    totalTime = 180;
                processAdd(splitHeadTail(url), totalTime, dateDB.get(), finalPcName);
//                processAddCompare(splitHeadTail(url), totalTime, dateDB.get(), finalPcName);
                totalTime = 0;
            } else
                totalTime += timeUsed;
            if (totalTime >= 600) {
                processAdd(splitHeadTail(url), totalTime, dateDB.get(), finalPcName);
//                processAddCompare(splitHeadTail(url), totalTime, dateDB.get(), finalPcName);
                totalTime = 0;
            }
            if (i == lstUserRS.size() - 2) {
                if (timeUsed >= 180)
                    processAdd(splitHeadTail(url), 180, dateDB.get(), finalPcName);
//                    processAddCompare(splitHeadTail(url), 180, dateDB.get(), finalPcName);

                else
                    processAdd(splitHeadTail(url), totalTime, dateDB.get(), finalPcName);
//                    processAddCompare(splitHeadTail(url), totalTime, dateDB.get(), finalPcName);

            }
        }
        if (lstUserRS.size() == 1)
            processAdd(splitHeadTail(url), 180, dateDB.get(), finalPcName);
//            processAddCompare(splitHeadTail(url), 180, dateDB.get(), finalPcName);

    }

    public float getSecondFromTime2(String time) {
        float total;
        String[] arr = time.split(":");
        int hour = Integer.parseInt(arr[0]) + 7;
        int minute = Integer.parseInt(arr[1]);
        float second = Float.parseFloat(arr[2]);
        total = hour * 60 * 60 + minute * 60 + second;
        return total;
    }

    public String getTimeFromEL2(String time) {
        String[] arr = time.split("T");
        String firstTime = arr[1];
        String[] arr2 = firstTime.split("Z");
        return arr2[0];
    }

    public void subProcess2(List<UserActivity> lstUserRS, String url, String finalPcName) {
        AtomicReference<String> dateDB = new AtomicReference<>("");
        dateDB.set(getDateFromEL(lstUserRS.get(0).getTime()));
        float totalTime = 0;
        for (int i = 0; i < lstUserRS.size() - 1; i++) {
            String timeRootF = getTimeFromEL(lstUserRS.get(i).getTime());
            float secondTimeF = getSecondFromTime(timeRootF);
            String timeRootS = getTimeFromEL(lstUserRS.get(i + 1).getTime());
            float secondTimeS = getSecondFromTime(timeRootS);
            float timeUsed = secondTimeS - secondTimeF;
            //time between two surf bigger than 3 minutes--->solve // break time =3m
            if (timeUsed >= 180) {
                if (totalTime == 0)
                    totalTime = 180;
                processAdd(splitHeadTail(url), totalTime, dateDB.get(), finalPcName);
                totalTime = 0;
            } else
                totalTime += timeUsed;
            if (totalTime >= 600) {
                processAdd(splitHeadTail(url), totalTime, dateDB.get(), finalPcName);
                totalTime = 0;
            }
            if (i == lstUserRS.size() - 2) {
                if (timeUsed >= 180)
                    processAdd(splitHeadTail(url), 180, dateDB.get(), finalPcName);
                else
                    processAdd(splitHeadTail(url), totalTime, dateDB.get(), finalPcName);
            }
        }
        if (lstUserRS.size() == 1)
            processAdd(splitHeadTail(url), 180, dateDB.get(), finalPcName);
    }
}
