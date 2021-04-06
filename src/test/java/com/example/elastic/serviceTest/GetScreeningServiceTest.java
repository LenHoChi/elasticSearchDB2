package com.example.elastic.serviceTest;

import org.jsmart.zerocode.core.domain.JsonTestCase;
import org.jsmart.zerocode.core.domain.LoadWith;
import org.jsmart.zerocode.core.domain.TargetEnv;
import org.jsmart.zerocode.core.domain.TestMapping;
import org.jsmart.zerocode.core.runner.ZeroCodeUnitRunner;
import org.jsmart.zerocode.core.runner.parallel.ZeroCodeLoadRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@TargetEnv("screening_service_host.properties")
@LoadWith("load_generatGion.properties")
@TestMapping(testClass = GetScreeningServiceTest.class, testMethod = "testGetScreeningLocalAndGlobal")
@RunWith(ZeroCodeUnitRunner.class)
public class GetScreeningServiceTest {
    public class LoadGetTest {
    }
    @Test
    @JsonTestCase("load_tests/get/get_screening_details_by_custid.json")
    public void testGetScreeningLocalAndGlobal() throws Exception {
    }
}
