package com.example.elastic.serviceTest;

import org.jsmart.zerocode.core.domain.LoadWith;
import org.jsmart.zerocode.core.domain.TestMapping;
import org.jsmart.zerocode.core.runner.ZeroCodeUnitRunner;
import org.junit.runner.RunWith;

@LoadWith("load_generatGion.properties")
@TestMapping(testClass = GetScreeningServiceTest.class, testMethod = "testGetScreeningLocalAndGlobal")
@RunWith(ZeroCodeUnitRunner.class)
public class LoadGetTest {
}
