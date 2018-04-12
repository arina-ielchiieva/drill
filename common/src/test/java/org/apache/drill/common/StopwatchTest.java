/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.common;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StopwatchTest {

  @Test
  public void testDummyStarted() {
    Stopwatch stopwatch = Stopwatch.createStarted(false);
    assertTrue("Stopwatch should be running", stopwatch.isRunning());
    assertEquals("Elapsed time should be 0", 0, stopwatch.elapsed(TimeUnit.NANOSECONDS));
    stopwatch.reset();
    assertFalse("Stopwatch should be stopped", stopwatch.isRunning());
    stopwatch.start();
    assertTrue("Stopwatch should be running", stopwatch.isRunning());
    stopwatch.stop();
    assertFalse("Stopwatch should be stopped", stopwatch.isRunning());
    assertEquals("Elapsed time should be 0",0, stopwatch.elapsed(TimeUnit.NANOSECONDS));
  }

  @Test
  public void testDummyUnstarted() {
    Stopwatch stopwatch = Stopwatch.createUnstarted(false);
    assertFalse("Stopwatch should be stopped", stopwatch.isRunning());
    stopwatch.start();
    assertTrue("Stopwatch should be running", stopwatch.isRunning());
    assertEquals("Elapsed time should be 0",0, stopwatch.elapsed(TimeUnit.NANOSECONDS));
    stopwatch.stop();
    assertFalse("Stopwatch should be stopped", stopwatch.isRunning());
    assertEquals("Elapsed time should be 0",0, stopwatch.elapsed(TimeUnit.NANOSECONDS));
  }

  @Test
  public void testGuavaWrapperStarted() {
    Stopwatch stopwatch = Stopwatch.createStarted(true);
    assertTrue("Stopwatch should be running", stopwatch.isRunning());
    long elapsedTime = stopwatch.elapsed(TimeUnit.NANOSECONDS);
    assertTrue("Elapsed time should be more then 0", elapsedTime > 0);
    stopwatch.stop();
    assertFalse("Stopwatch should be stopped", stopwatch.isRunning());
    assertTrue("Elapsed time should be more then previous elapsed time", stopwatch.elapsed(TimeUnit.NANOSECONDS) > elapsedTime);
  }

  @Test
  public void testGuavaWrapperUnstarted() {
    Stopwatch stopwatch = Stopwatch.createUnstarted(true);
    assertFalse("Stopwatch should be stopped", stopwatch.isRunning());
    assertEquals(0, stopwatch.elapsed(TimeUnit.NANOSECONDS));
    stopwatch.start();
    long elapsedTime = stopwatch.elapsed(TimeUnit.NANOSECONDS);
    assertTrue("Elapsed time should be more then 0", elapsedTime > 0);
    stopwatch.stop();
    assertFalse("Stopwatch should be stopped", stopwatch.isRunning());
    assertTrue("Elapsed time should be more then previous elapsed time", stopwatch.elapsed(TimeUnit.NANOSECONDS) > elapsedTime);
  }

}
