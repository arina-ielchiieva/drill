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

import com.google.common.base.Ticker;

import java.util.concurrent.TimeUnit;

/**
 * Helper that creates stopwatch based if debug level is enabled.
 * For debug level returns stopwatch wrapper around {@link com.google.common.base.Stopwatch},
 * otherwise returns dummy stopwatch that does nothing.
 */
public interface Stopwatch {

  static Stopwatch createUnstarted(boolean isDebugEnabled) {
    return isDebugEnabled ? GuavaStopwatchWrapper.createUnstarted() : DummyStopwatch.createUnstarted();
  }

  static Stopwatch createUnstarted(boolean isDebugEnabled, Ticker ticker) {
    return isDebugEnabled ? GuavaStopwatchWrapper.createUnstarted(ticker) : DummyStopwatch.createUnstarted(ticker);
  }

  static Stopwatch createStarted(boolean isDebugEnabled) {
    return isDebugEnabled ? GuavaStopwatchWrapper.createdStarted() : DummyStopwatch.createdStarted();
  }

  static Stopwatch createStarted(boolean isDebugEnabled, Ticker ticker) {
    return isDebugEnabled ? GuavaStopwatchWrapper.createdStarted(ticker) : DummyStopwatch.createdStarted(ticker);
  }

  boolean isRunning();

  Stopwatch start();

  Stopwatch stop();

  Stopwatch reset();

  long elapsed(TimeUnit desiredUnit);


  /**
   * Dummy stopwatch that does nothing when calling stopwatch methods.
   */
  class DummyStopwatch implements Stopwatch {

    private boolean isRunning;

    private DummyStopwatch() {

    }

    private static DummyStopwatch createUnstarted() {
      return new DummyStopwatch();
    }

    private static DummyStopwatch createUnstarted(Ticker ticker) {
      return createUnstarted();
    }

    private static DummyStopwatch createdStarted() {
      DummyStopwatch stopwatch = createUnstarted();
      stopwatch.isRunning = true;
      return stopwatch;
    }

    private static DummyStopwatch createdStarted(Ticker ticker) {
      return createdStarted();
    }

    @Override
    public boolean isRunning() {
      return isRunning;
    }

    @Override
    public Stopwatch start() {
      isRunning = true;
      return this;
    }

    @Override
    public Stopwatch stop() {
      isRunning = false;
      return this;
    }

    @Override
    public Stopwatch reset() {
      isRunning = false;
      return this;
    }

    @Override
    public long elapsed(TimeUnit desiredUnit) {
      return 0;
    }

    @Override
    public String toString() {
      return "DummyStopwatch{}";
    }
  }

  /**
   * Wrapper class that delegates all stopwatch logic to {@link com.google.common.base.Stopwatch}.
   */
  class GuavaStopwatchWrapper implements Stopwatch {

    private com.google.common.base.Stopwatch delegate;

    private GuavaStopwatchWrapper(com.google.common.base.Stopwatch delegate) {
      this.delegate = delegate;
    }

    private static GuavaStopwatchWrapper createUnstarted() {
      com.google.common.base.Stopwatch delegate = com.google.common.base.Stopwatch.createUnstarted();
      return new GuavaStopwatchWrapper(delegate);
    }

    private static GuavaStopwatchWrapper createUnstarted(Ticker ticker) {
      com.google.common.base.Stopwatch delegate = com.google.common.base.Stopwatch.createUnstarted(ticker);
      return new GuavaStopwatchWrapper(delegate);
    }

    private static GuavaStopwatchWrapper createdStarted() {
      com.google.common.base.Stopwatch delegate = com.google.common.base.Stopwatch.createStarted();
      return new GuavaStopwatchWrapper(delegate);
    }

    private static GuavaStopwatchWrapper createdStarted(Ticker ticker) {
      com.google.common.base.Stopwatch delegate = com.google.common.base.Stopwatch.createStarted(ticker);
      return new GuavaStopwatchWrapper(delegate);
    }

    @Override
    public boolean isRunning() {
      return delegate.isRunning();
    }

    @Override
    public Stopwatch start() {
      delegate = delegate.start();
      return this;
    }

    @Override
    public Stopwatch stop() {
      delegate = delegate.stop();
      return this;
    }

    @Override
    public Stopwatch reset() {
      delegate = delegate.reset();
      return this;
    }

    @Override
    public long elapsed(TimeUnit desiredUnit) {
      return delegate.elapsed(desiredUnit);
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }

}
