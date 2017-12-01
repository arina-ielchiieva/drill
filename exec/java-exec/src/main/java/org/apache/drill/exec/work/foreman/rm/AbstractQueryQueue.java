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
package org.apache.drill.exec.work.foreman.rm;

import org.apache.drill.exec.proto.UserBitShared.QueryId;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AbstractQueryQueue implements QueryQueue {

  private final ConcurrentLinkedQueue<QueueAcquirer> pendingQueue = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<QueueAcquirer> cancellationQueue = new ConcurrentLinkedQueue<>();
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  @Override
  public void enqueue(QueueAcquirer queueAcquirer) {
    pendingQueue.offer(queueAcquirer);
    executorService.execute(this);
  }

  @Override
  public void cancel(QueueAcquirer queueAcquirer) {
    cancellationQueue.offer(queueAcquirer);
    executorService.execute(this);
  }

  @Override
  public void run() {
    while (!pendingQueue.isEmpty() || !cancellationQueue.isEmpty()) {
      cancelPendingQueries();
      acquireLease();
    }
  }

  @Override
  public void close() {
    executorService.shutdownNow();
    internalClose();
  }

  protected abstract QueueLease getLease(QueryId queryId, double cost) throws Exception;

  protected abstract void internalRelease(QueueLease lease);

  protected abstract void internalClose();

  private void cancelPendingQueries() {
    QueueAcquirer queueAcquirer;
    while ((queueAcquirer = cancellationQueue.poll()) != null) {
      pendingQueue.remove(queueAcquirer);
      queueAcquirer.getListener().cancelled();
    }
  }

  private void acquireLease() {
    QueueAcquirer queueAcquirer;
    // try to acquire the lease if there is a pending query
    if ((queueAcquirer = pendingQueue.poll()) != null) {
      try {
        QueueLease lease = getLease(queueAcquirer.getQueryId(), queueAcquirer.getCost());

        // if cancellation was requested, release the lease
        if (cancellationQueue.remove(queueAcquirer)) {
          internalRelease(lease);
          queueAcquirer.getListener().cancelled();
        } else {
          queueAcquirer.getListener().admitted(lease);
        }

      } catch (Exception e) {
        // if cancellation was requested, ignore the exception
        if (cancellationQueue.remove(queueAcquirer)) {
          queueAcquirer.getListener().cancelled();
        } else {
          queueAcquirer.getListener().failed(e);
        }
      }
    }
  }
}
