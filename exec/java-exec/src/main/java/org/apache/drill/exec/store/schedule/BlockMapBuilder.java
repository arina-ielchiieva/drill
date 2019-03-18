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
package org.apache.drill.exec.store.schedule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.drill.common.exceptions.ErrorHelper;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableRangeMap;
import org.apache.drill.shaded.guava.com.google.common.collect.Range;

public class BlockMapBuilder {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BlockMapBuilder.class);
  private static final MetricRegistry metrics = DrillMetrics.getRegistry();
  private static final String BLOCK_MAP_BUILDER_TIMER = MetricRegistry.name(BlockMapBuilder.class, "blockMapBuilderTimer");

  private final Map<Path, ImmutableRangeMap<Long, BlockLocation>> blockMapMap = new ConcurrentHashMap<>();

  private final FileSystem fs;
  private final CompressionCodecFactory codecFactory;
  private final Map<String,DrillbitEndpoint> endPointMap;

  public BlockMapBuilder(FileSystem fs, Collection<DrillbitEndpoint> endpoints) {
    this.fs = fs;
    this.codecFactory = new CompressionCodecFactory(fs.getConf());
    this.endPointMap = buildEndpointMap(endpoints);
  }

  /**
   * Generates chunks of files that can be read in parallel for the given selection of file statuses.
   *
   * @param fileStatues list of file statuses (may contain directories)
   * @param blockify if single file can be read by multiple threads
   * @return list of file blocks to read
   * @throws IOException in case of errors creating list of file blocks
   */
  // Note: IOException will be throw in case of errors (covered by sneakyThrow)
  public List<CompleteFileWork> generateFileWork(List<FileStatus> fileStatues, boolean blockify) throws IOException {
    ForkJoinPool pool = new ForkJoinPool();
    try {
      BlockMapReader reader = new BlockMapReader(fs, codecFactory, endPointMap, fileStatues, blockify);
      return pool.invoke(reader);
    } finally {
      pool.shutdown();
    }
  }

  /**
   * Builds a mapping of Drillbit endpoints to host names.
   *
   * @param endpoints drillbit endpoints
   * @return map of host names and endpoints
   */
  private Map<String, DrillbitEndpoint> buildEndpointMap(Collection<DrillbitEndpoint> endpoints) {
    Stopwatch watch = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;

    Map<String, DrillbitEndpoint> endpointMap = endpoints.stream()
      .collect(Collectors.toMap(
        DrillbitEndpoint::getAddress,
        Function.identity(),
        (o, n) -> n));

    if (watch != null) {
      watch.stop();
      logger.debug("Took {} ms to build endpoint map", watch.elapsed(TimeUnit.MILLISECONDS));
    }
    return ImmutableMap.copyOf(endpointMap);
  }

  private class BlockMapReader extends RecursiveTask<List<CompleteFileWork>> {

    private final FileSystem fs;
    private final CompressionCodecFactory codecFactory;
    private final Map<String,DrillbitEndpoint> endPointMap;
    private final List<FileStatus> statuses;

    // This variable blockify indicates if a single file can be read by multiple threads
    // For examples, for CSV, it is set as true
    // because each row in a CSV file can be considered as an independent record;
    // for json, it is set as false
    // because each row in a json file cannot be determined as a record or not simply by that row alone
    private final boolean blockify;

    BlockMapReader(FileSystem fs,
                   CompressionCodecFactory codecFactory,
                   Map<String,DrillbitEndpoint> endPointMap,
                   List<FileStatus> statuses,
                   boolean blockify) {
      this.fs = fs;
      this.codecFactory = codecFactory;
      this.endPointMap = endPointMap;
      this.statuses = statuses;
      this.blockify = blockify;
    }

    @Override
    protected List<CompleteFileWork> compute() {

      // expand directories into files, if any
      List<FileStatus> files = new ArrayList<>();
      for (FileStatus status : statuses) {
        if (status.isDirectory()) {
          files.addAll(DrillFileSystemUtil.listFilesSafe(fs, status.getPath(), true));
        } else {
          files.add(status);
        }
      }

      // if there is only one file, do actual work
      if (files.size() == 1) {
        FileStatus status = files.get(0);

        Set<String> noDrillbitHosts = logger.isDebugEnabled() ? new HashSet<>() : null;
        List<CompleteFileWork> work = new ArrayList<>();

        boolean error = false;
        if (blockify && !compressed(status)) {
          try {
            ImmutableRangeMap<Long, BlockLocation> rangeMap = getBlockMap(status);
            rangeMap.asMapOfRanges().values().stream()
              .map(blockLocation -> createCompleteFileWork(noDrillbitHosts, status, blockLocation.getOffset(), blockLocation.getLength()))
              .forEach(work::add);
          } catch (IOException e) {
            logger.warn("Failure while generating file work.", e);
            error = true;
          }
        }

        if (!blockify || error || compressed(status)) {
          work.add(createCompleteFileWork(noDrillbitHosts, status, 0, status.getLen()));
        }

        // This if-condition is specific for empty CSV file
        // For CSV files, the global variable blockify is set as true
        // And if this CSV file is empty, rangeMap would be empty also
        // Therefore, at the point before this if-condition, work would not be populated
        if (work.isEmpty()) {
          work.add(createCompleteFileWork(noDrillbitHosts, status, 0, 0));
        }

        if (noDrillbitHosts != null) {
          noDrillbitHosts
            .forEach(host -> logger.debug("Failure finding Drillbit running on host {}. Skipping affinity to that host.", host));
        }

        return work;
      }

      // if there are multiple files, parallel their execution and wait for the results
      return files.stream()
        .map(file -> new BlockMapReader(fs, codecFactory, endPointMap, Collections.singletonList(file), blockify))
        .map(ForkJoinTask::join)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
    }

    private boolean compressed(FileStatus fileStatus) {
      return codecFactory.getCodec(fileStatus.getPath()) != null;
    }

    private CompleteFileWork createCompleteFileWork(Set<String> noDrillbitHosts, FileStatus status, long start, long length) {
      try {
        EndpointByteMap endpointByteMap = getEndpointByteMap(noDrillbitHosts, new FileStatusWork(status));
        return new CompleteFileWork(endpointByteMap, start, length, status.getPath());
      } catch (IOException e) {
        ErrorHelper.sneakyThrow(e);
        // this should not happen
        throw new RuntimeException();
      }
    }

    private ImmutableRangeMap<Long,BlockLocation> buildBlockMap(Path path) throws IOException {
      FileStatus status = fs.getFileStatus(path);
      return buildBlockMap(status);
    }

    /**
     * Builds a mapping of block locations to file byte range.
     *
     * @param status file status
     * @return byte range mapping
     */
    private ImmutableRangeMap<Long, BlockLocation> buildBlockMap(FileStatus status) throws IOException {
      Timer.Context context = metrics.timer(BLOCK_MAP_BUILDER_TIMER).time();
      BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, status.getLen());
      ImmutableRangeMap.Builder<Long, BlockLocation> blockMapBuilder = new ImmutableRangeMap.Builder<>();
      for (BlockLocation block : blocks) {
        long start = block.getOffset();
        long end = start + block.getLength();
        Range<Long> range = Range.closedOpen(start, end);
        blockMapBuilder.put(range, block);
      }
      ImmutableRangeMap<Long,BlockLocation> blockMap = blockMapBuilder.build();
      blockMapMap.put(status.getPath(), blockMap);
      context.stop();
      return blockMap;
    }

    private ImmutableRangeMap<Long,BlockLocation> getBlockMap(Path path) throws IOException{
      ImmutableRangeMap<Long,BlockLocation> blockMap = blockMapMap.get(path);
      return blockMap == null ? buildBlockMap(path) : blockMap;
    }

    private ImmutableRangeMap<Long,BlockLocation> getBlockMap(FileStatus status) throws IOException{
      ImmutableRangeMap<Long,BlockLocation> blockMap = blockMapMap.get(status.getPath());
      return blockMap == null ? buildBlockMap(status) : blockMap;
    }

    /**
     * For a given {@link FileWork}, calculate how many bytes are available on each on drillbit endpoint
     *
     * @param noDrillbitHosts set to gather list of absent drillbit hosts
     * @param work the FileWork to calculate endpoint bytes for
     * @return {@link EndpointByteMap} instance
     */
    private EndpointByteMap getEndpointByteMap(Set<String> noDrillbitHosts, FileWork work) throws IOException {
      Stopwatch watch = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;
      Path fileName = work.getPath();

      ImmutableRangeMap<Long, BlockLocation> blockMap = getBlockMap(fileName);
      EndpointByteMapImpl endpointByteMap = new EndpointByteMapImpl();
      long start = work.getStart();
      long end = start + work.getLength();
      Range<Long> rowGroupRange = Range.closedOpen(start, end);

      // Find sub map of ranges that intersect with the rowGroup
      ImmutableRangeMap<Long, BlockLocation> subRangeMap = blockMap.subRangeMap(rowGroupRange);

      // Iterate through each block in this sub map and get the host for the block location
      for (Map.Entry<Range<Long>, BlockLocation> block : subRangeMap.asMapOfRanges().entrySet()) {
        Range<Long> blockRange = block.getKey();
        String[] hosts = block.getValue().getHosts();
        Range<Long> intersection = rowGroupRange.intersection(blockRange);
        long bytes = intersection.upperEndpoint() - intersection.lowerEndpoint();

        // For each host in the current block location, add the intersecting bytes to the corresponding endpoint
        for (String host : hosts) {
          DrillbitEndpoint endpoint = endPointMap.get(host);
          if (endpoint != null) {
            endpointByteMap.add(endpoint, bytes);
          } else if (noDrillbitHosts != null) {
            noDrillbitHosts.add(host);
          }
        }
      }

      if (watch != null) {
        logger.debug("FileWork group ({},{}) max bytes {}", work.getPath(), work.getStart(), endpointByteMap.getMaxBytes());
        logger.debug("Took {} ms to set endpoint bytes", watch.stop().elapsed(TimeUnit.MILLISECONDS));
      }
      return endpointByteMap;
    }

  }

  private class FileStatusWork implements FileWork {

    private final FileStatus status;

    FileStatusWork(FileStatus status) {
      Preconditions.checkArgument(!status.isDirectory(), "FileStatus work only works with files, not directories.");
      this.status = status;
    }

    @Override
    public Path getPath() {
      return status.getPath();
    }

    @Override
    public long getStart() {
      return 0;
    }

    @Override
    public long getLength() {
      return status.getLen();
    }
  }


}
