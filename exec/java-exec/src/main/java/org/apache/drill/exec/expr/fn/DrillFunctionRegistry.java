/**
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
package org.apache.drill.exec.expr.fn;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.ArrayListMultimap;
import org.apache.calcite.sql.SqlOperator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.scanner.persistence.AnnotatedClassDescriptor;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.planner.sql.DrillSqlAggOperator;
import org.apache.drill.exec.planner.sql.DrillSqlAggOperatorWithoutInference;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;

import org.apache.drill.exec.planner.sql.DrillSqlOperatorWithoutInference;

/**
 * Registry of Drill functions.
 */
public class DrillFunctionRegistry {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFunctionRegistry.class);
  private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private static final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private static final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

  // key: function name (lowercase) value: list of functions with that name
  private final ListMultimap<String, DrillFuncHolder> registeredFunctions = ArrayListMultimap.create();

  // Hash map to prevent registering functions with exactly matching signatures
  // key: Function Name + Input's Major Type
  // value: Class name where function is implemented
  private final Map<String, String> functionSignatureMap = Maps.newConcurrentMap();

  // Set to prevent registering functions from the same jar two times
  private final Set<String> uniquePaths = Sets.newConcurrentHashSet();

  private static final ImmutableMap<String, Pair<Integer, Integer>> registeredFuncNameToArgRange = ImmutableMap.<String, Pair<Integer, Integer>> builder()
      // CONCAT is allowed to take [1, infinity) number of arguments.
      // Currently, this flexibility is offered by DrillOptiq to rewrite it as
      // a nested structure
      .put("CONCAT", Pair.of(1, Integer.MAX_VALUE))

          // When LENGTH is given two arguments, this function relies on DrillOptiq to rewrite it as
          // another function based on the second argument (encodingType)
      .put("LENGTH", Pair.of(1, 2))

          // Dummy functions
      .put("CONVERT_TO", Pair.of(2, 2))
      .put("CONVERT_FROM", Pair.of(2, 2))
      .put("FLATTEN", Pair.of(1, 1)).build();

  public DrillFunctionRegistry(ScanResult classpathScan) {
    registerFunctions(classpathScan);
    if (logger.isTraceEnabled()) {
      StringBuilder allFunctions = new StringBuilder();
      readLock.lock();
      try {
        for (DrillFuncHolder method : registeredFunctions.values()) {
          allFunctions.append(method.toString()).append("\n");
        }
      } finally {
        readLock.unlock();
      }
      logger.trace("Registered functions: [\n{}]", allFunctions);
    }
  }

  public int size(){
    readLock.lock();
    try {
      return registeredFunctions.size();
    } finally {
      readLock.unlock();
    }
  }

  /** Returns functions with given name. Function name is case insensitive. */
  public List<DrillFuncHolder> getMethods(String name) {
    readLock.lock();
    try {
      return Lists.newArrayList(this.registeredFunctions.get(name.toLowerCase()));
    } finally {
      readLock.unlock();
    }
  }

  public void register(DrillOperatorTable operatorTable) {
    registerOperatorsWithInference(operatorTable);
    registerOperatorsWithoutInference(operatorTable);
  }

  /**
   * Registers functions
   * @param classpathScan scan result of classpath
   * @return list of added functions
   */
  public Collection<String> registerFunctions(ScanResult classpathScan) {
    RegistryHelper registerFunctions = new RegistryHelper() {
      @Override
      void doWork(String functionName, String functionSignature, DrillFuncHolder holder, AnnotatedClassDescriptor func) {
        readLock.lock();
        try {
          String existingImplementation;
          if (tempUniquePaths.contains(holder.getPath())) {
            throw new AssertionError(String.format("Functions from the same %s have been already registered." +
                    " Func Name: %s, Class name: %s ", holder.getPath(), functionName, func.getClassName()));
          } else if ((existingImplementation = functionSignatureMap.get(functionSignature)) != null ||
              (existingImplementation = tempFunctionSignatureMap.get(functionSignature)) != null) {
            throw new AssertionError(String.format("Conflicting functions with similar signature found." +
                    " Func Name: %s, Class name: %s " + " Class name: %s",
                functionName, func.getClassName(), existingImplementation));
          } else if (holder.isAggregating() && !holder.isDeterministic()) {
            logger.warn("Aggregate functions must be deterministic, did not register function {}", func.getClassName());
          } else {
            tempFunctionSignatureMap.put(functionSignature, func.getClassName());
            tempRegisteredFunctions.put(functionName, holder);
            tempUniquePaths.add(holder.getPath());
          }
        } finally {
          readLock.unlock();
        }
      }

      @Override
      Collection<String> getResult() {
        functionSignatureMap.putAll(tempFunctionSignatureMap);
        uniquePaths.addAll(tempUniquePaths);

        writeLock.lock();
        try {
          registeredFunctions.putAll(tempRegisteredFunctions);
        } finally {
          writeLock.unlock();
        }

        return tempRegisteredFunctions.keySet();
      }
    };
    return registerFunctions.apply(classpathScan);
  }
/*
  public Collection<String> registerFunctions(ScanResult classpathScan) {
    FunctionConverter converter = new FunctionConverter();
    List<AnnotatedClassDescriptor> providerClasses = classpathScan.getAnnotatedClasses();
    final Map<String, String> tempFunctionSignatureMap = Maps.newHashMap();
    final ArrayListMultimap<String, DrillFuncHolder> tempRegisteredFunctions = ArrayListMultimap.create();
    for (AnnotatedClassDescriptor func : providerClasses) {
      DrillFuncHolder holder = converter.getHolder(func);
      if (holder != null) {
        // register handle for each name the function can be referred to
        String[] names = holder.getRegisteredNames();

        // Create the string for input types
        String functionInput = "";
        for (DrillFuncHolder.ValueReference ref : holder.parameters) {
          functionInput += ref.getType().toString();
        }
        for (String name : names) {
          String functionName = name.toLowerCase();
          // used to be here //todo remove upon final check
          // registeredFunctions.put(functionName, holder);
          String functionSignature = functionName + functionInput;
          String existingImplementation;
          if ((existingImplementation = functionSignatureMap.get(functionSignature)) != null ||
              (existingImplementation = tempFunctionSignatureMap.get(functionSignature)) != null) {
            throw new AssertionError(String.format("Conflicting functions with similar signature found." +
                " Func Name: %s, Class name: %s " + " Class name: %s",
                functionName, func.getClassName(), existingImplementation));
          } else if (holder.isAggregating() && !holder.isDeterministic()) {
            logger.warn("Aggregate functions must be deterministic, did not register function {}", func.getClassName());
          } else {
            tempFunctionSignatureMap.put(functionSignature, func.getClassName());
            tempRegisteredFunctions.put(functionName, holder);
          }
        }
      } else {
        logger.warn("Unable to initialize function for class {}", func.getClassName());
      }
    }

    functionSignatureMap.putAll(tempFunctionSignatureMap);
    registeredFunctions.putAll(tempRegisteredFunctions);

    return registeredFunctions.keySet(); //todo check how functions are displayed, probably use info from functionSignatureMap
  }*/

  /**
   * Deletes functions
   * @param classpathScan scan result of classpath
   * @return list of deleted functions
   */
  public Collection<String> deleteFunctions(ScanResult classpathScan) {
    RegistryHelper deleteFunctions = new RegistryHelper() {
      @Override
      public void doWork(String functionName, String functionSignature, DrillFuncHolder holder, AnnotatedClassDescriptor func) {
        if ((func.getClassName().equals(functionSignatureMap.get(functionSignature)))) {
          String removedSignature;
          if ((removedSignature = functionSignatureMap.remove(functionSignature)) != null) {
            tempFunctionSignatureMap.put(functionSignature, removedSignature);
          }
          // prepare holder parameters
          List<TypeProtos.MajorType> argTypes = Lists.newArrayList();
          for (DrillFuncHolder.ValueReference ref : holder.getParameters()) {
            argTypes.add(ref.getType());
          }

          if (uniquePaths.containsAll(tempUniquePaths)) {
            writeLock.lock();
            try {

              List<DrillFuncHolder> drillFuncHolders = registeredFunctions.get(functionName);

              // start new logic //todo discuss new logic
              for (DrillFuncHolder holderToDelete : drillFuncHolders) {
                if (tempUniquePaths.contains(holderToDelete.getPath())) {
                  drillFuncHolders.remove(holderToDelete);
                  tempRegisteredFunctions.put(functionName, holderToDelete);
                }
              }
              uniquePaths.removeAll(tempUniquePaths);
              // end new logic

              // old logic start
/*              DrillFuncHolder holderToDelete = null;
              for (DrillFuncHolder h : drillFuncHolders) {
                if (h.matches(holder.getReturnType(), argTypes)) {
                  holderToDelete = h;
                }
              }
              if (drillFuncHolders.remove(holderToDelete)) {
                tempRegisteredFunctions.put(functionName, holderToDelete);
              }*/

            } finally {
              writeLock.unlock();
            }
          }
        }
      }

      @Override
      Collection<String> getResult() {
        return tempRegisteredFunctions.keySet();
      }
    };
    return deleteFunctions.apply(classpathScan);
  }
/*  public Collection<String> deleteFunctions(ScanResult classpathScan) {
    FunctionConverter converter = new FunctionConverter();
    List<AnnotatedClassDescriptor> providerClasses = classpathScan.getAnnotatedClasses();
    for (AnnotatedClassDescriptor func : providerClasses) {
      DrillFuncHolder holder = converter.getHolder(func);
      if (holder != null) {
        // register handle for each name the function can be referred to
        String[] names = holder.getRegisteredNames();

        // Create the string for input types
        String functionInput = "";
        for (DrillFuncHolder.ValueReference ref : holder.parameters) {
          functionInput += ref.getType().toString();
        }
        for (String name : names) {
          String functionName = name.toLowerCase();
          String functionSignature = functionName + functionInput;
          if ((func.getClassName().equals(functionSignatureMap.get(functionSignature)))) {
            functionSignatureMap.remove(functionSignature);
            List<DrillFuncHolder> drillFuncHolders = registeredFunctions.get(functionName);
            // prepare holder parameters
            List<TypeProtos.MajorType> argTypes = Lists.newArrayList();
            for (DrillFuncHolder.ValueReference ref : holder.getParameters()) {
              argTypes.add(ref.getType());
            }

            DrillFuncHolder holderToDelete = null;
            for (DrillFuncHolder h : drillFuncHolders) {
              if (h.matches(holder.getReturnType(), argTypes)) {
                holderToDelete = h;
              }
            }
            drillFuncHolders.remove(holderToDelete);
          }
        }
      } else {
        logger.warn("Unable to initialize function for class {}", func.getClassName());
      }
      //todo add return with status of removed functions
    }
    return null;
  }*/

  /*

   public DrillFuncHolder findExactMatchingDrillFunction(String name, List<MajorType> argTypes, MajorType returnType) {
    for (DrillFuncHolder h : drillFuncRegistry.getMethods(name)) {
      if (h.matches(returnType, argTypes)) {
        return h;
      }
    }

    return null;
  }

   */

  private void registerOperatorsWithInference(DrillOperatorTable operatorTable) {
    final Map<String, DrillSqlOperator.DrillSqlOperatorBuilder> map = Maps.newHashMap();
    final Map<String, DrillSqlAggOperator.DrillSqlAggOperatorBuilder> mapAgg = Maps.newHashMap();
    readLock.lock();
    try {
      for (Entry<String, Collection<DrillFuncHolder>> function : registeredFunctions.asMap().entrySet()) {
        final ArrayListMultimap<Pair<Integer, Integer>, DrillFuncHolder> functions = ArrayListMultimap.create();
        final ArrayListMultimap<Integer, DrillFuncHolder> aggregateFunctions = ArrayListMultimap.create();
        final String name = function.getKey().toUpperCase();
        boolean isDeterministic = true;
        for (DrillFuncHolder func : function.getValue()) {
          final int paramCount = func.getParamCount();
          if (func.isAggregating()) {
            aggregateFunctions.put(paramCount, func);
          } else {
            final Pair<Integer, Integer> argNumberRange;
            if (registeredFuncNameToArgRange.containsKey(name)) {
              argNumberRange = registeredFuncNameToArgRange.get(name);
            } else {
              argNumberRange = Pair.of(func.getParamCount(), func.getParamCount());
            }
            functions.put(argNumberRange, func);
          }

          if (!func.isDeterministic()) {
            isDeterministic = false;
          }
        }
        for (Entry<Pair<Integer, Integer>, Collection<DrillFuncHolder>> entry : functions.asMap().entrySet()) {
          final Pair<Integer, Integer> range = entry.getKey();
          final int max = range.getRight();
          final int min = range.getLeft();
          if (!map.containsKey(name)) {
            map.put(name, new DrillSqlOperator.DrillSqlOperatorBuilder()
                .setName(name));
          }

          final DrillSqlOperator.DrillSqlOperatorBuilder drillSqlOperatorBuilder = map.get(name);
          drillSqlOperatorBuilder
              .addFunctions(entry.getValue())
              .setArgumentCount(min, max)
              .setDeterministic(isDeterministic);
        }
        for (Entry<Integer, Collection<DrillFuncHolder>> entry : aggregateFunctions.asMap().entrySet()) {
          if (!mapAgg.containsKey(name)) {
            mapAgg.put(name, new DrillSqlAggOperator.DrillSqlAggOperatorBuilder().setName(name));
          }

          final DrillSqlAggOperator.DrillSqlAggOperatorBuilder drillSqlAggOperatorBuilder = mapAgg.get(name);
          drillSqlAggOperatorBuilder
              .addFunctions(entry.getValue())
              .setArgumentCount(entry.getKey(), entry.getKey());
        }
      }
    } finally {
      readLock.unlock();
    }

    for(final Entry<String, DrillSqlOperator.DrillSqlOperatorBuilder> entry : map.entrySet()) {
      operatorTable.addOperatorWithInference(
          entry.getKey(),
          entry.getValue().build());
    }

    for(final Entry<String, DrillSqlAggOperator.DrillSqlAggOperatorBuilder> entry : mapAgg.entrySet()) {
      operatorTable.addOperatorWithInference(
          entry.getKey(),
          entry.getValue().build());
    }
  }

  private void registerOperatorsWithoutInference(DrillOperatorTable operatorTable) {
    SqlOperator op;
    readLock.lock();
    try {
      for (Entry<String, Collection<DrillFuncHolder>> function : registeredFunctions.asMap().entrySet()) {
        Set<Integer> argCounts = Sets.newHashSet();
        String name = function.getKey().toUpperCase();
        for (DrillFuncHolder func : function.getValue()) {
          if (argCounts.add(func.getParamCount())) {
            if (func.isAggregating()) {
              op = new DrillSqlAggOperatorWithoutInference(name, func.getParamCount());
            } else {
              boolean isDeterministic;
              // prevent Drill from folding constant functions with types that cannot be materialized
              // into literals
              if (DrillConstExecutor.NON_REDUCIBLE_TYPES.contains(func.getReturnType().getMinorType())) {
                isDeterministic = false;
              } else {
                isDeterministic = func.isDeterministic();
              }
              op = new DrillSqlOperatorWithoutInference(name, func.getParamCount(), func.getReturnType(), isDeterministic);
            }
            operatorTable.addOperatorWithoutInference(function.getKey(), op);
          }
        }
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Helper class to work with function registry holders using classpath scan result.
   */
  private abstract class RegistryHelper {
    final Map<String, String> tempFunctionSignatureMap;
    final ListMultimap<String, DrillFuncHolder> tempRegisteredFunctions;
    final Set<String> tempUniquePaths;

    RegistryHelper() {
      tempFunctionSignatureMap = Maps.newHashMap();
      tempRegisteredFunctions = ArrayListMultimap.create();
      tempUniquePaths = Sets.newConcurrentHashSet();
    }

    /**
     * Upon classpath scan result gets list of found function and applies certain actions on each function.
     * Provides ability to finalize work on all functions.
     * @param classpathScan classpath scan result
     * @return list of affected functions
     */
    final Collection<String> apply(ScanResult classpathScan) {
      FunctionConverter converter = new FunctionConverter();
      List<AnnotatedClassDescriptor> providerClasses = classpathScan.getAnnotatedClasses();

      for (AnnotatedClassDescriptor func : providerClasses) {
        DrillFuncHolder holder = converter.getHolder(func);
        if (holder != null) {
          // get function path
          tempUniquePaths.add(holder.getPath());

          // register handle for each name the function can be referred to
          String[] names = holder.getRegisteredNames();

          // Create the string for input types
          String functionInput = "";
          for (DrillFuncHolder.ValueReference ref : holder.parameters) {
            functionInput += ref.getType().toString();
          }
          for (String name : names) {
            String functionName = name.toLowerCase();
            // used to be here //todo remove upon final check
            // registeredFunctions.put(functionName, holder);
            String functionSignature = functionName + functionInput;
            doWork(functionName, functionSignature, holder, func);
          }
        } else {
          logger.warn("Unable to initialize function for class {}", func.getClassName());
        }
      }
      return getResult();
    }

    /**
     * Implement to apply certain actions on each function.
     */
    abstract void doWork(String functionName, String functionSignature, DrillFuncHolder holder, AnnotatedClassDescriptor func);

    /**
     * Implement to finalize work on found functions.
     * @return list of affected functions
     */
    abstract Collection<String> getResult();
  }
}