/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.connector.file.src.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogPartitionSpec;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.function.Function;

@PublicEvolving
/** wrap the partition pruning function for continuous file source. */
public class PartitionPruningWrapper implements Serializable {

    @Nullable private final FilterFunction<CatalogPartitionSpec> partitionsPruningFunction;

    private transient Function<CatalogPartitionSpec, Boolean> pruningFunction;

    private PartitionPruningWrapper(
            @Nullable FilterFunction<CatalogPartitionSpec> partitionsPruningFunction) {
        this.partitionsPruningFunction = partitionsPruningFunction;

    }

    public void open() {
        if (partitionsPruningFunction != null
                && partitionsPruningFunction instanceof RichFilterFunction) {
            try {
                ((RichFilterFunction) partitionsPruningFunction).open(new Configuration());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        this.pruningFunction =
                partitionsPruningFunction == null
                        ? (spec) -> true
                        : (spec) -> {
                    try {
                        return partitionsPruningFunction.filter(spec);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
    }

    public boolean prune(CatalogPartitionSpec spec) {
        return pruningFunction.apply(spec);
    }

    public static PartitionPruningWrapper wrap(
            @Nullable FilterFunction<CatalogPartitionSpec> partitionsPruningFunction) {
        return new PartitionPruningWrapper(partitionsPruningFunction);
    }



}
