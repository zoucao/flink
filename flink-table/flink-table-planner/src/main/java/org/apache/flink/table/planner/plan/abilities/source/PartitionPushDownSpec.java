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

package org.apache.flink.table.planner.plan.abilities.source;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.PartitionPruner;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A sub-class of {@link SourceAbilitySpec} that can not only serialize/deserialize the partitions
 * to/from JSON, but also can push the partitions into a {@link SupportsPartitionPushDown}.
 */
@JsonTypeName("PartitionPushDown")
public final class PartitionPushDownSpec extends SourceAbilitySpecBase {
    public static final String FIELD_NAME_PARTITIONS = "partitions";

    public static final String REMAINING_PARTITIONS = "remainingPartitions";
    public static final String FIELD_NAME_PREDICATES = "predicates";

    @JsonProperty(REMAINING_PARTITIONS)
    private final List<Map<String, String>> remainingPartitions;

    @JsonProperty(FIELD_NAME_PARTITIONS)
    private final List<String> partitions;

    @JsonProperty(FIELD_NAME_PREDICATES)
    private final RexNode predicates;

    @JsonCreator
    public PartitionPushDownSpec(
            @JsonProperty(FIELD_NAME_PARTITIONS) List<String> partitions,
            @JsonProperty(REMAINING_PARTITIONS) List<Map<String, String>> remainingPartitions,
            @JsonProperty(FIELD_NAME_PREDICATES) RexNode predicates) {
        this.partitions = partitions;
        this.remainingPartitions = new ArrayList<>(checkNotNull(remainingPartitions));
        this.predicates = predicates;
    }

    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {
        if (tableSource instanceof SupportsPartitionPushDown) {
            ((SupportsPartitionPushDown) tableSource).applyPartitions(remainingPartitions);
            if (!context.isBatchMode()) {
                List<String> inputFieldNames = context.getSourceRowType().getFieldNames();
                List<RowType.RowField> rowFields = context.getSourceRowType().getFields();
                LogicalType[] partitionFieldTypes =
                        partitions.stream()
                                .map(
                                        name -> {
                                            int index = inputFieldNames.indexOf(name);
                                            if (index < 0) {
                                                throw new TableException(
                                                        String.format(
                                                                "Partitioned key '%s' isn't found in input columns. "
                                                                        + "It should be checked before.",
                                                                name));
                                            }
                                            return rowFields.get(index).getType();
                                        })
                                .toArray(LogicalType[]::new);
                RichFilterFunction<CatalogPartitionSpec> pruningFunction =
                        PartitionPruner.generatePruningFunction(
                                context.getTableConfig(),
                                context.getClassLoader(),
                                partitions.toArray(new String[0]),
                                partitionFieldTypes,
                                predicates);
                ((SupportsPartitionPushDown) tableSource)
                        .applyPartitionPuringFunction(pruningFunction);
            }

        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsPartitionPushDown.",
                            tableSource.getClass().getName()));
        }
    }

    public List<Map<String, String>> getPartitions() {
        return remainingPartitions;
    }

    @Override
    public String getDigests(SourceAbilityContext context) {
        String digests = "";
        if (context.isBatchMode()) {
            digests =
                    this.partitions.stream()
                            .map(Object::toString)
                            .collect(Collectors.joining(", "));
        } else {
            final RowType sourceRowType = context.getSourceRowType();
            digests =
                    FlinkRexUtil.getExpressionString(
                            predicates,
                            JavaScalaConversionUtil.toScala(sourceRowType.getFieldNames()));
        }

        return "partitions=[" + digests + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PartitionPushDownSpec that = (PartitionPushDownSpec) o;
        return Objects.equals(partitions, that.partitions)
                && Objects.equals(remainingPartitions, that.remainingPartitions)
                && Objects.equals(predicates, that.predicates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), partitions, remainingPartitions, predicates);
    }
}
