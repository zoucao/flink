package org.apache.flink.connector.file.src.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogPartitionSpec;

import javax.annotation.Nullable;

import java.util.function.Function;

@PublicEvolving
/**
 * wrap the partition pruning function for continuous file source.
 */
public class PartitionPruningWrapper {

    @Nullable
    private final FilterFunction<CatalogPartitionSpec> partitionsPruningFunction;

    private Function<CatalogPartitionSpec, Boolean> pruningFunction;

    private PartitionPruningWrapper(@Nullable FilterFunction<CatalogPartitionSpec> partitionsPruningFunction) {
        this.partitionsPruningFunction = partitionsPruningFunction;
        this.pruningFunction = partitionsPruningFunction == null ?
                (spec) -> true :
                (spec) -> {
                    try {
                        return partitionsPruningFunction.filter(spec);
                    } catch (Exception e) {
                       throw new RuntimeException(e);
                    }
                };
    }

    public void open() {
        if (partitionsPruningFunction != null && partitionsPruningFunction instanceof RichFilterFunction) {
            try {
                ((RichFilterFunction) partitionsPruningFunction).open(new Configuration());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public boolean prune(CatalogPartitionSpec spec) {
        return pruningFunction.apply(spec);
    }

    public static PartitionPruningWrapper wrap(@Nullable FilterFunction<CatalogPartitionSpec> partitionsPruningFunction) {
        return new PartitionPruningWrapper(partitionsPruningFunction);
    }



}
