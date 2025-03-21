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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig.ClosureCleanerLevel;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/** The {@link ConfigOption configuration options} for job execution. */
@PublicEvolving
public class PipelineOptions {

    /** The job name used for printing and logging. */
    public static final ConfigOption<String> NAME =
            key("pipeline.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The job name used for printing and logging.");

    /**
     * A list of jar files that contain the user-defined function (UDF) classes and all classes used
     * from within the UDFs.
     */
    public static final ConfigOption<List<String>> JARS =
            key("pipeline.jars")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "A semicolon-separated list of the jars to package with the job jars to be sent to the"
                                    + " cluster. These have to be valid paths.");

    /**
     * A list of URLs that are added to the classpath of each user code classloader of the program.
     * Paths must specify a protocol (e.g. file://) and be accessible on all nodes
     */
    public static final ConfigOption<List<String>> CLASSPATHS =
            key("pipeline.classpaths")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "A semicolon-separated list of the classpaths to package with the job jars to be sent to"
                                    + " the cluster. These have to be valid URLs.");

    public static final ConfigOption<Boolean> AUTO_GENERATE_UIDS =
            key("pipeline.auto-generate-uids")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "When auto-generated UIDs are disabled, users are forced to manually specify UIDs on DataStream applications.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "It is highly recommended that users specify UIDs before deploying to"
                                                    + " production since they are used to match state in savepoints to operators"
                                                    + " in a job. Because auto-generated ID's are likely to change when modifying"
                                                    + " a job, specifying custom IDs allow an application to evolve over time"
                                                    + " without discarding state.")
                                    .build());

    public static final ConfigOption<Duration> AUTO_WATERMARK_INTERVAL =
            key("pipeline.auto-watermark-interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(200))
                    .withDescription(
                            "The interval of the automatic watermark emission. Watermarks are used throughout"
                                    + " the streaming system to keep track of the progress of time. They are used, for example,"
                                    + " for time based windowing.");

    public static final ConfigOption<ClosureCleanerLevel> CLOSURE_CLEANER_LEVEL =
            key("pipeline.closure-cleaner-level")
                    .enumType(ClosureCleanerLevel.class)
                    .defaultValue(ClosureCleanerLevel.RECURSIVE)
                    .withDescription("Configures the mode in which the closure cleaner works.");

    public static final ConfigOption<Boolean> FORCE_AVRO =
            key("pipeline.force-avro")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Forces Flink to use the Apache Avro serializer for POJOs.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Important: Make sure to include the %s module.",
                                            code("flink-avro"))
                                    .build());

    public static final ConfigOption<Boolean> FORCE_KRYO =
            key("pipeline.force-kryo")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If enabled, forces TypeExtractor to use Kryo serializer for POJOS even though we could"
                                    + " analyze as POJO. In some cases this might be preferable. For example, when using interfaces"
                                    + " with subclasses that cannot be analyzed as POJO.");

    public static final ConfigOption<Boolean> FORCE_KRYO_AVRO =
            key("pipeline.force-kryo-avro")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Forces Flink to register avro classes in kryo serializer.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Important: Make sure to include the flink-avro module."
                                                    + " Otherwise, nothing will be registered. For backward compatibility,"
                                                    + " the default value is empty to conform to the behavior of the older version."
                                                    + " That is, always register avro with kryo, and if flink-avro is not in the class"
                                                    + " path, register a dummy serializer. In Flink-2.0, we will set the default value to true.")
                                    .build());

    public static final ConfigOption<Boolean> GENERIC_TYPES =
            key("pipeline.generic-types")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If the use of generic types is disabled, Flink will throw an %s whenever it encounters"
                                                    + " a data type that would go through Kryo for serialization.",
                                            code("UnsupportedOperationException"))
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Disabling generic types can be helpful to eagerly find and eliminate the use of types"
                                                    + " that would go through Kryo serialization during runtime. Rather than checking types"
                                                    + " individually, using this option will throw exceptions eagerly in the places where generic"
                                                    + " types are used.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "We recommend to use this option only during development and pre-production"
                                                    + " phases, not during actual production use. The application program and/or the input data may be"
                                                    + " such that new, previously unseen, types occur at some point. In that case, setting this option"
                                                    + " would cause the program to fail.")
                                    .build());

    public static final ConfigOption<Map<String, String>> GLOBAL_JOB_PARAMETERS =
            key("pipeline.global-job-parameters")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Register a custom, serializable user configuration object. The configuration can be "
                                    + " accessed in operators");

    public static final ConfigOption<Map<String, String>> PARALLELISM_OVERRIDES =
            key("pipeline.jobvertex-parallelism-overrides")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "A parallelism override map (jobVertexId -> parallelism) which will be used to update"
                                    + " the parallelism of the corresponding job vertices of submitted JobGraphs.");

    public static final ConfigOption<Integer> MAX_PARALLELISM =
            key("pipeline.max-parallelism")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The program-wide maximum parallelism used for operators which haven't specified a"
                                    + " maximum parallelism. The maximum parallelism specifies the upper limit for dynamic scaling and"
                                    + " the number of key groups used for partitioned state."
                                    + " Changing the value explicitly when recovery from original job will lead to state incompatibility."
                                    + " Must be less than or equal to 32768.");

    public static final ConfigOption<Boolean> OBJECT_REUSE =
            key("pipeline.object-reuse")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When enabled objects that Flink internally uses for deserialization and passing"
                                    + " data to user-code functions will be reused. Keep in mind that this can lead to bugs when the"
                                    + " user-code function of an operation is not aware of this behaviour.");

    public static final ConfigOption<List<String>> SERIALIZATION_CONFIG =
            key("pipeline.serialization-config")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "List of pairs of class names and serializer configs to be used."
                                                    + " There is a %s field in the serializer config and each type has its own configuration."
                                                    + " Note: only standard YAML config parser is supported, please use \"config.yaml\" as the config file."
                                                    + " The fields involved are:",
                                            code("type"))
                                    .list(
                                            text(
                                                    "%s: the serializer type which could be \"pojo\", \"kryo\" or \"typeinfo\"."
                                                            + " If the serializer type is \"pojo\" or \"kryo\" without field %s,"
                                                            + " it means the data type will use POJO or Kryo serializer directly.",
                                                    code("type"), code("kryo-type")),
                                            text(
                                                    "%s: the Kryo serializer type which could be \"default\" or \"registered\"."
                                                            + " The Kryo serializer will use the serializer for the data type "
                                                            + " as default serializers when the kryo-type is \"default\", and register"
                                                            + " the data type and its serializer to Kryo serializer when the kryo-type is registered."
                                                            + " When the field exists, there must be a field %s to specify the serializer class name.",
                                                    code("kryo-type"), code("class")),
                                            text(
                                                    "%s: the serializer class name for type \"kryo\" or \"typeinfo\"."
                                                            + " For \"kryo\", it should be a subclass of %s."
                                                            + " For \"typeinfo\", it should be a subclass of %s.",
                                                    code("class"),
                                                    code("com.esotericsoftware.kryo.Serializer"),
                                                    code(
                                                            "org.apache.flink.api.common.typeinfo.TypeInfoFactory")))
                                    .text("Example:")
                                    .linebreak()
                                    .add(
                                            TextElement.code(
                                                    "[org.example.ExampleClass1: {type: pojo},"
                                                            + " org.example.ExampleClass2: {type: kryo},"
                                                            + " org.example.ExampleClass3: {type: kryo, kryo-type: default, class: org.example.Class3KryoSerializer},"
                                                            + " org.example.ExampleClass4: {type: kryo, kryo-type: registered, class: org.example.Class4KryoSerializer},"
                                                            + " org.example.ExampleClass5: {type: typeinfo, class: org.example.Class5TypeInfoFactory}]"))
                                    .build());

    public static final ConfigOption<Boolean> OPERATOR_CHAINING =
            key("pipeline.operator-chaining.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys("pipeline.operator-chaining")
                    .withDescription(
                            "Operator chaining allows non-shuffle operations to be co-located in the same thread "
                                    + "fully avoiding serialization and de-serialization.");

    public static final ConfigOption<Boolean>
            OPERATOR_CHAINING_CHAIN_OPERATORS_WITH_DIFFERENT_MAX_PARALLELISM =
                    key("pipeline.operator-chaining.chain-operators-with-different-max-parallelism")
                            .booleanType()
                            .defaultValue(true)
                            .withDescription(
                                    "Operators with different max parallelism can be chained together. Default behavior may prevent rescaling when the AdaptiveScheduler is used.");

    public static final ConfigOption<List<String>> CACHED_FILES =
            key("pipeline.cached-files")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Files to be registered at the distributed cache under the given name. The files will be "
                                                    + "accessible from any user-defined function in the (distributed) runtime under a local path. "
                                                    + "Files may be local files (which will be distributed via BlobServer), or files in a distributed "
                                                    + "file system. The runtime will copy the files temporarily to a local cache, if needed.")
                                    .linebreak()
                                    .linebreak()
                                    .text("Example:")
                                    .linebreak()
                                    .add(
                                            TextElement.code(
                                                    "name:file1,path:'file:///tmp/file1';name:file2,path:'hdfs:///tmp/file2'"))
                                    .build());

    public static final ConfigOption<VertexDescriptionMode> VERTEX_DESCRIPTION_MODE =
            key("pipeline.vertex-description-mode")
                    .enumType(VertexDescriptionMode.class)
                    .defaultValue(VertexDescriptionMode.TREE)
                    .withDescription("The mode how we organize description of a job vertex.");

    /** The mode how we organize description of a vertex. */
    @PublicEvolving
    public enum VertexDescriptionMode {
        /** Organizes the description in a multi line tree mode. */
        TREE,
        /** Organizes the description in a single line cascading mode, which is similar to name. */
        CASCADING
    }

    public static final ConfigOption<Boolean> VERTEX_NAME_INCLUDE_INDEX_PREFIX =
            key("pipeline.vertex-name-include-index-prefix")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether name of vertex includes topological index or not. "
                                    + "When it is true, the name will have a prefix of index of the vertex, like '[vertex-0]Source: source'. It is false by default");

    /** Will be removed in future Flink releases. */
    public static final ConfigOption<Boolean> ALLOW_UNALIGNED_SOURCE_SPLITS =
            key("pipeline.watermark-alignment.allow-unaligned-source-splits")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If watermark alignment is used, sources with multiple splits will "
                                    + "attempt to pause/resume split readers to avoid watermark "
                                    + "drift of source splits. "
                                    + "However, if split readers don't support pause/resume, an "
                                    + "UnsupportedOperationException will be thrown when there is "
                                    + "an attempt to pause/resume. To allow use of split readers that "
                                    + "don't support pause/resume and, hence, to allow unaligned splits "
                                    + "while still using watermark alignment, set this parameter to true. "
                                    + "The default value is false. Note: This parameter may be "
                                    + "removed in future releases.");
}
