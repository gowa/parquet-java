/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.proto;

import static java.util.Optional.ofNullable;
import static org.apache.parquet.proto.ProtoConstants.METADATA_ENUM_ITEM_SEPARATOR;
import static org.apache.parquet.proto.ProtoConstants.METADATA_ENUM_KEY_VALUE_SEPARATOR;
import static org.apache.parquet.proto.ProtoConstants.METADATA_ENUM_PREFIX;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.util.Timestamps;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import com.twitter.elephantbird.util.Protobufs;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.MemberSubstitution;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.Generic.OfNonGenericType.ForLoadedType;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.Implementation.Context;
import net.bytebuddy.implementation.InvokeDynamic;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.Removal;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.StackSize;
import net.bytebuddy.implementation.bytecode.assign.InstanceCheck;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import net.bytebuddy.implementation.bytecode.collection.ArrayFactory;
import net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import net.bytebuddy.implementation.bytecode.constant.JavaConstantValue;
import net.bytebuddy.implementation.bytecode.constant.TextConstant;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.Handle;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.proto.ProtoWriteSupport.ByteBuddyProto3FastMessageWriters.MessageWriterVisitor.FieldPath;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.IncompatibleSchemaModificationException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link WriteSupport} for writing Protocol Buffers.
 */
public class ProtoWriteSupport<T extends MessageOrBuilder> extends WriteSupport<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoWriteSupport.class);
  public static final String PB_CLASS_WRITE = "parquet.proto.writeClass";
  // PARQUET-968 introduces changes to allow writing specs compliant schemas with parquet-protobuf.
  // In the past, collection were not written using the LIST and MAP wrappers and thus were not compliant
  // with the parquet specs. This flag, is set to true, allows to write using spec compliant schemas
  // but is set to false by default to keep backward compatibility.
  public static final String PB_SPECS_COMPLIANT_WRITE = "parquet.proto.writeSpecsCompliant";

  public static final String PB_UNWRAP_PROTO_WRAPPERS = "parquet.proto.unwrapProtoWrappers";

  public static final String PB_FAST_MESSAGE_WRITER_MODE = "parquet.proto.fastMessageWriterMode";

  private boolean writeSpecsCompliant = false;
  private boolean unwrapProtoWrappers = false;
  private FastMessageWriterMode fastMessageWriterMode = FastMessageWriterMode.CODEGEN;
  private RecordConsumer recordConsumer;
  private Class<? extends Message> protoMessage;
  private Descriptor descriptor;
  private MessageWriter messageWriter;
  // Keep protobuf enum value with number in the metadata, so that in read time, a reader can read at least
  // the number back even with an outdated schema which might not contain all enum values.
  private Map<String, Map<String, Integer>> protoEnumBookKeeper = new HashMap<>();

  public ProtoWriteSupport() {}

  public ProtoWriteSupport(Class<? extends Message> protobufClass) {
    this.protoMessage = protobufClass;
  }

  public ProtoWriteSupport(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public String getName() {
    return "protobuf";
  }

  public static void setSchema(Configuration configuration, Class<? extends Message> protoClass) {
    configuration.setClass(PB_CLASS_WRITE, protoClass, Message.class);
  }

  /**
   * Make parquet-protobuf use the LIST and MAP wrappers for collections. Set to false if you need backward
   * compatibility with parquet before PARQUET-968 (1.9.0 and older).
   *
   * @param configuration       The hadoop configuration
   * @param writeSpecsCompliant If set to true, the old schema style will be used (without wrappers).
   */
  public static void setWriteSpecsCompliant(Configuration configuration, boolean writeSpecsCompliant) {
    configuration.setBoolean(PB_SPECS_COMPLIANT_WRITE, writeSpecsCompliant);
  }

  public static void setUnwrapProtoWrappers(Configuration configuration, boolean unwrapProtoWrappers) {
    configuration.setBoolean(PB_UNWRAP_PROTO_WRAPPERS, unwrapProtoWrappers);
  }

  public enum FastMessageWriterMode {
    OFF,
    REFLECTION,
    CODEGEN,
    BEST
  }

  public static void setFastMessageWriterMode(Configuration configuration, FastMessageWriterMode mode) {
    configuration.set(PB_FAST_MESSAGE_WRITER_MODE, mode == null ? null : mode.name());
  }

  /**
   * Writes Protocol buffer to parquet file.
   *
   * @param record instance of Message.Builder or Message.
   */
  @Override
  public void write(T record) {
    recordConsumer.startMessage();
    try {
      messageWriter.writeTopLevelMessage(record);
    } catch (RuntimeException e) {
      Message m = (record instanceof Message.Builder) ? ((Message.Builder) record).build() : (Message) record;
      LOG.error("Cannot write message {}: {}", e.getMessage(), m);
      throw e;
    }
    recordConsumer.endMessage();
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    return init(new HadoopParquetConfiguration(configuration));
  }

  @Override
  public WriteContext init(ParquetConfiguration configuration) {

    Map<String, String> extraMetaData = new HashMap<>();

    // if no protobuf descriptor was given in constructor, load descriptor from configuration (set with
    // setProtobufClass)
    if (descriptor == null) {
      if (protoMessage == null) {
        Class<? extends Message> pbClass = configuration.getClass(PB_CLASS_WRITE, null, Message.class);
        if (pbClass != null) {
          protoMessage = pbClass;
        } else {
          String msg = "Protocol buffer class or descriptor not specified.";
          String hint =
              " Please use method ProtoParquetOutputFormat.setProtobufClass(...) or other similar method.";
          throw new BadConfigurationException(msg + hint);
        }
      }
      descriptor = Protobufs.getMessageDescriptor(protoMessage);
      extraMetaData.put(ProtoReadSupport.PB_CLASS, protoMessage.getName());
    }

    unwrapProtoWrappers = configuration.getBoolean(PB_UNWRAP_PROTO_WRAPPERS, unwrapProtoWrappers);
    writeSpecsCompliant = configuration.getBoolean(PB_SPECS_COMPLIANT_WRITE, writeSpecsCompliant);
    fastMessageWriterMode = FastMessageWriterMode.valueOf(
        configuration.get(PB_FAST_MESSAGE_WRITER_MODE, fastMessageWriterMode.name()));
    MessageType rootSchema = new ProtoSchemaConverter(configuration).convert(descriptor);
    validatedMapping(descriptor, rootSchema);

    this.messageWriter = new MessageWriter(descriptor, rootSchema, protoMessage);
    createFastMessageWriters(messageWriter);

    extraMetaData.put(ProtoReadSupport.PB_DESCRIPTOR, descriptor.toProto().toString());
    extraMetaData.put(PB_SPECS_COMPLIANT_WRITE, String.valueOf(writeSpecsCompliant));
    extraMetaData.put(PB_UNWRAP_PROTO_WRAPPERS, String.valueOf(unwrapProtoWrappers));
    return new WriteContext(rootSchema, extraMetaData);
  }

  @Override
  public FinalizedWriteContext finalizeWrite() {
    Map<String, String> protoMetadata = enumMetadata();
    return new FinalizedWriteContext(protoMetadata);
  }

  private Map<String, String> enumMetadata() {
    Map<String, String> enumMetadata = new HashMap<>();
    for (Map.Entry<String, Map<String, Integer>> enumNameNumberMapping : protoEnumBookKeeper.entrySet()) {
      StringBuilder nameNumberPairs = new StringBuilder();
      if (enumNameNumberMapping.getValue().isEmpty()) {
        // No enum is ever written to any column of this file, put an empty string as the value in the metadata
        LOG.info("No enum is written for {}", enumNameNumberMapping.getKey());
      }
      int idx = 0;
      for (Map.Entry<String, Integer> nameNumberPair :
          enumNameNumberMapping.getValue().entrySet()) {
        nameNumberPairs
            .append(nameNumberPair.getKey())
            .append(METADATA_ENUM_KEY_VALUE_SEPARATOR)
            .append(nameNumberPair.getValue());
        idx++;
        if (idx < enumNameNumberMapping.getValue().size()) {
          nameNumberPairs.append(METADATA_ENUM_ITEM_SEPARATOR);
        }
      }
      enumMetadata.put(METADATA_ENUM_PREFIX + enumNameNumberMapping.getKey(), nameNumberPairs.toString());
    }
    return enumMetadata;
  }

  class FieldWriter {
    String fieldName;
    int index = -1;
    FieldDescriptor fieldDescriptor;

    void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }

    /**
     * sets index of field inside parquet message.
     */
    void setIndex(int index) {
      this.index = index;
    }

    void setFieldDescriptor(FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
    }

    /**
     * Used for writing repeated fields
     */
    void writeRawValue(Object value) {}

    /**
     * Used for writing nonrepeated (optional, required) fields
     */
    void writeField(Object value) {
      writeBeforeAll();
      writeRawValue(value);
      writeAfterAll();
    }

    void writeBeforeAll() {
      recordConsumer.startField(fieldName, index);
    }

    void writeAfterAll() {
      recordConsumer.endField(fieldName, index);
    }

    java.lang.reflect.Type getFieldReflectionType() {
      throw new IllegalStateException("should override");
    }
  }

  class MessageWriter extends FieldWriter {

    final FieldWriter[] fieldWriters;
    final Class<? extends Message> protoMessageClass;

    FastMessageWriter fastMessageWriter = FastMessageWriter.NOOP;

    @SuppressWarnings("unchecked")
    MessageWriter(Descriptor descriptor, GroupType schema, Class<? extends Message> protoMessageClass) {
      List<FieldDescriptor> fields = descriptor.getFields();
      fieldWriters = (FieldWriter[]) Array.newInstance(FieldWriter.class, fields.size());
      this.protoMessageClass = protoMessageClass;

      for (FieldDescriptor fieldDescriptor : fields) {
        String name = fieldDescriptor.getName();
        Type type = schema.getType(name);

        java.lang.reflect.Type fieldReflectionType =
            ReflectionUtil.getFieldMessageType(fieldDescriptor, protoMessageClass);
        FieldWriter writer = createWriter(
            fieldDescriptor,
            type,
            fieldReflectionType instanceof ReflectionUtil.ArrayReflectionType
                ? ((ReflectionUtil.ArrayReflectionType) fieldReflectionType).getElement()
                : fieldReflectionType);

        if (fieldDescriptor.isRepeated() && !fieldDescriptor.isMapField()) {
          writer = new ArrayWriter(writer, (ReflectionUtil.ArrayReflectionType) fieldReflectionType);
        }

        writer.setFieldName(name);
        writer.setIndex(schema.getFieldIndex(name));
        writer.setFieldDescriptor(fieldDescriptor);

        fieldWriters[fieldDescriptor.getIndex()] = writer;
      }
    }

    @Override
    Class<? extends Message> getFieldReflectionType() {
      return protoMessageClass;
    }

    Optional<? extends Class<? extends MessageOrBuilder>> getProto3MessageOrBuilderInterface() {
      return ReflectionUtil.getProto3MessageOrBuilderInterface(protoMessageClass);
    }

    private FieldWriter createWriter(
        FieldDescriptor fieldDescriptor, Type type, java.lang.reflect.Type fieldReflectionType) {

      switch (fieldDescriptor.getJavaType()) {
        case STRING:
          return new StringWriter();
        case MESSAGE:
          return createMessageWriter(fieldDescriptor, type, fieldReflectionType);
        case INT:
          return new IntWriter();
        case LONG:
          return new LongWriter();
        case FLOAT:
          return new FloatWriter();
        case DOUBLE:
          return new DoubleWriter();
        case ENUM:
          return new EnumWriter(fieldDescriptor.getEnumType(), (Class<?>) fieldReflectionType);
        case BOOLEAN:
          return new BooleanWriter();
        case BYTE_STRING:
          return new BinaryWriter();
      }

      return unknownType(fieldDescriptor); // should not be executed, always throws exception.
    }

    private FieldWriter createMessageWriter(
        FieldDescriptor fieldDescriptor, Type type, java.lang.reflect.Type fieldReflectionType) {
      if (fieldDescriptor.isMapField()) {
        return createMapWriter(fieldDescriptor, type, (ReflectionUtil.MapReflectionType) fieldReflectionType);
      }

      if (unwrapProtoWrappers) {
        Descriptor messageType = fieldDescriptor.getMessageType();
        if (messageType.equals(Timestamp.getDescriptor())) {
          return new TimestampWriter();
        }
        if (messageType.equals(Date.getDescriptor())) {
          return new DateWriter();
        }
        if (messageType.equals(TimeOfDay.getDescriptor())) {
          return new TimeWriter();
        }
        if (messageType.equals(DoubleValue.getDescriptor())) {
          return new DoubleValueWriter();
        }
        if (messageType.equals(FloatValue.getDescriptor())) {
          return new FloatValueWriter();
        }
        if (messageType.equals(Int64Value.getDescriptor())) {
          return new Int64ValueWriter();
        }
        if (messageType.equals(UInt64Value.getDescriptor())) {
          return new UInt64ValueWriter();
        }
        if (messageType.equals(Int32Value.getDescriptor())) {
          return new Int32ValueWriter();
        }
        if (messageType.equals(UInt32Value.getDescriptor())) {
          return new UInt32ValueWriter();
        }
        if (messageType.equals(BoolValue.getDescriptor())) {
          return new BoolValueWriter();
        }
        if (messageType.equals(StringValue.getDescriptor())) {
          return new StringValueWriter();
        }
        if (messageType.equals(BytesValue.getDescriptor())) {
          return new BytesValueWriter();
        }
      }

      // This can happen now that recursive schemas get truncated to bytes.  Write the bytes.
      if (type.isPrimitive()
          && type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
        return new BinaryMessageWriter((Class<? extends Message>) fieldReflectionType);
      }

      return new MessageWriter(fieldDescriptor.getMessageType(), getGroupType(type), (Class<? extends Message>)
          fieldReflectionType);
    }

    private GroupType getGroupType(Type type) {
      LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
      if (logicalTypeAnnotation == null) {
        return type.asGroupType();
      }
      return logicalTypeAnnotation
          .accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<GroupType>() {
            @Override
            public Optional<GroupType> visit(
                LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
              return ofNullable(type.asGroupType()
                  .getType("list")
                  .asGroupType()
                  .getType("element")
                  .asGroupType());
            }

            @Override
            public Optional<GroupType> visit(
                LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
              return ofNullable(type.asGroupType()
                  .getType("key_value")
                  .asGroupType()
                  .getType("value")
                  .asGroupType());
            }
          })
          .orElse(type.asGroupType());
    }

    private MapWriter createMapWriter(
        FieldDescriptor fieldDescriptor, Type type, ReflectionUtil.MapReflectionType mapReflectionType) {
      List<FieldDescriptor> fields = fieldDescriptor.getMessageType().getFields();
      if (fields.size() != 2) {
        throw new UnsupportedOperationException(
            "Expected two fields for the map (key/value), but got: " + fields);
      }

      // KeyFieldWriter
      FieldDescriptor keyProtoField = fields.get(0);
      FieldWriter keyWriter = createWriter(keyProtoField, type, mapReflectionType.getKey());
      keyWriter.setFieldName(keyProtoField.getName());
      keyWriter.setIndex(0);
      keyWriter.setFieldDescriptor(keyProtoField);

      // ValueFieldWriter
      FieldDescriptor valueProtoField = fields.get(1);
      FieldWriter valueWriter = createWriter(
          valueProtoField,
          writeSpecsCompliant ? type : type.asGroupType().getType("value"),
          mapReflectionType.getValue());
      valueWriter.setFieldName(valueProtoField.getName());
      valueWriter.setIndex(1);
      valueWriter.setFieldDescriptor(valueProtoField);

      return new MapWriter(keyWriter, valueWriter, mapReflectionType);
    }

    /**
     * Writes top level message. It cannot call startGroup()
     */
    void writeTopLevelMessage(Object value) {
      writeAllFields((MessageOrBuilder) value);
    }

    /**
     * Writes message as part of repeated field. It cannot start field
     */
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.startGroup();
      writeAllFields((MessageOrBuilder) value);
      recordConsumer.endGroup();
    }

    private void writeAllFields(MessageOrBuilder pb) {
      if (fastMessageWriter.writeAllFields(pb)) {
        return;
      }

      Descriptor messageDescriptor = pb.getDescriptorForType();
      Descriptors.FileDescriptor.Syntax syntax =
          messageDescriptor.getFile().getSyntax();

      if (Descriptors.FileDescriptor.Syntax.PROTO2.equals(syntax)) {
        // Returns changed fields with values. Map is ordered by id.
        Map<FieldDescriptor, Object> changedPbFields = pb.getAllFields();

        for (Map.Entry<FieldDescriptor, Object> entry : changedPbFields.entrySet()) {
          FieldDescriptor fieldDescriptor = entry.getKey();

          if (fieldDescriptor.isExtension()) {
            // Field index of an extension field might overlap with a base field.
            throw new UnsupportedOperationException(
                "Cannot convert Protobuf message with extension field(s)");
          }

          int fieldIndex = fieldDescriptor.getIndex();
          fieldWriters[fieldIndex].writeField(entry.getValue());
        }
      } else if (Descriptors.FileDescriptor.Syntax.PROTO3.equals(syntax)) {
        List<FieldDescriptor> fieldDescriptors = messageDescriptor.getFields();
        for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
          FieldDescriptor.Type type = fieldDescriptor.getType();

          // For a field in a oneOf that isn't set don't write anything
          if (fieldDescriptor.getContainingOneof() != null && !pb.hasField(fieldDescriptor)) {
            continue;
          }

          if (!fieldDescriptor.isRepeated()
              && FieldDescriptor.Type.MESSAGE.equals(type)
              && !pb.hasField(fieldDescriptor)) {
            continue;
          }
          int fieldIndex = fieldDescriptor.getIndex();
          FieldWriter fieldWriter = fieldWriters[fieldIndex];
          fieldWriter.writeField(pb.getField(fieldDescriptor));
        }
      }
    }

    private void setFastMessageWriter(FastMessageWriter fastMessageWriter) {
      this.fastMessageWriter = fastMessageWriter == null ? FastMessageWriter.NOOP : fastMessageWriter;
    }
  }

  class RepeatedFieldWriter extends FieldWriter {

    @Override
    final void writeRawValue(Object value) {
      writeBeforeElement();
      writeElement(value);
      writeAfterElement();
    }

    @Override
    final void writeField(Object value) {
      List<?> list = (List<?>) value;
      if (list.isEmpty()) {
        return;
      }

      writeBeforeAll();
      for (Object element : list) {
        writeRawValue(element);
      }
      writeAfterAll();
    }

    void writeBeforeAll() {}

    void writeAfterAll() {}

    void writeBeforeElement() {}

    void writeAfterElement() {}

    void writeElement(Object element) {}
  }

  class ArrayWriter extends RepeatedFieldWriter {
    final FieldWriter fieldWriter;
    final ReflectionUtil.ArrayReflectionType fieldReflectionType;

    ArrayWriter(FieldWriter fieldWriter, ReflectionUtil.ArrayReflectionType fieldReflectionType) {
      this.fieldWriter = fieldWriter;
      this.fieldReflectionType = fieldReflectionType;
    }

    @Override
    ReflectionUtil.ArrayReflectionType getFieldReflectionType() {
      return fieldReflectionType;
    }

    @Override
    final void writeBeforeAll() {
      recordConsumer.startField(fieldName, index);
      if (writeSpecsCompliant) {
        recordConsumer.startGroup();

        recordConsumer.startField("list", 0); // This is the wrapper group for the array field
      }
    }

    @Override
    final void writeAfterAll() {
      if (writeSpecsCompliant) {
        recordConsumer.endField("list", 0);

        recordConsumer.endGroup();
      }
      recordConsumer.endField(fieldName, index);
    }

    @Override
    final void writeBeforeElement() {
      if (writeSpecsCompliant) {
        recordConsumer.startGroup();
        recordConsumer.startField("element", 0); // This is the mandatory inner field
      }
    }

    @Override
    final void writeAfterElement() {
      if (writeSpecsCompliant) {
        recordConsumer.endField("element", 0);
        recordConsumer.endGroup();
      }
    }

    @Override
    final void writeElement(Object element) {
      fieldWriter.writeRawValue(element);
    }
  }

  /**
   * validates mapping between protobuffer fields and parquet fields.
   */
  private void validatedMapping(Descriptor descriptor, GroupType parquetSchema) {
    List<FieldDescriptor> allFields = descriptor.getFields();

    for (FieldDescriptor fieldDescriptor : allFields) {
      String fieldName = fieldDescriptor.getName();
      int fieldIndex = fieldDescriptor.getIndex();
      int parquetIndex = parquetSchema.getFieldIndex(fieldName);
      if (fieldIndex != parquetIndex) {
        String message = "FieldIndex mismatch name=" + fieldName + ": " + fieldIndex + " != " + parquetIndex;
        throw new IncompatibleSchemaModificationException(message);
      }
    }
  }

  class StringWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      Binary binaryString = Binary.fromString((String) value);
      recordConsumer.addBinary(binaryString);
    }

    @Override
    Class<String> getFieldReflectionType() {
      return String.class;
    }
  }

  interface JavaPrimitiveWriter {}

  class IntWriter extends FieldWriter implements JavaPrimitiveWriter {
    final void writeRawValuePrimitive(int value) {
      recordConsumer.addInteger(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((int) value);
    }

    @Override
    Class<Integer> getFieldReflectionType() {
      return int.class;
    }
  }

  class LongWriter extends FieldWriter implements JavaPrimitiveWriter {

    final void writeRawValuePrimitive(long value) {
      recordConsumer.addLong(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((long) value);
    }

    @Override
    Class<Long> getFieldReflectionType() {
      return long.class;
    }
  }

  class MapWriter extends RepeatedFieldWriter {

    private final FieldWriter keyWriter;
    private final FieldWriter valueWriter;
    private final ReflectionUtil.MapReflectionType fieldReflectionType;

    public MapWriter(
        FieldWriter keyWriter, FieldWriter valueWriter, ReflectionUtil.MapReflectionType fieldReflectionType) {
      super();
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
      this.fieldReflectionType = fieldReflectionType;
    }

    @Override
    ReflectionUtil.MapReflectionType getFieldReflectionType() {
      return fieldReflectionType;
    }

    @Override
    final void writeBeforeAll() {
      recordConsumer.startField(fieldName, index);
      if (writeSpecsCompliant) {
        recordConsumer.startGroup();

        recordConsumer.startField("key_value", 0); // This is the wrapper group for the map field
      }
    }

    @Override
    final void writeAfterAll() {
      if (writeSpecsCompliant) {
        recordConsumer.endField("key_value", 0);

        recordConsumer.endGroup();
      }
      recordConsumer.endField(fieldName, index);
    }

    @Override
    final void writeBeforeElement() {
      recordConsumer.startGroup();
    }

    @Override
    final void writeAfterElement() {
      recordConsumer.endGroup();
    }

    @Override
    final void writeElement(Object element) {
      Message msg = (Message) element;
      final Descriptor descriptorForType = msg.getDescriptorForType();
      final FieldDescriptor keyDesc = descriptorForType.findFieldByName("key");
      final FieldDescriptor valueDesc = descriptorForType.findFieldByName("value");

      keyWriter.writeField(msg.getField(keyDesc));
      valueWriter.writeField(msg.getField(valueDesc));
    }
  }

  class FloatWriter extends FieldWriter implements JavaPrimitiveWriter {

    final void writeRawValuePrimitive(float value) {
      recordConsumer.addFloat(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((float) value);
    }

    @Override
    Class<Float> getFieldReflectionType() {
      return float.class;
    }
  }

  class DoubleWriter extends FieldWriter implements JavaPrimitiveWriter {
    final void writeRawValuePrimitive(double value) {
      recordConsumer.addDouble(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((double) value);
    }

    @Override
    Class<Double> getFieldReflectionType() {
      return double.class;
    }
  }

  class EnumWriter extends FieldWriter {
    Map<String, Integer> enumNameNumberPairs;
    final Class<?> fieldReflectionType;

    public EnumWriter(Descriptors.EnumDescriptor enumType, Class<?> fieldReflectionType) {
      this.fieldReflectionType = fieldReflectionType;

      if (protoEnumBookKeeper.containsKey(enumType.getFullName())) {
        enumNameNumberPairs = protoEnumBookKeeper.get(enumType.getFullName());
      } else {
        enumNameNumberPairs = new HashMap<>();
        protoEnumBookKeeper.put(enumType.getFullName(), enumNameNumberPairs);
      }
    }

    @Override
    final void writeRawValue(Object value) {
      Descriptors.EnumValueDescriptor enumValueDesc = (Descriptors.EnumValueDescriptor) value;
      Binary binary = Binary.fromString(enumValueDesc.getName());
      recordConsumer.addBinary(binary);
      enumNameNumberPairs.putIfAbsent(enumValueDesc.getName(), enumValueDesc.getNumber());
    }

    @Override
    Class<?> getFieldReflectionType() {
      return fieldReflectionType;
    }
  }

  class BooleanWriter extends FieldWriter implements JavaPrimitiveWriter {

    final void writeRawValuePrimitive(boolean value) {
      recordConsumer.addBoolean(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((boolean) value);
    }

    @Override
    Class<Boolean> getFieldReflectionType() {
      return boolean.class;
    }
  }

  class BinaryWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      ByteString byteString = (ByteString) value;
      Binary binary = Binary.fromConstantByteArray(byteString.toByteArray());
      recordConsumer.addBinary(binary);
    }

    @Override
    Class<ByteString> getFieldReflectionType() {
      return ByteString.class;
    }
  }

  class BinaryMessageWriter extends FieldWriter {
    final Class<? extends Message> protoMessageClass;

    BinaryMessageWriter(Class<? extends Message> protoMessageClass) {
      this.protoMessageClass = protoMessageClass;
    }

    @Override
    Class<? extends Message> getFieldReflectionType() {
      return protoMessageClass;
    }

    @Override
    final void writeRawValue(Object value) {
      ByteString byteString =
          // TODO: figure out a way to use MessageOrBuilder
          value instanceof Message
              ? ((Message) value).toByteString()
              // Worst-case, just dump as plain java string.
              : ByteString.copyFromUtf8(value.toString());
      Binary binary = Binary.fromConstantByteArray(byteString.toByteArray());
      recordConsumer.addBinary(binary);
    }
  }

  interface WrappersProtoWriter {}

  class TimestampWriter extends FieldWriter implements WrappersProtoWriter {
    @Override
    void writeRawValue(Object value) {
      Timestamp timestamp = (Timestamp) value;
      recordConsumer.addLong(Timestamps.toNanos(timestamp));
    }

    @Override
    Class<Timestamp> getFieldReflectionType() {
      return Timestamp.class;
    }
  }

  class DateWriter extends FieldWriter implements WrappersProtoWriter {
    @Override
    void writeRawValue(Object value) {
      Date date = (Date) value;
      LocalDate localDate = LocalDate.of(date.getYear(), date.getMonth(), date.getDay());
      recordConsumer.addInteger((int) localDate.toEpochDay());
    }

    @Override
    Class<Date> getFieldReflectionType() {
      return Date.class;
    }
  }

  class TimeWriter extends FieldWriter implements WrappersProtoWriter {
    @Override
    void writeRawValue(Object value) {
      TimeOfDay timeOfDay = (TimeOfDay) value;
      LocalTime localTime = LocalTime.of(
          timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds(), timeOfDay.getNanos());
      recordConsumer.addLong(localTime.toNanoOfDay());
    }

    @Override
    Class<TimeOfDay> getFieldReflectionType() {
      return TimeOfDay.class;
    }
  }

  class DoubleValueWriter extends FieldWriter implements WrappersProtoWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addDouble(((DoubleValue) value).getValue());
    }

    @Override
    Class<DoubleValue> getFieldReflectionType() {
      return DoubleValue.class;
    }
  }

  class FloatValueWriter extends FieldWriter implements WrappersProtoWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addFloat(((FloatValue) value).getValue());
    }

    @Override
    Class<FloatValue> getFieldReflectionType() {
      return FloatValue.class;
    }
  }

  class Int64ValueWriter extends FieldWriter implements WrappersProtoWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addLong(((Int64Value) value).getValue());
    }

    @Override
    Class<Int64Value> getFieldReflectionType() {
      return Int64Value.class;
    }
  }

  class UInt64ValueWriter extends FieldWriter implements WrappersProtoWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addLong(((UInt64Value) value).getValue());
    }

    @Override
    Class<UInt64Value> getFieldReflectionType() {
      return UInt64Value.class;
    }
  }

  class Int32ValueWriter extends FieldWriter implements WrappersProtoWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addInteger(((Int32Value) value).getValue());
    }

    @Override
    Class<Int32Value> getFieldReflectionType() {
      return Int32Value.class;
    }
  }

  class UInt32ValueWriter extends FieldWriter implements WrappersProtoWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addLong(((UInt32Value) value).getValue());
    }

    @Override
    Class<UInt32Value> getFieldReflectionType() {
      return UInt32Value.class;
    }
  }

  class BoolValueWriter extends FieldWriter implements WrappersProtoWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addBoolean(((BoolValue) value).getValue());
    }

    @Override
    Class<BoolValue> getFieldReflectionType() {
      return BoolValue.class;
    }
  }

  class StringValueWriter extends FieldWriter implements WrappersProtoWriter {
    @Override
    void writeRawValue(Object value) {
      Binary binaryString = Binary.fromString(((StringValue) value).getValue());
      recordConsumer.addBinary(binaryString);
    }

    @Override
    Class<StringValue> getFieldReflectionType() {
      return StringValue.class;
    }
  }

  class BytesValueWriter extends FieldWriter implements WrappersProtoWriter {
    @Override
    void writeRawValue(Object value) {
      byte[] byteArray = ((BytesValue) value).getValue().toByteArray();
      Binary binary = Binary.fromConstantByteArray(byteArray);
      recordConsumer.addBinary(binary);
    }

    @Override
    Class<BytesValue> getFieldReflectionType() {
      return BytesValue.class;
    }
  }

  private FieldWriter unknownType(FieldDescriptor fieldDescriptor) {
    String exceptionMsg = "Unknown type with descriptor \"" + fieldDescriptor + "\" and type \""
        + fieldDescriptor.getJavaType() + "\".";
    throw new InvalidRecordException(exceptionMsg);
  }

  // resolve reflection methods early, so tests would fail fast should anything is changed in interfaces/classes
  static final ResolvedReflection Reflection = new ResolvedReflection();

  static class ResolvedReflection {

    final RecordConsumerMethods RecordConsumer = new RecordConsumerMethods();
    final ByteBuddyProto3FastMessageWriterMethods ByteBuddyProto3FastMessageWriter =
        new ByteBuddyProto3FastMessageWriterMethods();
    final FieldOfObjectWriterMethods FieldOfObjectWriter = new FieldOfObjectWriterMethods();
    final FastMessageWriterMethods FastMessageWriter = new FastMessageWriterMethods();
    final FieldWriterMethods FieldWriter = new FieldWriterMethods();

    static class RecordConsumerMethods {
      final Method startMessage = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "startMessage");
      final Method endMessage = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "endMessage");
      final Method startField =
          ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "startField", String.class, int.class);
      final Method endField =
          ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "endField", String.class, int.class);
      final Method startGroup = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "startGroup");
      final Method endGroup = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "endGroup");
      final Method addInteger = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "addInteger", int.class);
      final Method addLong = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "addLong", long.class);
      final Method addBoolean =
          ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "addBoolean", boolean.class);
      final Method addBinary = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "addBinary", Binary.class);
      final Method addFloat = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "addFloat", float.class);
      final Method addDouble = ReflectionUtil.getDeclaredMethod(RecordConsumer.class, "addDouble", double.class);

      final Map<Class<?>, Method> PRIMITIVES = initPrimitives();

      private Map<Class<?>, Method> initPrimitives() {
        Map<Class<?>, Method> m = new HashMap<>();
        m.put(int.class, addInteger);
        m.put(long.class, addLong);
        m.put(boolean.class, addBoolean);
        m.put(float.class, addFloat);
        m.put(double.class, addDouble);
        return Collections.unmodifiableMap(m);
      }

      private RecordConsumerMethods() {}
    }

    static class ByteBuddyProto3FastMessageWriterMethods {
      final Method getRecordConsumer = ReflectionUtil.getDeclaredMethod(
          ByteBuddyProto3FastMessageWriters.ByteBuddyProto3FastMessageWriter.class, "getRecordConsumer");

      private ByteBuddyProto3FastMessageWriterMethods() {}
    }

    static class FieldOfObjectWriterMethods {
      final Method writeFieldOfObject = ReflectionUtil.getDeclaredMethod(
          JavaReflectionProto3FastMessageWriters.FieldOfObjectWriter.class,
          "writeFieldOfObject",
          Object.class);

      private FieldOfObjectWriterMethods() {}
    }

    static class FastMessageWriterMethods {
      final Method writeAllFields =
          ReflectionUtil.getDeclaredMethod(FastMessageWriter.class, "writeAllFields", MessageOrBuilder.class);

      private FastMessageWriterMethods() {}
    }

    static class FieldWriterMethods {
      final Method writeField =
          ReflectionUtil.getDeclaredMethod(ProtoWriteSupport.FieldWriter.class, "writeField", Object.class);

      private FieldWriterMethods() {}
    }

    private ResolvedReflection() {}
  }

  static class ReflectionUtil {

    static Optional<? extends Class<? extends MessageOrBuilder>> getProto3MessageOrBuilderInterface(
        Class<?> messageClass) {
      return Stream.of(messageClass)
          .filter(Objects::nonNull)
          .filter(GeneratedMessageV3.class::isAssignableFrom)
          .filter(x -> Protobufs.getMessageDescriptor((Class<? extends Message>) x)
                  .getFile()
                  .getSyntax()
              == Descriptors.FileDescriptor.Syntax.PROTO3)
          .flatMap(x -> Arrays.stream(x.getInterfaces()))
          .filter(MessageOrBuilder.class::isAssignableFrom)
          .map(x -> (Class<? extends MessageOrBuilder>) x)
          .findFirst();
    }

    static Method getDeclaredMethod(
        Class<?> proto3MessageOrBuilderInterface,
        String name,
        FieldDescriptor fieldDescriptor,
        Class<?>... parameters) {
      return getDeclaredMethod(
          proto3MessageOrBuilderInterface,
          name.replace("{}", getFieldNameForMethod(fieldDescriptor)),
          parameters);
    }

    // almost the same as com.google.protobuf.Descriptors.FieldDescriptor#fieldNameToJsonName
    // but capitalizing the first letter after each last digit
    static String getFieldNameForMethod(FieldDescriptor fieldDescriptor) {
      String name = fieldDescriptor.getType() == FieldDescriptor.Type.GROUP
          ? fieldDescriptor.getMessageType().getName()
          : fieldDescriptor.getName();
      final int length = name.length();
      StringBuilder result = new StringBuilder(length);
      boolean isNextUpperCase = false;
      for (int i = 0; i < length; i++) {
        char ch = name.charAt(i);
        if (ch == '_') {
          isNextUpperCase = true;
        } else if ('0' <= ch && ch <= '9') {
          isNextUpperCase = true;
          result.append(ch);
        } else if (isNextUpperCase || i == 0) {
          // This closely matches the logic for ASCII characters in:
          // http://google3/google/protobuf/descriptor.cc?l=249-251&rcl=228891689
          if ('a' <= ch && ch <= 'z') {
            ch = (char) (ch - 'a' + 'A');
          }
          result.append(ch);
          isNextUpperCase = false;
        } else {
          result.append(ch);
        }
      }
      return result.toString();
    }

    static class ArrayReflectionType implements java.lang.reflect.Type {
      final Class<?> element;

      ArrayReflectionType(Class<?> element) {
        this.element = element;
      }

      Class<?> getElement() {
        return element;
      }
    }

    static class MapReflectionType implements java.lang.reflect.Type {
      final Class<?> key;
      final Class<?> value;

      MapReflectionType(Class<?> key, Class<?> value) {
        this.key = key;
        this.value = value;
      }

      Class<?> getKey() {
        return key;
      }

      Class<?> getValue() {
        return value;
      }
    }

    static java.lang.reflect.Type getFieldMessageType(
        FieldDescriptor fieldDescriptor, Class<? extends Message> protoMessageClass) {
      if (protoMessageClass == null) {
        return null;
      }
      if (fieldDescriptor.isMapField()) {
        try {
          Method getter = protoMessageClass.getMethod("get" + getFieldNameForMethod(fieldDescriptor) + "Map");
          ParameterizedType mapKV = (ParameterizedType) getter.getGenericReturnType();
          return new MapReflectionType(
              (Class<?>) mapKV.getActualTypeArguments()[0], (Class<?>) mapKV.getActualTypeArguments()[1]);
        } catch (Exception e) {
          throw new UnsupportedOperationException(
              "could not find a getter method for a map field: " + fieldDescriptor, e);
        }
      } else if (fieldDescriptor.isRepeated()) {
        try {
          Method getter =
              protoMessageClass.getMethod("get" + getFieldNameForMethod(fieldDescriptor), int.class);
          return new ArrayReflectionType(getter.getReturnType());
        } catch (Exception e) {
          throw new UnsupportedOperationException(
              "could not find a getter method for a repeated field: " + fieldDescriptor, e);
        }
      } else {
        try {
          Method getter = protoMessageClass.getMethod("get" + getFieldNameForMethod(fieldDescriptor));
          return getter.getReturnType();
        } catch (Exception e) {
          throw new UnsupportedOperationException(
              "could not find a getter method for a non-repeated field: " + fieldDescriptor, e);
        }
      }
    }

    static Method getDeclaredMethod(Class<?> clazz, String name, Class<?>... parameterTypes) {
      try {
        return clazz.getDeclaredMethod(name, parameterTypes);
      } catch (NoSuchMethodException e) {
        throw new FastMessageWriterCreationException(e);
      }
    }

    static <T> Constructor<T> getConstructor(Class<T> clazz, Class<?>... parameterTypes) {
      try {
        return clazz.getConstructor(parameterTypes);
      } catch (NoSuchMethodException e) {
        throw new FastMessageWriterCreationException(e);
      }
    }

    static <T> T newInstance(Constructor<T> constructor, Object... initParams) {
      try {
        return constructor.newInstance(initParams);
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new FastMessageWriterCreationException(e);
      }
    }
  }

  private void createFastMessageWriters(MessageWriter messageWriter) {
    switch (fastMessageWriterMode) {
      case OFF:
        break;
      case REFLECTION:
        new JavaReflectionProto3FastMessageWriters().createFastMessageWriters(messageWriter);
        break;
      case CODEGEN:
        new ByteBuddyProto3FastMessageWriters(this).createFastMessageWriters(messageWriter);
        break;
      case BEST:
        if (ByteBuddyProto3FastMessageWriters.isByteBuddyAvailable()) {
          new ByteBuddyProto3FastMessageWriters(this).createFastMessageWriters(messageWriter);
        } else {
          new JavaReflectionProto3FastMessageWriters().createFastMessageWriters(messageWriter);
        }
        break;
    }
  }

  /**
   * A plugin for {@link MessageWriter#writeAllFields(MessageOrBuilder)} that is potentially
   * capable to write MessageOrBuilder fields faster.
   */
  interface FastMessageWriter {
    FastMessageWriter NOOP = messageOrBuilder -> false;

    /**
     * Performs all the steps that {@link MessageWriter#writeAllFields(MessageOrBuilder)}
     * would normally do, but faster.
     * @param messageOrBuilder
     * @return true if this writer has written fields of the passed messageOrBuilder
     *         false otherwise
     */
    boolean writeAllFields(MessageOrBuilder messageOrBuilder);
  }

  static class ByteBuddyProto3FastMessageWriters extends Proto3FastMessageWriters {
    static final AtomicLong BYTE_BUDDY_CLASS_SEQUENCE = new AtomicLong();

    final ProtoWriteSupport<?> protoWriteSupport;

    abstract static class ByteBuddyProto3FastMessageWriter implements FastMessageWriter {
      final ProtoWriteSupport<?> protoWriteSupport;
      final ProtoWriteSupport<?>.MessageWriter[] messageWriters;

      ByteBuddyProto3FastMessageWriter(ProtoWriteSupport<?> protoWriteSupport, ProtoWriteSupport<?>.MessageWriter rootMessageWriter) {
        this.protoWriteSupport = protoWriteSupport;
        this.messageWriters = createMessageWriterArray(rootMessageWriter);
      }

      private static ProtoWriteSupport<?>.MessageWriter[] createMessageWriterArray(ProtoWriteSupport<?>.MessageWriter rootMessageWriter) {
        final List<ProtoWriteSupport<?>.MessageWriter> writers = new ArrayList<>();
        MessageWriterVisitor.traverse(new MessageWriterVisitor.RegularField<>(new FieldPath(), null, rootMessageWriter), new MessageWriterVisitor() {
          @Override
          public boolean visitMessageWriter(RegularField<?> field) {
            if (field.getFieldReflectionTypeProto3MessageOrBuilderInterface().isPresent()) {
              writers.add((ProtoWriteSupport<?>.MessageWriter) field.rawValueWriter());
              return true;
            }
            return false;
          }

          @Override
          public boolean visitMapMessageWriter(MapField field) {
            return false;
          }
        });
        return writers.toArray(new ProtoWriteSupport<?>.MessageWriter[0]);
      }

      /**
       * A helper used by ByteBuddy generated bytecode to get access to {@link #recordConsumer}
       * @return recordConsumer
       */
      RecordConsumer getRecordConsumer() {
        return protoWriteSupport.recordConsumer;
      }

      Map<String, Integer> enumNameNumberPairs(String enumTypeFullName) {
        return protoWriteSupport.protoEnumBookKeeper.get(enumTypeFullName);
      }
    }

    ByteBuddyProto3FastMessageWriters(ProtoWriteSupport<?> protoWriteSupport) {
      this.protoWriteSupport = protoWriteSupport;
    }

    static boolean isByteBuddyAvailable() {
      try {
        Class.forName("net.bytebuddy.ByteBuddy", false, ProtoWriteSupport.class.getClassLoader());
        return true;
      } catch (ClassNotFoundException e) {
        return false;
      }
    }

    @Override
    FastMessageWriter generateFastMessageWriter(
        ProtoWriteSupport<?>.MessageWriter messageWriter,
        Queue<ProtoWriteSupport<?>.FieldWriter> furtherFieldWriters) {

      FastMessageWriterImplementation impl = new FastMessageWriterImplementation(
          protoWriteSupport,
          messageWriter);

      DynamicType.Unloaded<ByteBuddyProto3FastMessageWriter> unloaded = impl.make();

      try {
        unloaded.saveIn(new java.io.File("generated_debug"));
      } catch (Exception e) {
      }

      furtherFieldWriters.addAll(impl.notOptimizedMessageWriters);

      Class<? extends ByteBuddyProto3FastMessageWriter> fastMessageWriterClass = unloaded.load(
              null, ClassLoadingStrategy.UsingLookup.of(MethodHandles.lookup()))
          .getLoaded();
      return ReflectionUtil.newInstance(
          ReflectionUtil.getConstructor(fastMessageWriterClass, ProtoWriteSupport.class, ProtoWriteSupport.MessageWriter.class), protoWriteSupport, messageWriter);
    }

    @Override
    void assertOptimizationAvailable() {
      if (!isByteBuddyAvailable()) {
        throw new FastMessageWriterCreationException("ByteBuddy optimization for proto3 is not available");
      }
    }

    static final Implementation NOOP = new Implementation.Simple(
        (methodVisitor, implementationContext, instrumentedMethod) -> new ByteCodeAppender.Size(0, 0));

    interface MessageWriterVisitor {
      final class FieldPath {
        final List<String> path = new ArrayList<>();

        String getField() {
          return isRoot() ? "" : path.get(path.size() - 1);
        }

        boolean isRoot() {
          return path.isEmpty();
        }

        FieldPath push(String segment) {
          FieldPath r = new FieldPath();
          r.path.addAll(this.path);
          r.path.add(segment);
          return r;
        }

        @Override
        public boolean equals(Object o) {
          if (o == null || getClass() != o.getClass()) return false;
          FieldPath fieldPath = (FieldPath) o;
          return Objects.equals(path, fieldPath.path);
        }

        @Override
        public int hashCode() {
          return Objects.hashCode(path);
        }

        @Override
        public String toString() {
          return "FieldPath{" + "path=" + path + '}';
        }
      }

      abstract class Field<T extends ProtoWriteSupport<?>.FieldWriter> {
        final FieldPath fieldPath;
        final Class<? extends Message> owningType;
        final T fieldWriter;

        Field(FieldPath fieldPath, Class<? extends Message> owningType, T fieldWriter) {
          this.fieldPath = fieldPath;
          this.owningType = owningType;
          this.fieldWriter = fieldWriter;
        }

        Optional<? extends Class<? extends MessageOrBuilder>> getOwningTypeProto3MessageOrBuilderInterface() {
          return ReflectionUtil.getProto3MessageOrBuilderInterface(owningType);
        }

        FieldDescriptor protoDescriptor() {
          return fieldWriter.fieldDescriptor;
        }

        String parquetFieldName() {
          return fieldWriter.fieldName;
        }

        int parquetFieldIndex() {
          return fieldWriter.index;
        }
      }

      class RegularField<T extends ProtoWriteSupport<?>.FieldWriter> extends Field<T> {
        RegularField(FieldPath fieldPath, Class<? extends Message> owningType, T fieldWriter) {
          super(fieldPath, owningType, fieldWriter);
        }

        @Override
        public String toString() {
          return "RegularField{" +
              "fieldPath=" + fieldPath +
              ", owningType=" + owningType +
              ", fieldWriter=" + fieldWriter +
              '}';
        }

        ProtoWriteSupport<?>.FieldWriter rawValueWriter() {
          if (fieldWriter instanceof ProtoWriteSupport<?>.ArrayWriter) {
            return ((ProtoWriteSupport<?>.ArrayWriter) fieldWriter).fieldWriter;
          }
          return fieldWriter;
        }

        RegularField<? extends ProtoWriteSupport<?>.MessageWriter> asRawValueMessageWriter() {
          if (!isMessage()) {
            throw new IllegalStateException();
          }
          return new RegularField<>(
              fieldPath, owningType, (ProtoWriteSupport<?>.MessageWriter) rawValueWriter());
        }

        boolean isRoot() {
          return fieldPath.isRoot();
        }

        Class<?> getFieldReflectionType() {
          return (Class<?>) rawValueWriter().getFieldReflectionType();
        }

        Optional<? extends Class<? extends MessageOrBuilder>>
            getFieldReflectionTypeProto3MessageOrBuilderInterface() {
          if (!isMessage()) {
            throw new IllegalStateException("illegal to call on non-message field");
          }
          return ReflectionUtil.getProto3MessageOrBuilderInterface(
              (Class<?>) rawValueWriter().getFieldReflectionType());
        }

        boolean isRepeated() {
          return fieldWriter instanceof ProtoWriteSupport<?>.ArrayWriter;
        }

        boolean isOptional() {
          if (isRepeated()) {
            return false;
          }
          return protoDescriptor().hasPresence();
        }

        boolean isPrimitive() {
          return rawValueWriter() instanceof JavaPrimitiveWriter;
        }

        boolean isProtoWrapper() {
          return rawValueWriter() instanceof WrappersProtoWriter;
        }

        boolean isMessage() {
          return rawValueWriter() instanceof ProtoWriteSupport<?>.MessageWriter;
        }

        boolean isString() {
          return rawValueWriter() instanceof ProtoWriteSupport<?>.StringWriter;
        }

        boolean isBinary() {
          return rawValueWriter() instanceof ProtoWriteSupport<?>.BinaryWriter;
        }

        boolean isBinaryMessage() {
          return rawValueWriter() instanceof ProtoWriteSupport<?>.BinaryMessageWriter;
        }

        boolean isEnum() {
          return rawValueWriter() instanceof ProtoWriteSupport<?>.EnumWriter;
        }
      }

      class MapField extends Field<ProtoWriteSupport<?>.MapWriter> {
        MapField(
            FieldPath fieldPath,
            Class<? extends Message> owningType,
            ProtoWriteSupport<?>.MapWriter mapWriter) {
          super(fieldPath, owningType, mapWriter);
        }

        RegularField<ProtoWriteSupport<?>.FieldWriter> key() {
          return new RegularField<>(fieldPath.push("key"), owningType, fieldWriter.keyWriter);
        }

        RegularField<ProtoWriteSupport<?>.FieldWriter> value() {
          return new RegularField<>(fieldPath.push("value"), owningType, fieldWriter.valueWriter);
        }

        @Override
        public String toString() {
          return "MapField{" +
              "fieldPath=" + fieldPath +
              '}';
        }
      }

      default boolean visitMessageWriter(RegularField<?> field) {
        return ReflectionUtil.getProto3MessageOrBuilderInterface(field.getFieldReflectionType())
            .isPresent();
      }

      default void visitNonMessageWriter(RegularField<?> field) {}

      default boolean visitMapMessageWriter(MapField field) {
        return field.value()
            .getFieldReflectionTypeProto3MessageOrBuilderInterface()
            .isPresent();
      }

      default void visitMapNonMessageWriter(MapField field) {}

      static void traverse(
          RegularField<? extends ProtoWriteSupport<?>.MessageWriter> start, MessageWriterVisitor visitor) {
        if (!start.isMessage()) {
          throw new IllegalStateException("start must be message");
        }

        class FieldPathAndMessageWriter {
          final FieldPath fieldPath;
          final ProtoWriteSupport<?>.MessageWriter messageWriter;

          FieldPathAndMessageWriter(FieldPath fieldPath, ProtoWriteSupport<?>.MessageWriter messageWriter) {
            this.fieldPath = fieldPath;
            this.messageWriter = messageWriter;
          }
        }

        class Traverse {
          void traverse() {
            Queue<FieldPathAndMessageWriter> queue = new LinkedList<>();
            visitField(start, queue);
            while (!queue.isEmpty()) {
              FieldPathAndMessageWriter next = queue.poll();
              Class<? extends Message> owningType = next.messageWriter.getFieldReflectionType();

              for (ProtoWriteSupport<?>.FieldWriter fieldWriter : next.messageWriter.fieldWriters) {
                FieldPath field = next.fieldPath.push(fieldWriter.fieldName);
                if (fieldWriter instanceof ProtoWriteSupport<?>.MapWriter) {
                  MapField mapField = new MapField(
                      field, owningType, (ProtoWriteSupport<?>.MapWriter) fieldWriter);
                  visitMap(mapField, queue);
                } else {
                  RegularField<?> regularField = new RegularField<>(field, owningType, fieldWriter);
                  visitField(regularField, queue);
                }
              }
            }
          }

          void visitField(RegularField<?> regularField, Queue<FieldPathAndMessageWriter> queue) {
            if (regularField.isMessage()) {
              if (visitor.visitMessageWriter(regularField)) {
                queue.add(new FieldPathAndMessageWriter(
                    regularField.fieldPath,
                    (ProtoWriteSupport<?>.MessageWriter) regularField.rawValueWriter()));
              }
            } else {
              visitor.visitNonMessageWriter(regularField);
            }
          }

          void visitMap(MapField mapField, Queue<FieldPathAndMessageWriter> queue) {
            RegularField<?> valueField = mapField.value();
            if (valueField.isMessage()) {
              if (visitor.visitMapMessageWriter(mapField)) {
                queue.add(new FieldPathAndMessageWriter(
                    mapField.fieldPath.push("value"),
                    (ProtoWriteSupport<?>.MessageWriter) mapField.fieldWriter.valueWriter));
              }
            } else {
              visitor.visitMapNonMessageWriter(mapField);
            }
          }
        }

        new Traverse().traverse();
      }
    }

    /*
    MessageWriter () [proto3]                            X.writeAllFields(m) -> v = m.getField1(i)
    ArrayField (1) -> MessageWriter () [proto3]        X.writeAllFields1(v)
    ArrayField (1.1) -> MessageWriter () [proto3]  X.writeAllFields2
    MessageWriter (1.1.1) [proto3]             X.writeAllFields3

    MessageWriter () [proto3]                            X.writeAllFields(m) -> v = m.getField1(i)
    ArrayField (1) -> MessageWriter () [proto2]        mw.writeRawValue(v)
    ArrayField (1.1) -> MessageWriter () [proto3]  Y.writeAllFields(z)
    MessageWriter (1.1.1) [proto3]
    */

    static class FastMessageWriterImplementation {
      final ProtoWriteSupport<?> protoWriteSupport;
      final ProtoWriteSupport<?>.MessageWriter rootMessageWriter;

      final List<ProtoWriteSupport<?>.MessageWriter> notOptimizedMessageWriters = new ArrayList<>();

      final Map<FieldPath, String> fieldPathToMethodNameMap = new HashMap<>();
      final Map<FieldPath, MethodDescription> fieldPathToMethodDescriptionMap = new HashMap<>();

      final Map<FieldPath, String> mapFieldPathToMethodNameMap = new HashMap<>();
      final Map<FieldPath, MethodDescription> mapFieldPathToMethodDescriptionMap = new HashMap<>();

      final Map<String, Integer> enumTypeFullNameToFieldIdx = new HashMap<>();
      final Map<String, Class<?>> enumTypeFullNameToClassMap = new HashMap<>();

      final DynamicType.Builder<ByteBuddyProto3FastMessageWriter>[] classBuilder;

      FastMessageWriterImplementation(
          ProtoWriteSupport<?> protoWriteSupport,
          ProtoWriteSupport<?>.MessageWriter rootMessageWriter) {
        this.protoWriteSupport = protoWriteSupport;
        this.rootMessageWriter = rootMessageWriter;

        classBuilder = new DynamicType.Builder[]{
            new ByteBuddy()
                .subclass(ByteBuddyProto3FastMessageWriter.class)
                .name(ByteBuddyProto3FastMessageWriter.class.getName() + "$Generated$"
                + BYTE_BUDDY_CLASS_SEQUENCE.incrementAndGet())
        };

        final MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.MessageWriter> start =
            new MessageWriterVisitor.RegularField<>(new FieldPath(), null, rootMessageWriter);

        // first scan - collect methods and fields for enums
        MessageWriterVisitor.traverse(start, new MessageWriterVisitor() {
          @Override
          public boolean visitMessageWriter(RegularField<?> field) {
            if (field.getFieldReflectionTypeProto3MessageOrBuilderInterface()
                .isPresent()) {
              registerWriteAllFieldsForMessageMethod(field);
              return true;
            } else {
              notOptimizedMessageWriters.add((ProtoWriteSupport<?>.MessageWriter) field.rawValueWriter());
              return false;
            }
          }

          @Override
          public void visitNonMessageWriter(RegularField<?> field) {
            maybeRegisterEnum(field);
          }

          private void maybeRegisterEnum(RegularField<?> field) {
            if (field.isEnum()) {
              ProtoWriteSupport<?>.EnumWriter enumWriter = (ProtoWriteSupport<?>.EnumWriter) field.rawValueWriter();
              String enumTypeFullName = enumWriter.fieldDescriptor.getEnumType().getFullName();
              if (enumTypeFullNameToFieldIdx.putIfAbsent(enumTypeFullName, enumTypeFullNameToFieldIdx.size()) == null) {
                enumTypeFullNameToClassMap.put(enumTypeFullName, enumWriter.getFieldReflectionType());
              }
            }
          }

          @Override
          public boolean visitMapMessageWriter(MapField field) {
            if ((field.value().isMessage() && field.value().getFieldReflectionTypeProto3MessageOrBuilderInterface().isPresent())) {
              registerWriteMapEntry(field);
            }
            return field.value().isMessage() && field.value().getFieldReflectionTypeProto3MessageOrBuilderInterface().isPresent();
          }

          @Override
          public void visitMapNonMessageWriter(MapField field) {
            if (!field.value().isBinaryMessage()) {
              registerWriteMapEntry(field);
              maybeRegisterEnum(field.value());
            }
          }
        });

        // register enum fields
        // final Map<String, Integer> enumNameNumberPairs<idx>;
        // final Descriptors.EnumDescriptor enumDescriptor<idx>;
        // final List<Descriptors.EnumValueDescriptor> enumValues<idx>;

        for (int enumTypeFieldIdx : enumTypeFullNameToFieldIdx.values()) {
          classBuilder[0] = classBuilder[0].define(new FieldDescription.Latent(classBuilder[0].toTypeDescription(), new FieldDescription.Token(
              "enumNameNumberPairs" + enumTypeFieldIdx,
              Modifier.PRIVATE | Modifier.FINAL,
              TypeDescription.Generic.Builder.parameterizedType(Map.class, String.class, Integer.class).build()))
          );
          classBuilder[0] = classBuilder[0].define(new FieldDescription.Latent(classBuilder[0].toTypeDescription(), new FieldDescription.Token(
              "enumDescriptor" + enumTypeFieldIdx,
              Modifier.PRIVATE | Modifier.FINAL,
              new TypeDescription.Generic.OfNonGenericType.ForLoadedType(Descriptors.EnumDescriptor.class)))
          );
          classBuilder[0] = classBuilder[0].define(new FieldDescription.Latent(classBuilder[0].toTypeDescription(), new FieldDescription.Token(
              "enumValues" + enumTypeFieldIdx,
              Modifier.PRIVATE | Modifier.FINAL,
              TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(Descriptors.EnumValueDescriptor[].class)))
          );
        }

        classBuilder[0] = classBuilder[0]
            .constructor(ElementMatchers.any())
            .intercept(SuperMethodCall.INSTANCE.andThen(new FastMessageWriterConstructor()));

        // second scan - register methods
        MessageWriterVisitor.traverse(start, new MessageWriterVisitor() {
          @Override
          public boolean visitMessageWriter(RegularField<?> field) {
            if (field.getFieldReflectionTypeProto3MessageOrBuilderInterface()
                .isPresent()) {

              classBuilder[0] = classBuilder[0].define(fieldPathToMethodDescriptionMap.get(field.fieldPath))
                  .intercept(new WriteAllFieldsForMessageImplementation(field.asRawValueMessageWriter()));

              return true;
            } else {
              return false;
            }
          }

          @Override
          public boolean visitMapMessageWriter(MapField field) {
            if ((field.value().isMessage() && field.value().getFieldReflectionTypeProto3MessageOrBuilderInterface().isPresent())) {
              classBuilder[0] = classBuilder[0].define(mapFieldPathToMethodDescriptionMap.get(field.fieldPath)).intercept(new WriteAllFieldsForMapEntryImplementation(field));
              return field.value().isMessage() && field.value().getFieldReflectionTypeProto3MessageOrBuilderInterface().isPresent();
            }
            return false;
          }

          @Override
          public void visitMapNonMessageWriter(MapField field) {
             if (!field.value().isBinaryMessage()) {
               classBuilder[0] = classBuilder[0].define(mapFieldPathToMethodDescriptionMap.get(field.fieldPath)).intercept(new WriteAllFieldsForMapEntryImplementation(field));
             }
          }
        });
      }

      DynamicType.Unloaded<ByteBuddyProto3FastMessageWriter> make() {
        return classBuilder[0].make();
      }

      boolean writeSpecsCompliant() {
        return protoWriteSupport.writeSpecsCompliant;
      }

      class FastMessageWriterConstructor extends Implementations {

        FastMessageWriterConstructor() {
          // final Map<String, Integer> enumNameNumberPairs<idx>;
          // final Descriptors.EnumDescriptor enumDescriptor<idx>;
          // final List<Descriptors.EnumValueDescriptor> enumValues<idx>;

          for (Map.Entry<String, Integer> enumTypeFullNameToFieldIdx : enumTypeFullNameToFieldIdx.entrySet()) {
            String enumTypeFullName = enumTypeFullNameToFieldIdx.getKey();
            int idx = enumTypeFullNameToFieldIdx.getValue();

            add(MethodVariableAccess.loadThis(),
                MethodVariableAccess.loadThis(),
                new TextConstant(enumTypeFullName),
                Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(ByteBuddyProto3FastMessageWriter.class, "enumNameNumberPairs", String.class)),
                FieldAccess.forField(classBuilder[0].toTypeDescription().getDeclaredFields()
                    .filter(ElementMatchers.named("enumNameNumberPairs" + idx))
                    .getOnly()).write()
            );

            add(MethodVariableAccess.loadThis(),
                Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(enumTypeFullNameToClassMap.get(enumTypeFullName), "getDescriptor")),
                FieldAccess.forField(classBuilder[0].toTypeDescription().getDeclaredFields()
                    .filter(ElementMatchers.named("enumDescriptor" + idx))
                    .getOnly()).write()
            );

            add(MethodVariableAccess.loadThis(),
                MethodVariableAccess.loadThis(),
                FieldAccess.forField(classBuilder[0].toTypeDescription().getDeclaredFields()
                    .filter(ElementMatchers.named("enumDescriptor" + idx))
                    .getOnly()).read(),
                Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Descriptors.EnumDescriptor.class, "getValues")),
                ArrayFactory.forType(TypeDescription.Generic.OfNonGenericType.ForLoadedType.of(Descriptors.EnumValueDescriptor.class)).withValues(Collections.emptyList()),
                Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(List.class, "toArray", Object[].class)),
                TypeCasting.to(TypeDescription.ForLoadedType.of(Descriptors.EnumValueDescriptor[].class)),
                FieldAccess.forField(classBuilder[0].toTypeDescription().getDeclaredFields()
                    .filter(ElementMatchers.named("enumValues" + idx))
                    .getOnly()).write()
            );
          }
          add(Codegen.returnVoid());
        }
      }

      class WriteAllFieldsForMessageImplementation extends FastMessageWriterMethodBase {
        final MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.MessageWriter> field;

        WriteAllFieldsForMessageImplementation(
            MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.MessageWriter> regularField) {
          this.field = regularField;

          try (LocalVar thisLocalVar = localVars.register(classBuilder[0].toTypeDescription())) {
            writeMessageFields(regularField);
          }
        }
      }

      class WriteAllFieldsForMapEntryImplementation extends FastMessageWriterMethodBase {
        final MessageWriterVisitor.MapField field;

        WriteAllFieldsForMapEntryImplementation(MessageWriterVisitor.MapField field) {
          this.field = field;

          try (LocalVar thisLocalVar = localVars.register(classBuilder[0].toTypeDescription())) {
            try (LocalVar key = localVars.register(field.key().getFieldReflectionType())) {
              try (LocalVar value = localVars.register(field.value().isEnum() ? int.class : field.value().getFieldReflectionType())) {
                try (LocalVar recordConsumerVar = localVars.register(RecordConsumer.class)) {
                  steps.add(Codegen.storeRecordConsumer(recordConsumerVar));
                  steps.add(
                      recordConsumerVar.load(),
                      Codegen.invokeMethod(Reflection.RecordConsumer.startGroup)
                  );
                  steps.add(writeFromVar(field.key(), key, recordConsumerVar));
//                   if (field.value().isMessage()) {
//                     writeMessageFieldsInternal(value, recordConsumerVar, (MessageWriterVisitor.RegularField) field.value());
//                   } else {
                  steps.add(writeFromVar(field.value(), value, recordConsumerVar));
//                   }
                  steps.add(
                      recordConsumerVar.load(),
                      Codegen.invokeMethod(Reflection.RecordConsumer.endGroup)
                  );
                  steps.add(Codegen.returnVoid());
                }
              }
            }
          }
        }

        Implementation writeFromVar(MessageWriterVisitor.RegularField<?> field, LocalVar val, LocalVar recordConsumer) {
          if (field.isEnum()) {
            return new EnumFieldWriter((MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.EnumWriter>) field, recordConsumer).writeFromVar(val);
          } else if (field.isMessage()) {
            return new MessageFieldWriter(field, recordConsumer).writeFromVar(val);
          } else if (field.isString()) {
            return new StringFieldWriter(field, recordConsumer).writeFromVar(val);
          } else if (field.isBinary()) {
            return new BinaryFieldWriter(field, recordConsumer).writeFromVar(val);
          } else if (field.isProtoWrapper()) {
            return new ProtoWrapperFieldWriter(field, recordConsumer).writeFromVar(val);
          } else if (field.isPrimitive()) {
            return new PrimitiveFieldWriter(field, recordConsumer).writeFromVar(val);
          }
          throw new IllegalStateException("field: " + field);
        }
      }

      abstract class FastMessageWriterMethodBase implements Implementation {
        final Implementations steps = new Implementations();
        final LocalVars localVars = new LocalVars();

        Implementation compound;

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
          return compound.appender(implementationTarget);
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
          if (compound != null) {
            throw new IllegalStateException();
          }
          steps.add(localVars.asImplementation());
          compound = new Implementation.Compound(steps);
          return compound.prepare(instrumentedType);
        }

        protected void writeMessageFields(
            MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.MessageWriter> regularField) {
          boolean rootMessage = regularField.isRoot();
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface = regularField
              .getFieldReflectionTypeProto3MessageOrBuilderInterface()
              .get();

          try (LocalVar messageOrBuilderArg = localVars.register(
              rootMessage ? MessageOrBuilder.class : proto3MessageOrBuilderInterface)) {
            localVars.frameEmptyStack();

            if (rootMessage) {
              steps.add(Codegen.returnFalseIfNotInstanceOf(
                  messageOrBuilderArg, proto3MessageOrBuilderInterface));
            }

            try (LocalVar proto3MessageOrBuilder = rootMessage
                ? localVars.register(proto3MessageOrBuilderInterface)
                : messageOrBuilderArg.alias()) {
              if (rootMessage) {
                steps.add(Codegen.castToProto3MessageOrBuilderInterface(
                    proto3MessageOrBuilder, messageOrBuilderArg, proto3MessageOrBuilderInterface));
              }
              try (LocalVar recordConsumerVar = localVars.register(RecordConsumer.class)) {
                steps.add(Codegen.storeRecordConsumer(recordConsumerVar));
                steps.add(new MessageFieldWriter(regularField, recordConsumerVar).writeMessageFieldsInternal(proto3MessageOrBuilder));
              }
            }

            if (rootMessage) {
              steps.add(Codegen.returnTrue());
            } else {
              steps.add(Codegen.returnVoid());
            }
          }
        }

        abstract class RegularFieldWriterTemplate extends Implementations {
          final MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field;
          final LocalVar recordConsumerVar;

          RegularFieldWriterTemplate(
              MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field,
              LocalVar recordConsumerVar) {
            this.field = field;
            this.recordConsumerVar = recordConsumerVar;
          }

          String getterMethodTemplate() {
            return "get{}";
          }

          Implementation fieldGetConvertWrite(LocalVar proto3MessageOrBuilder) {
            if (field.isRepeated()) {
              Label afterIfCountGreaterThanZero = new Label();
              try (LocalVar countVar = localVars.register(int.class)) {
                add(
                    proto3MessageOrBuilder.load(),
                    Codegen.invokeProtoMethod(
                        proto3MessageOrBuilder.clazz(),
                        "get{}Count",
                        field.protoDescriptor()),
                    countVar.store(),
                    countVar.load(),
                    Codegen.jumpTo(Opcodes.IFLE, afterIfCountGreaterThanZero),
                    recordConsumerVar.load(),
                    new TextConstant(field.parquetFieldName()),
                    IntegerConstant.forValue(field.parquetFieldIndex()),
                    Codegen.invokeMethod(Reflection.RecordConsumer.startField));

                if (writeSpecsCompliant()) {
                  add(
                      recordConsumerVar.load(),
                      Codegen.invokeMethod(Reflection.RecordConsumer.startGroup),
                      recordConsumerVar.load(),
                      new TextConstant("list"),
                      IntegerConstant.forValue(0),
                      Codegen.invokeMethod(Reflection.RecordConsumer.startField));
                }

                Label nextIteration = new Label();
                Label afterForLoop = new Label();
                try (LocalVar iVar = localVars.register(int.class)) {
                  add(
                      IntegerConstant.forValue(0),
                      iVar.store(),
                      Codegen.visitLabel(nextIteration),
                      localVars.frameEmptyStack(),
                      iVar.load(),
                      countVar.load(),
                      Codegen.jumpTo(Opcodes.IF_ICMPGE, afterForLoop));

                  if (writeSpecsCompliant()) {
                    add(
                        recordConsumerVar.load(),
                        Codegen.invokeMethod(Reflection.RecordConsumer.startGroup),
                        recordConsumerVar.load(),
                        new TextConstant("element"),
                        IntegerConstant.forValue(0),
                        Codegen.invokeMethod(Reflection.RecordConsumer.startField));
                  }

                  writeRepeatedRawValue(proto3MessageOrBuilder, iVar);

                  if (writeSpecsCompliant()) {
                    add(
                        recordConsumerVar.load(),
                        new TextConstant("element"),
                        IntegerConstant.forValue(0),
                        Codegen.invokeMethod(Reflection.RecordConsumer.endField),
                        recordConsumerVar.load(),
                        Codegen.invokeMethod(Reflection.RecordConsumer.endGroup));
                  }

                  add(Codegen.incIntVar(iVar, 1), Codegen.jumpTo(Opcodes.GOTO, nextIteration));
                }

                add(Codegen.visitLabel(afterForLoop), localVars.frameEmptyStack());

                if (writeSpecsCompliant()) {
                  add(
                      recordConsumerVar.load(),
                      new TextConstant("list"),
                      IntegerConstant.forValue(0),
                      Codegen.invokeMethod(Reflection.RecordConsumer.endField),
                      recordConsumerVar.load(),
                      Codegen.invokeMethod(Reflection.RecordConsumer.endGroup));
                }

                add(
                    recordConsumerVar.load(),
                    new TextConstant(field.parquetFieldName()),
                    IntegerConstant.forValue(field.parquetFieldIndex()),
                    Codegen.invokeMethod(Reflection.RecordConsumer.endField));
              }
              add(Codegen.visitLabel(afterIfCountGreaterThanZero), localVars.frameEmptyStack());
            } else {
              Label afterEndField = new Label();
              if (field.isOptional()) {
                add(
                    proto3MessageOrBuilder.load(),
                    Codegen.invokeProtoMethod(
                        proto3MessageOrBuilder.clazz(), "has{}", field.protoDescriptor()),
                    Codegen.jumpTo(Opcodes.IFEQ, afterEndField));
              }

              add(
                  recordConsumerVar.load(),
                  new TextConstant(field.fieldWriter.fieldName),
                  IntegerConstant.forValue(field.fieldWriter.index),
                  Codegen.invokeMethod(Reflection.RecordConsumer.startField)
              );

              writeRawValue(proto3MessageOrBuilder);

              add(recordConsumerVar.load(),
                  new TextConstant(field.fieldWriter.fieldName),
                  IntegerConstant.forValue(field.fieldWriter.index),
                  Codegen.invokeMethod(Reflection.RecordConsumer.endField));

              if (field.isOptional()) {
                add(Codegen.visitLabel(afterEndField), localVars.frameEmptyStack());
              }
            }

            return this;
          }

          void beforeConvertRepeatedValue(LocalVar proto3MessageOrBuilder, LocalVar iVar) {
            add(recordConsumerVar.load(),
                proto3MessageOrBuilder.load(),
                iVar.load(),
                Codegen.invokeProtoMethod(proto3MessageOrBuilder.clazz(), getterMethodTemplate(), field.protoDescriptor(), int.class));
          }

          void afterConvertRepeatedValue(LocalVar proto3MessageOrBuilder, LocalVar iVar) {
          }

          void writeRepeatedRawValue(LocalVar proto3MessageOrBuilder, LocalVar iVar) {
            beforeConvertRepeatedValue(proto3MessageOrBuilder, iVar);
            convertRawValueAndWrite();
            afterConvertRepeatedValue(proto3MessageOrBuilder, iVar);
          }

          Implementation writeFromVar(LocalVar var) {
            add(recordConsumerVar.load(),
                new TextConstant(field.parquetFieldName()),
                IntegerConstant.forValue(field.parquetFieldIndex()),
                Codegen.invokeMethod(Reflection.RecordConsumer.startField)
            );
            add(recordConsumerVar.load(),
                var.load());
            convertRawValueAndWrite();
            add(recordConsumerVar.load(),
                new TextConstant(field.parquetFieldName()),
                IntegerConstant.forValue(field.parquetFieldIndex()),
                Codegen.invokeMethod(Reflection.RecordConsumer.endField)
            );
            return this;
          }

          void beforeConvertSingleValue(LocalVar proto3MessageOrBuilder) {
            add(recordConsumerVar.load(),
                proto3MessageOrBuilder.load(),
                Codegen.invokeProtoMethod(proto3MessageOrBuilder.clazz(), getterMethodTemplate(), field.protoDescriptor()));
          }

          void afterConvertSingleValue(LocalVar proto3MessageOrBuilder) {
          }

          void writeRawValue(LocalVar proto3MessageOrBuilder) {
            beforeConvertSingleValue(proto3MessageOrBuilder);
            convertRawValueAndWrite();
            afterConvertSingleValue(proto3MessageOrBuilder);
          }

          abstract void convertRawValueAndWrite();
        }

        private Implementation writePrimitiveField(
            LocalVar proto3MessageOrBuilder,
            LocalVar recordConsumerVar,
            MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field) {
          return new PrimitiveFieldWriter(field, recordConsumerVar).fieldGetConvertWrite(proto3MessageOrBuilder);
        }

        private Implementation writeBinaryField(LocalVar proto3MessageOrBuilder, LocalVar recordConsumerVar, MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field) {
          return new BinaryFieldWriter(field, recordConsumerVar).fieldGetConvertWrite(proto3MessageOrBuilder);
        }

        private Implementation writeStringField(LocalVar proto3MessageOrBuilder, LocalVar recordConsumerVar, MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field) {
          return new StringFieldWriter(field, recordConsumerVar).fieldGetConvertWrite(proto3MessageOrBuilder);
        }

        private Implementation writeProtoWrapperField(LocalVar proto3MessageOrBuilder, LocalVar recordConsumerVar, MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field) {
          return new ProtoWrapperFieldWriter(field, recordConsumerVar).fieldGetConvertWrite(proto3MessageOrBuilder);
        }

        private Implementation writeEnumField(LocalVar proto3MessageOrBuilder, LocalVar recordConsumerVar, MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.EnumWriter> field) {
          return new EnumFieldWriter(field, recordConsumerVar).fieldGetConvertWrite(proto3MessageOrBuilder);
        }

        private Implementation writeMessageField(
            LocalVar proto3MessageOrBuilder,
            LocalVar recordConsumerVar,
            MessageWriterVisitor.RegularField<?> field) {
          if (!field.getFieldReflectionTypeProto3MessageOrBuilderInterface()
              .isPresent()) {
            return null;
          }
          return new MessageFieldWriter(field, recordConsumerVar).fieldGetConvertWrite(proto3MessageOrBuilder);
        }

        class PrimitiveFieldWriter extends RegularFieldWriterTemplate {
          public PrimitiveFieldWriter(MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field,
                                      LocalVar recordConsumerVar) {
            super(field, recordConsumerVar);
          }

          @Override
          void convertRawValueAndWrite() {
            add(Codegen.invokeMethod(Reflection.RecordConsumer.PRIMITIVES.get(field.getFieldReflectionType())));
          }
        }

        class MessageFieldWriter extends RegularFieldWriterTemplate {

          MessageFieldWriter(MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field,
                             LocalVar recordConsumerVar) {
            super(field, recordConsumerVar);
          }

          String getterMethodTemplate() {
            return "get{}OrBuilder";
          }

          @Override
          void convertRawValueAndWrite() {
            MethodDescription methodDescription = fieldPathToMethodDescriptionMap.get(field.fieldPath);
            if (methodDescription == null) {
              throw new IllegalStateException("field: " + field);
            }
            add(MethodInvocation.invoke(
                methodDescription));
          }

          @Override
          void beforeConvertRepeatedValue(LocalVar proto3MessageOrBuilder, LocalVar iVar) {
            startGroup();
            add(MethodVariableAccess.loadThis(),
                proto3MessageOrBuilder.load(),
                iVar.load(),
                Codegen.invokeProtoMethod(
                    proto3MessageOrBuilder.clazz(),
                    getterMethodTemplate(),
                    field.protoDescriptor(),
                    int.class));
          }

          @Override
          void afterConvertRepeatedValue(LocalVar proto3MessageOrBuilder, LocalVar iVar) {
            endGroup();
          }

          Implementation writeFromVar(LocalVar var) {
            add(recordConsumerVar.load(),
                new TextConstant(field.parquetFieldName()),
                IntegerConstant.forValue(field.parquetFieldIndex()),
                Codegen.invokeMethod(Reflection.RecordConsumer.startField)
            );
            startGroup();
//             add(var.load());
            writeMessageFieldsInternal(var);
//             convertRawValueAndWrite();
            endGroup();
            add(recordConsumerVar.load(),
                new TextConstant(field.parquetFieldName()),
                IntegerConstant.forValue(field.parquetFieldIndex()),
                Codegen.invokeMethod(Reflection.RecordConsumer.endField)
            );
            return this;
          }

          @Override
          void beforeConvertSingleValue(LocalVar proto3MessageOrBuilder) {
            startGroup();

            add(MethodVariableAccess.loadThis(),
                proto3MessageOrBuilder.load(),
                Codegen.invokeProtoMethod(
                    proto3MessageOrBuilder.clazz(),
                    getterMethodTemplate(),
                    field.protoDescriptor()));
          }

          @Override
          void afterConvertSingleValue(LocalVar proto3MessageOrBuilder) {
            endGroup();
          }

          void startGroup() {
            add(recordConsumerVar.load(),
                Codegen.invokeMethod(Reflection.RecordConsumer.startGroup));
          }

          void endGroup() {
            add(recordConsumerVar.load(),
                Codegen.invokeMethod(Reflection.RecordConsumer.endGroup));
          }

          Implementation writeMessageFieldsInternal(
              LocalVar proto3MessageOrBuilder) {

            MessageWriterVisitor.traverse((MessageWriterVisitor.RegularField) field, new MessageWriterVisitor() {
              @Override
              public boolean visitMessageWriter(RegularField<?> field) {
                if (field == MessageFieldWriter.this.field) {
                  return true;
                }
                add(writeMessageRegularField(field, proto3MessageOrBuilder, recordConsumerVar));
                return false;
              }

              @Override
              public void visitNonMessageWriter(RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field) {
                add(writeNonMessageRegularField(field, proto3MessageOrBuilder, recordConsumerVar));
              }

              @Override
              public boolean visitMapMessageWriter(MapField field) {
                if (field.value().getFieldReflectionTypeProto3MessageOrBuilderInterface().isPresent()) {
                  add(writeMapField(field, proto3MessageOrBuilder));
                } else {
                  add(notOptimizedField(field));
                }
                return false;
              }

              @Override
              public void visitMapNonMessageWriter(MapField field) {
                if (field.value().isBinaryMessage()) {
                  add(notOptimizedField(field));
                } else {
                  add(writeMapField(field, proto3MessageOrBuilder));
                }
              }
            });

            return this;
          }

          private Implementation writeMapField(MessageWriterVisitor.MapField field, LocalVar proto3MessageOrBuilder) {
            return new Implementations() {{
              MethodDescription methodDescription = mapFieldPathToMethodDescriptionMap.get(field.fieldPath);
              if (methodDescription == null) {
                throw new IllegalStateException("field: " + field);
              }

              TypeDescription keyType = TypeDescription.ForLoadedType.of(field.fieldWriter.getFieldReflectionType().getKey()).asBoxed();
              TypeDescription valueType = TypeDescription.ForLoadedType.of(field.value().isEnum() ? Integer.class : field.fieldWriter.getFieldReflectionType().getValue()).asBoxed();

              Label after = new Label();
              add(proto3MessageOrBuilder.load(),
                  Codegen.invokeProtoMethod(proto3MessageOrBuilder.clazz(),"get{}Count", field.protoDescriptor()),
                  Codegen.jumpTo(Opcodes.IFLE, after),

                  recordConsumerVar.load(),
                  new TextConstant(field.parquetFieldName()),
                  IntegerConstant.forValue(field.parquetFieldIndex()),
                  Codegen.invokeMethod(Reflection.RecordConsumer.startField));

              if (writeSpecsCompliant()) {
                add(
                    recordConsumerVar.load(),
                    Codegen.invokeMethod(Reflection.RecordConsumer.startGroup),
                    recordConsumerVar.load(),
                    new TextConstant("key_value"),
                    IntegerConstant.forValue(0),
                    Codegen.invokeMethod(Reflection.RecordConsumer.startField)
                );
              }

              add(proto3MessageOrBuilder.load(),
                  Codegen.invokeProtoMethod(proto3MessageOrBuilder.clazz(),"get{}" + (field.value().isEnum() ? "Value" : "") + "Map", field.protoDescriptor()),
                  MethodVariableAccess.loadThis());

              add(new StackManipulation.Simple(new StackManipulation.Simple.Dispatcher() {
                @Override
                public StackManipulation.Size apply(MethodVisitor methodVisitor, Context implementationContext) {
                  methodVisitor.visitInvokeDynamicInsn(
                      "accept",
                      "(" + classBuilder[0].toTypeDescription().getDescriptor()+ ")Ljava/util/function/BiConsumer;",
                      new Handle(
                          Opcodes.H_INVOKESTATIC,
                          "java/lang/invoke/LambdaMetafactory",
                          "metafactory",
                          "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodHandle;Ljava/lang/invoke/MethodType;)Ljava/lang/invoke/CallSite;", false),
                      JavaConstantValue.Visitor.INSTANCE.onMethodType(JavaConstant.MethodType.of(void.class, Object.class, Object.class)),
                      new Handle(
                          Opcodes.H_INVOKEVIRTUAL,
                          classBuilder[0].toTypeDescription().getInternalName(),
                          methodDescription.getInternalName(),
                          methodDescription.getDescriptor(), false),
                      JavaConstantValue.Visitor.INSTANCE.onMethodType(JavaConstant.MethodType.of(TypeDescription.ForLoadedType.of(void.class), keyType, valueType))
                      );
                  return StackManipulation.Size.ZERO;
                }
              }));
              add(Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Map.class, "forEach", BiConsumer.class)));

              if (writeSpecsCompliant()) {
                add(
                    recordConsumerVar.load(),
                    new TextConstant("key_value"),
                    IntegerConstant.forValue(0),
                    Codegen.invokeMethod(Reflection.RecordConsumer.endField),
                    recordConsumerVar.load(),
                    Codegen.invokeMethod(Reflection.RecordConsumer.endGroup)
                );
              }

              add(recordConsumerVar.load(),
                  new TextConstant(field.parquetFieldName()),
                  IntegerConstant.forValue(field.parquetFieldIndex()),
                  Codegen.invokeMethod(Reflection.RecordConsumer.endField));

              add(Codegen.visitLabel(after),
                  localVars.frameEmptyStack()
              );
            }};
          }

          protected Implementation writeNonMessageRegularField(
              MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field,
              LocalVar proto3MessageOrBuilder, LocalVar recordConsumerVar) {
            Implementation implementation = null;
            if (field.isPrimitive()) {
              implementation = writePrimitiveField(proto3MessageOrBuilder, recordConsumerVar, field);
            } else if (field.isBinary()) {
              implementation = writeBinaryField(proto3MessageOrBuilder, recordConsumerVar, field);
            } else if (field.isString()) {
              implementation = writeStringField(proto3MessageOrBuilder, recordConsumerVar, field);
            } else if (field.isProtoWrapper()) {
              implementation = writeProtoWrapperField(proto3MessageOrBuilder, recordConsumerVar, field);
            } else if (field.isEnum()) {
              implementation = writeEnumField(proto3MessageOrBuilder, recordConsumerVar, (MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.EnumWriter>) field);
            }
            if (implementation == null) {
              implementation = notOptimizedField(field);
            }
            return implementation;
          }

          protected Implementation writeMessageRegularField(MessageWriterVisitor.RegularField<?> field,
                                                            LocalVar proto3MessageOrBuilder, LocalVar recordConsumerVar) {
            Implementation implementation =
                writeMessageField(proto3MessageOrBuilder, recordConsumerVar, field);
            if (implementation == null) {
              implementation = notOptimizedField(field);
            }
            return implementation;
          }

          protected Implementation notOptimizedField(MessageWriterVisitor.Field<?> field) {
            Implementation implementation;
            JavaReflectionProto3FastMessageWriters.FieldOfObjectWriter fieldOfObjectWriter =
                JavaReflectionProto3FastMessageWriters.createFieldOfObjectWriter(
                    field.getOwningTypeProto3MessageOrBuilderInterface()
                        .get(),
                    field.fieldWriter);

            implementation = MethodCall.invoke(Reflection.FieldOfObjectWriter.writeFieldOfObject)
                .on(fieldOfObjectWriter)
                .withArgument(0)
                .andThen(NOOP);
            return implementation;
          }
        }

        class BinaryFieldWriter extends RegularFieldWriterTemplate {
          BinaryFieldWriter(MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field,
                            LocalVar recordConsumerVar) {
            super(field, recordConsumerVar);
          }
          /*
            ByteString byteString = (ByteString) value;
            Binary binary = Binary.fromConstantByteArray(byteString.toByteArray());
            recordConsumer.addBinary(binary);
          */

          @Override
          void convertRawValueAndWrite() {
            add(
                Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(ByteString.class, "toByteArray")),
                Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Binary.class, "fromConstantByteArray", byte[].class)),
                Codegen.invokeMethod(Reflection.RecordConsumer.addBinary)
            );
          }
        }

        class StringFieldWriter extends RegularFieldWriterTemplate {
          StringFieldWriter(MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field,
                                    LocalVar recordConsumerVar) {
            super(field, recordConsumerVar);
          }

          /*
            Binary binaryString = Binary.fromString((String) value);
            recordConsumer.addBinary(binaryString);
          */
          @Override
          void convertRawValueAndWrite() {
            add(Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Binary.class, "fromString", String.class)),
                Codegen.invokeMethod(Reflection.RecordConsumer.addBinary));
          }
        }

        class ProtoWrapperFieldWriter extends RegularFieldWriterTemplate {
          ProtoWrapperFieldWriter(MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.FieldWriter> field,
                                         LocalVar recordConsumerVar) {
            super(field, recordConsumerVar);
          }

          @Override
          void convertRawValueAndWrite() {
            ProtoWriteSupport<?>.FieldWriter fieldWriter = field.rawValueWriter();
            if (fieldWriter instanceof ProtoWriteSupport<?>.BytesValueWriter) {
              add(
                Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(BytesValue.class, "getValue")),
                Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(ByteString.class, "toByteArray")),
                Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Binary.class, "fromConstantByteArray", byte[].class)),
                Codegen.invokeMethod(Reflection.RecordConsumer.addBinary)
              );
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.StringValueWriter) {
              add(
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(StringValue.class, "getValue")),
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Binary.class, "fromString", String.class)),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addBinary)
              );
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.BoolValueWriter) {
              add(
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(BoolValue.class, "getValue")),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addBoolean)
              );
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.UInt32ValueWriter) {
              add(
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(UInt32Value.class, "getValue")),
                  Codegen.castIntToLong(),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addLong)
              );
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.Int32ValueWriter) {
              add(
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Int32Value.class, "getValue")),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addInteger)
              );
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.UInt64ValueWriter) {
              add(
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(UInt64Value.class, "getValue")),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addLong)
              );
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.Int64ValueWriter) {
              add(
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Int64Value.class, "getValue")),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addLong)
              );
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.FloatValueWriter) {
              add(
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(FloatValue.class, "getValue")),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addFloat)
              );
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.DoubleValueWriter) {
              add(
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(DoubleValue.class, "getValue")),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addDouble)
              );
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.TimeWriter) {
              try (LocalVar timeOfDay = localVars.register(TimeOfDay.class)) {
                add(
                    timeOfDay.store(),
                    timeOfDay.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(TimeOfDay.class, "getHours")),
                    timeOfDay.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(TimeOfDay.class, "getMinutes")),
                    timeOfDay.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(TimeOfDay.class, "getSeconds")),
                    timeOfDay.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(TimeOfDay.class, "getNanos")),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(LocalTime.class, "of", int.class, int.class, int.class, int.class)),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(LocalTime.class, "toNanoOfDay")),
                    Codegen.invokeMethod(Reflection.RecordConsumer.addLong)
                );
              }
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.DateWriter) {
              try (LocalVar date = localVars.register(Date.class)) {
                add(
                    date.store(),
                    date.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Date.class, "getYear")),
                    date.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Date.class, "getMonth")),
                    date.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Date.class, "getDay")),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(LocalDate.class, "of", int.class, int.class, int.class)),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(LocalDate.class, "toEpochDay")),
                    Codegen.castLongToInt(),
                    Codegen.invokeMethod(Reflection.RecordConsumer.addInteger)
                );
              }
            } else if (fieldWriter instanceof ProtoWriteSupport<?>.TimestampWriter) {
              add(
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Timestamps.class, "toNanos", Timestamp.class)),
                  Codegen.invokeMethod(Reflection.RecordConsumer.addLong)
              );
            } else {
              throw new IllegalStateException();
            }
          }
        }

        class EnumFieldWriter extends RegularFieldWriterTemplate {

          EnumFieldWriter(MessageWriterVisitor.RegularField<? extends ProtoWriteSupport<?>.EnumWriter> field,
                                 LocalVar recordConsumerVar) {
            super(field, recordConsumerVar);
          }

          String getterMethodTemplate() {
            return "get{}Value";
          }

          @Override
          void beforeConvertRepeatedValue(LocalVar proto3MessageOrBuilder, LocalVar iVar) {
            add(proto3MessageOrBuilder.load(),
                iVar.load(),
                Codegen.invokeProtoMethod(proto3MessageOrBuilder.clazz(), getterMethodTemplate(), field.protoDescriptor(), int.class));
          }

          Implementation writeFromVar(LocalVar var) {
            add(recordConsumerVar.load(),
                new TextConstant(field.parquetFieldName()),
                IntegerConstant.forValue(field.parquetFieldIndex()),
                Codegen.invokeMethod(Reflection.RecordConsumer.startField)
            );
            add(var.load());
            convertRawValueAndWrite();
            add(recordConsumerVar.load(),
                new TextConstant(field.parquetFieldName()),
                IntegerConstant.forValue(field.parquetFieldIndex()),
                Codegen.invokeMethod(Reflection.RecordConsumer.endField)
            );
            return this;
          }

          @Override
          void beforeConvertSingleValue(LocalVar proto3MessageOrBuilder) {
            add(proto3MessageOrBuilder.load(),
                Codegen.invokeProtoMethod(proto3MessageOrBuilder.clazz(), getterMethodTemplate(), field.protoDescriptor()));
          }

          /**
           *         int enumNumber = getEnumValue.getValue(object);
           *         ProtocolMessageEnum enum_ = forNumber.apply(enumNumber);
           *         Enum<?> javaEnum = (Enum<?>) enum_;
           *         Descriptors.EnumValueDescriptor enumValueDescriptor;
           *         if (javaEnum != null) {
           *           enumValueDescriptor = enumValues.get(javaEnum.ordinal());
           *         } else {
           *           enumValueDescriptor = enumDescriptor.findValueByNumberCreatingIfUnknown(enumNumber);
           *         }
           */
          @Override
          void convertRawValueAndWrite() {
            ProtoWriteSupport<?>.EnumWriter enumWriter = (ProtoWriteSupport<?>.EnumWriter) field.rawValueWriter();
            String enumTypeFullName = enumWriter.fieldDescriptor.getEnumType().getFullName();

            try(LocalVar enumNumber = localVars.register(int.class)) {
              Class<?> enumClass = enumTypeFullNameToClassMap.get(enumTypeFullName);
              add(enumNumber.store(),
                  enumNumber.load(),
                  Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(enumClass, "forNumber", int.class))
              );
              try (LocalVar enumRef = localVars.register(enumClass)) {
                add(
                    enumRef.store(),
                    enumRef.load()
                );
                Label ifEnumRefIsNull = new Label();
                Label afterEnumValueResolved = new Label();
                add(Codegen.jumpTo(Opcodes.IFNULL, ifEnumRefIsNull),
                    MethodVariableAccess.loadThis(),
                    FieldAccess.forField(classBuilder[0].toTypeDescription().getDeclaredFields()
                        .filter(ElementMatchers.named("enumValues" + enumTypeFullNameToFieldIdx.get(enumTypeFullName)))
                        .getOnly()).read(),
                    enumRef.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Enum.class, "ordinal")),
                    ArrayAccess.REFERENCE.load(),
                    Codegen.jumpTo(Opcodes.GOTO, afterEnumValueResolved),
                    Codegen.visitLabel(ifEnumRefIsNull),
                    localVars.frameEmptyStack(),
                    MethodVariableAccess.loadThis(),
                    FieldAccess.forField(classBuilder[0].toTypeDescription().getDeclaredFields()
                        .filter(ElementMatchers.named("enumDescriptor" + enumTypeFullNameToFieldIdx.get(enumTypeFullName)))
                        .getOnly()).read(),
                    enumNumber.load(),
                    Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Descriptors.EnumDescriptor.class, "findValueByNumberCreatingIfUnknown", int.class)),
                    Codegen.visitLabel(afterEnumValueResolved),
                    localVars.frameSame1(Descriptors.EnumValueDescriptor.class)
                    );

                try (LocalVar enumValueDesc = localVars.register(Descriptors.EnumValueDescriptor.class)) {
                  add(enumValueDesc.store());
                  try (LocalVar enumValueDescName = localVars.register(String.class)) {
                    add(enumValueDesc.load(),
                        Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Descriptors.EnumValueDescriptor.class, "getName")),
                        enumValueDescName.store(),
                        enumValueDescName.load(),
                        Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Binary.class, "fromString", String.class))
                    );
                    try (LocalVar binary = localVars.register(Binary.class)) {
                      add(binary.store(),
                          recordConsumerVar.load(),
                          binary.load(),
                          Codegen.invokeMethod(Reflection.RecordConsumer.addBinary),
                          MethodVariableAccess.loadThis(),
                          FieldAccess.forField(classBuilder[0].toTypeDescription().getDeclaredFields()
                              .filter(ElementMatchers.named("enumNameNumberPairs" + enumTypeFullNameToFieldIdx.get(enumTypeFullName)))
                              .getOnly()).read(),
                          enumValueDescName.load(),
                          enumValueDesc.load(),
                          Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Descriptors.EnumValueDescriptor.class, "getNumber")),
                          Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Integer.class, "valueOf", int.class)),
                          Codegen.invokeMethod(ReflectionUtil.getDeclaredMethod(Map.class, "putIfAbsent", Object.class, Object.class)),
                          Removal.SINGLE
                      );
                    }
                  }
                }
              }
            }
          }
        }
      }

      static class Codegen {
        private static StackManipulation incIntVar(LocalVar var, int inc) {
          int offset = var.offset();
          return new StackManipulation.AbstractBase() {
            @Override
            public Size apply(MethodVisitor methodVisitor, Context implementationContext) {
              methodVisitor.visitIincInsn(offset, inc);
              return Size.ZERO;
            }
          };
        }

        private static StackManipulation jumpTo(int jumpInst, Label label) {
          return new StackManipulation.AbstractBase() {
            @Override
            public Size apply(MethodVisitor methodVisitor, Context implementationContext) {
              methodVisitor.visitJumpInsn(jumpInst, label);
              return Size.ZERO;
            }
          };
        }

        private static StackManipulation visitLabel(Label label) {
          return new StackManipulation.AbstractBase() {
            @Override
            public Size apply(MethodVisitor methodVisitor, Context implementationContext) {
              methodVisitor.visitLabel(label);
              return Size.ZERO;
            }
          };
        }

        private static Implementation returnTrue() {
          return FixedValue.value(true);
        }

        private static Implementation returnFalse() {
          return FixedValue.value(false);
        }

        private static Implementation returnVoid() {
          return new Implementations(MethodReturn.VOID);
        }

        private static StackManipulation castLongToInt() {
          return castPrimitive(Opcodes.L2I);
        }

        private static StackManipulation castIntToLong() {
          return castPrimitive(Opcodes.I2L);
        }

        private static StackManipulation castPrimitive(int opcode) {
          return new StackManipulation.AbstractBase() {
            @Override
            public Size apply(MethodVisitor methodVisitor, Context implementationContext) {
              methodVisitor.visitInsn(opcode);
              return Size.ZERO;
            }
          };
        }

        private static StackManipulation invokeMethod(Method method) {
          return MethodInvocation.invoke(new MethodDescription.ForLoadedMethod(method));
        }

        private static StackManipulation invokeProtoMethod(
            Class<?> proto3MessageOrBuilderInterface,
            String name,
            FieldDescriptor fieldDescriptor,
            Class<?>... parameters) {
          return invokeMethod(ReflectionUtil.getDeclaredMethod(
              proto3MessageOrBuilderInterface, name, fieldDescriptor, parameters));
        }

        private static StackManipulation storeRecordConsumer(LocalVar recordConsumerVar) {
          return new StackManipulation.Compound(
              MethodVariableAccess.loadThis(),
              invokeMethod(Reflection.ByteBuddyProto3FastMessageWriter.getRecordConsumer),
              recordConsumerVar.store());
        }

        private static StackManipulation castToProto3MessageOrBuilderInterface(
            LocalVar dest,
            LocalVar src,
            Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
          return new StackManipulation.Compound(
              src.load(),
              TypeCasting.to(TypeDescription.ForLoadedType.of(proto3MessageOrBuilderInterface)),
              dest.store());
        }

        private static Implementation returnFalseIfNotInstanceOf(LocalVar var, Class<?> clazz) {
          return new Implementations() {
            {
              Label afterInstanceOf = new Label();
              add(
                  var.load(),
                  InstanceCheck.of(TypeDescription.ForLoadedType.of(clazz)),
                  jumpTo(Opcodes.IFNE, afterInstanceOf));
              add(returnFalse());
              add(visitLabel(afterInstanceOf), var.vars().frameEmptyStack());
            }
          };
        }
      }

      private void registerWriteAllFieldsForMessageMethod(MessageWriterVisitor.RegularField<?> field) {
        String methodName;
        FieldPath fieldPath = field.fieldPath;
        if (fieldPath.isRoot()) {
          methodName = Reflection.FastMessageWriter.writeAllFields.getName();
        } else {
          methodName = Reflection.FastMessageWriter.writeAllFields.getName() + "$"
              + fieldPathToMethodDescriptionMap.size();
        }
        fieldPathToMethodNameMap.put(fieldPath, methodName);

        MethodDescription.Token token = new MethodDescription.Token(
            methodName,
            fieldPath.isRoot() ? Visibility.PUBLIC.getMask() : Visibility.PRIVATE.getMask(),
            ForLoadedType.of(fieldPath.isRoot() ? boolean.class : void.class),
            Arrays.asList(ForLoadedType.of(fieldPath.isRoot() ? MessageOrBuilder.class :field.getFieldReflectionTypeProto3MessageOrBuilderInterface()
                .get())));

        fieldPathToMethodDescriptionMap.put(fieldPath, new MethodDescription.Latent(classBuilder[0].toTypeDescription(), token));
      }

      private void registerWriteMapEntry(MessageWriterVisitor.MapField field) {
        String methodName = "writeMapEntry$"
            + mapFieldPathToMethodDescriptionMap.size();
        FieldPath fieldPath = field.fieldPath;
        mapFieldPathToMethodNameMap.put(fieldPath, methodName);

        MethodDescription.Token token = new MethodDescription.Token(
            methodName,
            Visibility.PRIVATE.getMask(),
            ForLoadedType.of(void.class),
            Arrays.asList(ForLoadedType.of(field.key().getFieldReflectionType()), ForLoadedType.of(field.value().isEnum() ? int.class : field.value().getFieldReflectionType())));

        mapFieldPathToMethodDescriptionMap.put(fieldPath, new MethodDescription.Latent(classBuilder[0].toTypeDescription(), token));
      }

      static class Implementations implements Implementation {
        final List<Implementation> implementations = new ArrayList<>();
        final List<StackManipulation> ongoing = new ArrayList<>();

        Implementation compound;

        Implementations() {}

        Implementations(Implementation... implementations) {
          add(implementations);
        }

        Implementations(ByteCodeAppender... appenders) {
          add(appenders);
        }

        Implementations(StackManipulation... stackManipulations) {
          add(stackManipulations);
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
          return compound.appender(implementationTarget);
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
          if (compound != null) {
            throw new IllegalStateException();
          }
          flushOngoing();
          compound = new Implementation.Compound(implementations);
          return compound.prepare(instrumentedType);
        }

        public Implementations add(Implementation... implementations) {
          flushOngoing();
          this.implementations.addAll(Arrays.asList(implementations));
          return this;
        }

        public Implementations add(ByteCodeAppender... appenders) {
          return add(new Implementation.Simple(appenders));
        }

        public Implementations add(StackManipulation... stackManipulations) {
          ongoing.addAll(Arrays.asList(stackManipulations));
          return this;
        }

        private void flushOngoing() {
          if (!ongoing.isEmpty()) {
            implementations.add(new Implementation.Simple(ongoing.toArray(new StackManipulation[0])));
            ongoing.clear();
          }
        }
      }

      static class LocalVar implements AutoCloseable {
        final LocalVars vars;
        final TypeDescription typeDescription;
        final Class<?> clazz;
        final int stackSize;

        int refCount;

        int offset;

        LocalVar(Class<?> clazz, TypeDescription typeDescription, LocalVars vars) {
          this.clazz = clazz;
          this.typeDescription = typeDescription;
          this.vars = vars;
          this.stackSize = StackSize.of(typeDescription);
        }

        LocalVars vars() {
          return vars;
        }

        int offset() {
          assertRegistered();
          return offset;
        }

        TypeDescription typeDescription() {
          return typeDescription;
        }

        StackManipulation load() {
          return MethodVariableAccess.of(typeDescription()).loadFrom(offset());
        }

        StackManipulation store() {
          return MethodVariableAccess.of(typeDescription()).storeAt(offset());
        }

        Class<?> clazz() {
          if (clazz == null) {
            throw new IllegalStateException();
          }
          return clazz;
        }

        int stackSize() {
          return stackSize;
        }

        LocalVar register() {
          vars.register(this);
          return this;
        }

        LocalVar alias() {
          assertRegistered();
          refCount += 1;
          return this;
        }

        LocalVar unregister() {
          int index = assertRegistered();
          refCount -= 1;
          if (refCount == 0) {
            if (index != vars.vars.size() - 1) {
              throw new IllegalStateException("cannot deregister var " + this + "  from " + vars.vars);
            }
            vars.vars.remove(this);
          }
          return this;
        }

        int assertRegistered() {
          int index = getIndex();
          if (index < 0) {
            throw new IllegalStateException("not registered");
          }
          return index;
        }

        int getIndex() {
          return vars.vars.indexOf(this);
        }

        @Override
        public void close() {
          unregister();
        }

        @Override
        public String toString() {
          return "LocalVar{" + "vars="
              + vars + ", typeDescription="
              + typeDescription + ", stackSize="
              + stackSize + ", offset="
              + offset + '}';
        }
      }

      static class LocalVars {
        final List<TypeDescription> frame = new ArrayList<>();
        List<LocalVar> vars = new ArrayList<>();
        int maxSize;

        LocalVar register(LocalVar var) {
          if (vars.contains(var)) {
            throw new IllegalStateException("cannot register var twice: " + var + ", " + vars);
          }
          int offset =
              vars.isEmpty() ? 0 : vars.get(vars.size() - 1).offset + vars.get(vars.size() - 1).stackSize;
          vars.add(var);
          var.offset = offset;
          var.refCount = 1;

          maxSize = Math.max(maxSize, getSize());
          return var;
        }

        StackManipulation frameSame1(Class<?> varOnStack) {
          List<TypeDescription> currTypes = types();
          try {
            return new StackManipulation.AbstractBase() {
              @Override
              public Size apply(MethodVisitor methodVisitor, Context implementationContext) {
                implementationContext
                    .getFrameGeneration()
                    .same1(methodVisitor, TypeDescription.ForLoadedType.of(varOnStack), currTypes);
                return Size.ZERO;
              }
            };
          } finally {
            this.frame.clear();
            this.frame.addAll(currTypes);
          }
        }

        StackManipulation frameEmptyStack() {
          List<TypeDescription> currTypes = types();
          List<TypeDescription> frame = new ArrayList<>(this.frame);
          try {
            if (currTypes.size() < frame.size()) {
              int commonLength = commonTypesLength(currTypes, frame);
              if (commonLength < currTypes.size() || frame.size() - currTypes.size() > 3) {
                return new StackManipulation.AbstractBase() {
                  @Override
                  public Size apply(MethodVisitor methodVisitor, Context implementationContext) {
                    implementationContext
                        .getFrameGeneration()
                        .full(methodVisitor, Collections.emptyList(), currTypes);
                    return Size.ZERO;
                  }
                };
              } else {
                return new StackManipulation.AbstractBase() {
                  @Override
                  public Size apply(MethodVisitor methodVisitor, Context implementationContext) {
                    implementationContext
                        .getFrameGeneration()
                        .chop(methodVisitor, frame.size() - currTypes.size(), currTypes);
                    return Size.ZERO;
                  }
                };
              }
            } else if (currTypes.size() == frame.size()) {
              int commonLength = commonTypesLength(currTypes, frame);
              if (commonLength != currTypes.size()) {
                return new StackManipulation.AbstractBase() {
                  @Override
                  public Size apply(MethodVisitor methodVisitor, Context implementationContext) {
                    implementationContext
                        .getFrameGeneration()
                        .full(methodVisitor, Collections.emptyList(), currTypes);
                    return Size.ZERO;
                  }
                };
              } else {
                return new StackManipulation.AbstractBase() {
                  @Override
                  public Size apply(MethodVisitor methodVisitor, Context implementationContext) {
                    implementationContext
                        .getFrameGeneration()
                        .same(methodVisitor, currTypes);
                    return Size.ZERO;
                  }
                };
              }
            } else {
              int commonLength = commonTypesLength(currTypes, frame);
              if (commonLength < frame.size() || currTypes.size() - frame.size() > 3) {
                return new StackManipulation.AbstractBase() {
                  @Override
                  public Size apply(MethodVisitor methodVisitor, Context implementationContext) {
                    implementationContext
                        .getFrameGeneration()
                        .full(methodVisitor, Collections.emptyList(), currTypes);
                    return Size.ZERO;
                  }
                };
              } else {
                return new StackManipulation.AbstractBase() {
                  @Override
                  public Size apply(MethodVisitor methodVisitor, Context implementationContext) {
                    implementationContext
                        .getFrameGeneration()
                        .append(
                            methodVisitor,
                            currTypes.subList(frame.size(), currTypes.size()),
                            frame);
                    return Size.ZERO;
                  }
                };
              }
            }
          } finally {
            this.frame.clear();
            this.frame.addAll(currTypes);
          }
        }

        int commonTypesLength(List<TypeDescription> a, List<TypeDescription> b) {
          int len = Math.min(a.size(), b.size());
          for (int i = 0; i < len; i++) {
            if (!Objects.equals(a.get(i), b.get(i))) {
              return i;
            }
          }
          return len;
        }

        LocalVar register(TypeDescription typeDescription) {
          LocalVar var = new LocalVar(null, typeDescription, this);
          return register(var);
        }

        LocalVar register(Class<?> clazz) {
          LocalVar var = new LocalVar(clazz, TypeDescription.ForLoadedType.of(clazz), this);
          return register(var);
        }

        Implementation asImplementation() {
          return new Implementation.Simple(new ByteCodeAppender() {
            @Override
            public Size apply(
                MethodVisitor methodVisitor,
                Context implementationContext,
                MethodDescription instrumentedMethod) {
              return new Size(0, maxSize);
            }
          });
        }

        int getSize() {
          int size = 0;
          for (LocalVar var : vars) {
            size += var.stackSize();
          }
          return size;
        }

        List<TypeDescription> types() {
          List<TypeDescription> types = new ArrayList<>();
          for (LocalVar var : vars) {
            types.add(var.typeDescription);
          }
          return types;
        }
      }
    }
  }

  abstract static class Proto3FastMessageWriters {
    void createFastMessageWriters(ProtoWriteSupport<?>.MessageWriter messageWriter) {
      assertOptimizationAvailable();
      Queue<ProtoWriteSupport<?>.FieldWriter> fieldWriters = new LinkedList<>();
      fieldWriters.add(messageWriter);
      while (!fieldWriters.isEmpty()) {
        ProtoWriteSupport<?>.FieldWriter fieldWriter = fieldWriters.poll();
        if (fieldWriter instanceof ProtoWriteSupport.MessageWriter) {
          ProtoWriteSupport<?>.MessageWriter writer = (ProtoWriteSupport<?>.MessageWriter) fieldWriter;
          if (ReflectionUtil.getProto3MessageOrBuilderInterface(writer.protoMessageClass)
              .isPresent()) {
            FastMessageWriter fastMessageWriter = generateFastMessageWriter(writer, fieldWriters);
            writer.setFastMessageWriter(fastMessageWriter);
          } else {
            fieldWriters.addAll(Arrays.asList(writer.fieldWriters));
          }
        } else if (fieldWriter instanceof ProtoWriteSupport.MapWriter) {
          ProtoWriteSupport<?>.MapWriter writer = (ProtoWriteSupport<?>.MapWriter) fieldWriter;
          fieldWriters.add(writer.valueWriter);
        } else if (fieldWriter instanceof ProtoWriteSupport.ArrayWriter) {
          ProtoWriteSupport<?>.ArrayWriter writer = (ProtoWriteSupport<?>.ArrayWriter) fieldWriter;
          fieldWriters.add(writer.fieldWriter);
        }
      }
    }

    void assertOptimizationAvailable() {}

    abstract FastMessageWriter generateFastMessageWriter(
        ProtoWriteSupport<?>.MessageWriter messageWriter,
        Queue<ProtoWriteSupport<?>.FieldWriter> furtherFieldWriters);
  }

  static class JavaReflectionProto3FastMessageWriters extends Proto3FastMessageWriters {

    static FieldHasValue getHasValueOrNull(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      Method hasValueMethodOrNull = getHasValueMethodOrNull(fieldDescriptor, proto3MessageOrBuilderInterface);
      if (hasValueMethodOrNull == null) {
        return null;
      }
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (FieldHasValue) LambdaMetafactory.metafactory(
              lookup,
              "hasValue",
              MethodType.methodType(FieldHasValue.class),
              MethodType.methodType(boolean.class, Object.class),
              lookup.unreflect(hasValueMethodOrNull),
              MethodType.methodType(boolean.class, proto3MessageOrBuilderInterface))
          .getTarget()
          .invokeExact();
    }

    static GetRepeatedFieldSize getRepeatedFieldSize(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (GetRepeatedFieldSize) LambdaMetafactory.metafactory(
              lookup,
              "getSize",
              MethodType.methodType(GetRepeatedFieldSize.class),
              MethodType.methodType(int.class, Object.class),
              lookup.unreflect(
                  getRepeatedFieldSizeMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(int.class, proto3MessageOrBuilderInterface))
          .getTarget()
          .invokeExact();
    }

    static ObjectValueGetter getGetObjectValue(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (ObjectValueGetter) LambdaMetafactory.metafactory(
              lookup,
              "getValue",
              MethodType.methodType(ObjectValueGetter.class),
              MethodType.methodType(Object.class, Object.class),
              lookup.unreflect(getGetObjectValueMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(Object.class, proto3MessageOrBuilderInterface))
          .getTarget()
          .invokeExact();
    }

    static IntValueGetter getGetIntValue(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (IntValueGetter) LambdaMetafactory.metafactory(
              lookup,
              "getValue",
              MethodType.methodType(IntValueGetter.class),
              MethodType.methodType(int.class, Object.class),
              lookup.unreflect(getGetIntValueMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(int.class, proto3MessageOrBuilderInterface))
          .getTarget()
          .invokeExact();
    }

    static <T extends ProtocolMessageEnum> IntFunction<T> getEnumForNumber(Class<T> enumClazz) throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (IntFunction<T>) LambdaMetafactory.metafactory(
              lookup,
              "apply",
              MethodType.methodType(IntFunction.class),
              MethodType.methodType(Object.class, int.class),
              lookup.unreflect(getEnumForNumberMethod(enumClazz)),
              MethodType.methodType(enumClazz, int.class))
          .getTarget()
          .invokeExact();
    }

    static LongValueGetter getGetLongValue(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (LongValueGetter) LambdaMetafactory.metafactory(
              lookup,
              "getValue",
              MethodType.methodType(LongValueGetter.class),
              MethodType.methodType(long.class, Object.class),
              lookup.unreflect(getGetLongValueMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(long.class, proto3MessageOrBuilderInterface))
          .getTarget()
          .invokeExact();
    }

    static DoubleValueGetter getGetDoubleValue(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (DoubleValueGetter) LambdaMetafactory.metafactory(
              lookup,
              "getValue",
              MethodType.methodType(DoubleValueGetter.class),
              MethodType.methodType(double.class, Object.class),
              lookup.unreflect(getGetDoubleValueMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(double.class, proto3MessageOrBuilderInterface))
          .getTarget()
          .invokeExact();
    }

    static FloatValueGetter getGetFloatValue(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (FloatValueGetter) LambdaMetafactory.metafactory(
              lookup,
              "getValue",
              MethodType.methodType(FloatValueGetter.class),
              MethodType.methodType(float.class, Object.class),
              lookup.unreflect(getGetFloatValueMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(float.class, proto3MessageOrBuilderInterface))
          .getTarget()
          .invokeExact();
    }

    static BooleanValueGetter getGetBooleanValue(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (BooleanValueGetter) LambdaMetafactory.metafactory(
              lookup,
              "getValue",
              MethodType.methodType(BooleanValueGetter.class),
              MethodType.methodType(boolean.class, Object.class),
              lookup.unreflect(
                  getGetBooleanValueMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(boolean.class, proto3MessageOrBuilderInterface))
          .getTarget()
          .invokeExact();
    }

    static ObjectListElementGetter getGetObjectListElement(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (ObjectListElementGetter) LambdaMetafactory.metafactory(
              lookup,
              "getElement",
              MethodType.methodType(ObjectListElementGetter.class),
              MethodType.methodType(Object.class, Object.class, int.class),
              lookup.unreflect(
                  getGetObjectListElementMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(Object.class, proto3MessageOrBuilderInterface, int.class))
          .getTarget()
          .invokeExact();
    }

    static IntListElementGetter getGetIntListElement(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (IntListElementGetter) LambdaMetafactory.metafactory(
              lookup,
              "getElement",
              MethodType.methodType(IntListElementGetter.class),
              MethodType.methodType(int.class, Object.class, int.class),
              lookup.unreflect(
                  getGetIntListElementMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(int.class, proto3MessageOrBuilderInterface, int.class))
          .getTarget()
          .invokeExact();
    }

    static LongListElementGetter getGetLongListElement(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (LongListElementGetter) LambdaMetafactory.metafactory(
              lookup,
              "getElement",
              MethodType.methodType(LongListElementGetter.class),
              MethodType.methodType(long.class, Object.class, int.class),
              lookup.unreflect(
                  getGetLongListElementMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(long.class, proto3MessageOrBuilderInterface, int.class))
          .getTarget()
          .invokeExact();
    }

    static DoubleListElementGetter getGetDoubleListElement(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (DoubleListElementGetter) LambdaMetafactory.metafactory(
              lookup,
              "getElement",
              MethodType.methodType(DoubleListElementGetter.class),
              MethodType.methodType(double.class, Object.class, int.class),
              lookup.unreflect(
                  getGetDoubleListElementMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(double.class, proto3MessageOrBuilderInterface, int.class))
          .getTarget()
          .invokeExact();
    }

    static FloatListElementGetter getGetFloatListElement(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (FloatListElementGetter) LambdaMetafactory.metafactory(
              lookup,
              "getElement",
              MethodType.methodType(FloatListElementGetter.class),
              MethodType.methodType(float.class, Object.class, int.class),
              lookup.unreflect(
                  getGetFloatListElementMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(float.class, proto3MessageOrBuilderInterface, int.class))
          .getTarget()
          .invokeExact();
    }

    static BooleanListElementGetter getGetBooleanListElement(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
        throws Throwable {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (BooleanListElementGetter) LambdaMetafactory.metafactory(
              lookup,
              "getElement",
              MethodType.methodType(BooleanListElementGetter.class),
              MethodType.methodType(boolean.class, Object.class, int.class),
              lookup.unreflect(
                  getGetBooleanListElementMethod(fieldDescriptor, proto3MessageOrBuilderInterface)),
              MethodType.methodType(boolean.class, proto3MessageOrBuilderInterface, int.class))
          .getTarget()
          .invokeExact();
    }

    static Method getHasValueMethodOrNull(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (fieldDescriptor.isRepeated()) {
        throw new IllegalStateException("not supported for repeated fields, but got " + fieldDescriptor);
      }
      if (!fieldDescriptor.hasPresence()) {
        return null;
      }
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface, "has" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor));
    }

    static Method getRepeatedFieldSizeMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (!fieldDescriptor.isRepeated()) {
        throw new IllegalStateException("not supported for non-repeated fields, but got " + fieldDescriptor);
      }
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface,
          "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor) + "Count");
    }

    static Method getGetObjectValueMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (fieldDescriptor.isRepeated() && !fieldDescriptor.isMapField()) {
        throw new IllegalStateException("not supported for repeated fields, but got " + fieldDescriptor);
      }
      String mapOrBuilder = fieldDescriptor.isMapField()
          ? fieldDescriptor.getMessageType().getFields().get(1).getJavaType() == FieldDescriptor.JavaType.ENUM
              ? "ValueMap"
              : "Map"
          : fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE ? "OrBuilder" : "";
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface,
          "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor) + mapOrBuilder);
    }

    static Method getGetIntValueMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (fieldDescriptor.isRepeated()
          || !(fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.INT
              || fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.ENUM)) {
        throw new IllegalStateException(
            "not supported for repeated fields or non-int or non-enums, but got " + fieldDescriptor);
      }
      String enumValueSuffix = fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.ENUM ? "Value" : "";
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface,
          "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor) + enumValueSuffix);
    }

    static Method getEnumForNumberMethod(Class<? extends ProtocolMessageEnum> clazz) {
      return ReflectionUtil.getDeclaredMethod(clazz, "forNumber", int.class);
    }

    static Method getGetLongValueMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.LONG) {
        throw new IllegalStateException(
            "not supported for repeated fields or non-long, but got " + fieldDescriptor);
      }
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface, "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor));
    }

    static Method getGetDoubleValueMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.DOUBLE) {
        throw new IllegalStateException(
            "not supported for repeated fields or non-double, but got " + fieldDescriptor);
      }
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface, "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor));
    }

    static Method getGetFloatValueMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.FLOAT) {
        throw new IllegalStateException(
            "not supported for repeated fields or non-float, but got " + fieldDescriptor);
      }
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface, "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor));
    }

    static Method getGetBooleanValueMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.BOOLEAN) {
        throw new IllegalStateException(
            "not supported for repeated fields or non-boolean, but got " + fieldDescriptor);
      }
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface, "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor));
    }

    static Method getGetObjectListElementMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (!fieldDescriptor.isRepeated() || fieldDescriptor.isMapField()) {
        throw new IllegalStateException(
            "not supported for non-repeated fields or maps, but got " + fieldDescriptor);
      }
      String orBuilder =
          !fieldDescriptor.isMapField() && fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE
              ? "OrBuilder"
              : "";
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface,
          "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor) + orBuilder,
          int.class);
    }

    static Method getGetIntListElementMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (!fieldDescriptor.isRepeated()
          || !(fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.INT
              || fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.ENUM)) {
        throw new IllegalStateException(
            "not supported for non-repeated fields or non-ints or non-enums, but got " + fieldDescriptor);
      }
      String enumValueSuffix = fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.ENUM ? "Value" : "";
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface,
          "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor) + enumValueSuffix,
          int.class);
    }

    static Method getGetLongListElementMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (!fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.LONG) {
        throw new IllegalStateException(
            "not supported for non-repeated fields or non-longs, but got " + fieldDescriptor);
      }
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface,
          "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor),
          int.class);
    }

    static Method getGetDoubleListElementMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (!fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.DOUBLE) {
        throw new IllegalStateException(
            "not supported for non-repeated fields or non-doubles, but got " + fieldDescriptor);
      }
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface,
          "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor),
          int.class);
    }

    static Method getGetFloatListElementMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (!fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.FLOAT) {
        throw new IllegalStateException(
            "not supported for non-repeated fields or non-floats, but got " + fieldDescriptor);
      }
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface,
          "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor),
          int.class);
    }

    static Method getGetBooleanListElementMethod(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      if (!fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.BOOLEAN) {
        throw new IllegalStateException(
            "not supported for non-repeated fields or non-booleans, but got " + fieldDescriptor);
      }
      return ReflectionUtil.getDeclaredMethod(
          proto3MessageOrBuilderInterface,
          "get" + ReflectionUtil.getFieldNameForMethod(fieldDescriptor),
          int.class);
    }

    static class JavaReflectionProto3FastMessageWriter implements FastMessageWriter {
      final Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface;
      final JavaReflectionProto3FastMessageWriters.FieldOfObjectWriter[] fieldOfObjectWriters;

      JavaReflectionProto3FastMessageWriter(
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface,
          JavaReflectionProto3FastMessageWriters.FieldOfObjectWriter[] fieldOfObjectWriters) {
        this.proto3MessageOrBuilderInterface = proto3MessageOrBuilderInterface;
        this.fieldOfObjectWriters = fieldOfObjectWriters;
      }

      @Override
      public boolean writeAllFields(MessageOrBuilder messageOrBuilder) {
        if (proto3MessageOrBuilderInterface.isInstance(messageOrBuilder)) {
          for (JavaReflectionProto3FastMessageWriters.FieldOfObjectWriter fieldOfObjectWriter :
              fieldOfObjectWriters) {
            fieldOfObjectWriter.writeFieldOfObject(messageOrBuilder);
          }
          return true;
        }
        return false;
      }
    }

    FastMessageWriter generateFastMessageWriter(
        ProtoWriteSupport<?>.MessageWriter messageWriter,
        Queue<ProtoWriteSupport<?>.FieldWriter> furtherFieldWriters) {
      Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface =
          ReflectionUtil.getProto3MessageOrBuilderInterface(messageWriter.protoMessageClass)
              .get();

      FieldOfObjectWriter[] fieldOfObjectWriters = new FieldOfObjectWriter[messageWriter.fieldWriters.length];

      ProtoWriteSupport<?>.FieldWriter[] fieldWriters = messageWriter.fieldWriters;
      for (int i = 0; i < fieldWriters.length; i++) {
        ProtoWriteSupport<?>.FieldWriter fieldWriter = fieldWriters[i];
        FieldOfObjectWriter fieldOfObjectWriter =
            createFieldOfObjectWriter(proto3MessageOrBuilderInterface, fieldWriter);
        fieldOfObjectWriters[i] = fieldOfObjectWriter;
      }

      return new JavaReflectionProto3FastMessageWriter(proto3MessageOrBuilderInterface, fieldOfObjectWriters);
    }

    static FieldOfObjectWriter createFieldOfObjectWriter(
        Class<? extends MessageOrBuilder> owningProto3MessageOrBuilderInterface,
        ProtoWriteSupport<?>.FieldWriter fieldWriter) {
      FieldDescriptor fieldDescriptor = fieldWriter.fieldDescriptor;
      try {
        switch (fieldDescriptor.getJavaType()) {
          case INT:
            return fieldDescriptor.isRepeated()
                ? new IntListFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.ArrayWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface)
                : new IntFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.IntWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface);
          case LONG:
            return fieldDescriptor.isRepeated()
                ? new LongListFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.ArrayWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface)
                : new LongFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.LongWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface);
          case FLOAT:
            return fieldDescriptor.isRepeated()
                ? new FloatListFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.ArrayWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface)
                : new FloatFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.FloatWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface);
          case DOUBLE:
            return fieldDescriptor.isRepeated()
                ? new DoubleListFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.ArrayWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface)
                : new DoubleFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.DoubleWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface);
          case BOOLEAN:
            return fieldDescriptor.isRepeated()
                ? new BooleanListFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.ArrayWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface)
                : new BooleanFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.BooleanWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface);
          case STRING:
          case BYTE_STRING:
            return fieldDescriptor.isRepeated()
                ? new ObjectListFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.ArrayWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface)
                : new ObjectFieldOfObjectWriter(fieldWriter, owningProto3MessageOrBuilderInterface);
          case ENUM:
            return fieldDescriptor.isRepeated()
                ? new EnumListFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.ArrayWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface)
                : new EnumFieldOfObjectWriter(
                    (ProtoWriteSupport<?>.EnumWriter) fieldWriter,
                    owningProto3MessageOrBuilderInterface);
          case MESSAGE: // this also covers when message is written as bytes
            if (fieldDescriptor.isMapField()) {
              ProtoWriteSupport<?>.MapWriter mapWriter = (ProtoWriteSupport<?>.MapWriter) fieldWriter;
              if (mapWriter.valueWriter.fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                return new MapEnumFieldOfObjectWriter(mapWriter, owningProto3MessageOrBuilderInterface);
              } else {
                return new MapFieldOfObjectWriter(mapWriter, owningProto3MessageOrBuilderInterface);
              }
            } else {
              return fieldDescriptor.isRepeated()
                  ? new ObjectListFieldOfObjectWriter(
                      (ProtoWriteSupport<?>.ArrayWriter) fieldWriter,
                      owningProto3MessageOrBuilderInterface)
                  : new ObjectFieldOfObjectWriter(fieldWriter, owningProto3MessageOrBuilderInterface);
            }
        }
      } catch (Throwable t) {
        throw new FastMessageWriterCreationException(
            "was not able to initialize field-of-object writer for the field " + fieldDescriptor, t);
      }
      throw new FastMessageWriterCreationException("unsupported JavaType of the field " + fieldDescriptor);
    }

    interface FieldOfObjectWriter {
      void writeFieldOfObject(Object object);
    }

    interface FieldHasValue {
      boolean hasValue(Object object);
    }

    interface GetRepeatedFieldSize {
      int getSize(Object object);
    }

    interface ObjectValueGetter {
      Object getValue(Object object);
    }

    interface IntValueGetter {
      int getValue(Object object);
    }

    interface LongValueGetter {
      long getValue(Object object);
    }

    interface DoubleValueGetter {
      double getValue(Object object);
    }

    interface FloatValueGetter {
      float getValue(Object object);
    }

    interface BooleanValueGetter {
      boolean getValue(Object object);
    }

    interface ObjectListElementGetter {
      Object getElement(Object object, int pos);
    }

    interface IntListElementGetter {
      int getElement(Object object, int pos);
    }

    interface LongListElementGetter {
      long getElement(Object object, int pos);
    }

    interface DoubleListElementGetter {
      double getElement(Object object, int pos);
    }

    interface FloatListElementGetter {
      float getElement(Object object, int pos);
    }

    interface BooleanListElementGetter {
      boolean getElement(Object object, int pos);
    }

    static class ObjectFieldOfObjectWriter implements FieldOfObjectWriter {
      final ObjectValueGetter getValue;
      final FieldHasValue fieldHasValue;

      final ProtoWriteSupport<?>.FieldWriter fieldWriter;

      ObjectFieldOfObjectWriter(
          ProtoWriteSupport<?>.FieldWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldWriter = fieldWriter;
        this.fieldHasValue = getHasValueOrNull(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getValue = getGetObjectValue(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
          return;
        }
        fieldWriter.writeBeforeAll();
        fieldWriter.writeRawValue(getValue.getValue(object));
        fieldWriter.writeAfterAll();
      }
    }

    static class ObjectListFieldOfObjectWriter implements FieldOfObjectWriter {
      final GetRepeatedFieldSize getRepeatedFieldSize;
      final ObjectListElementGetter getListElement;

      final ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      final ProtoWriteSupport<?>.FieldWriter elementWriter;

      ObjectListFieldOfObjectWriter(
          ProtoWriteSupport<?>.ArrayWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.arrayWriter = fieldWriter;
        this.elementWriter = fieldWriter.fieldWriter;
        this.getRepeatedFieldSize =
            getRepeatedFieldSize(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getListElement =
            getGetObjectListElement(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        int size = getRepeatedFieldSize.getSize(object);
        if (size == 0) {
          return;
        }
        arrayWriter.writeBeforeAll();
        for (int i = 0; i < size; i++) {
          arrayWriter.writeBeforeElement();
          Object value = getListElement.getElement(object, i);
          elementWriter.writeRawValue(value);
          arrayWriter.writeAfterElement();
        }
        arrayWriter.writeAfterAll();
      }
    }

    static class IntFieldOfObjectWriter implements FieldOfObjectWriter {
      final IntValueGetter getValue;
      final FieldHasValue fieldHasValue;

      final ProtoWriteSupport<?>.IntWriter fieldWriter;

      IntFieldOfObjectWriter(
          ProtoWriteSupport<?>.IntWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldWriter = fieldWriter;
        this.fieldHasValue = getHasValueOrNull(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getValue = getGetIntValue(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
          return;
        }
        fieldWriter.writeBeforeAll();
        fieldWriter.writeRawValuePrimitive(getValue.getValue(object));
        fieldWriter.writeAfterAll();
      }
    }

    static class IntListFieldOfObjectWriter implements FieldOfObjectWriter {
      final GetRepeatedFieldSize getRepeatedFieldSize;
      final IntListElementGetter getListElement;

      final ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      final ProtoWriteSupport<?>.IntWriter elementWriter;

      IntListFieldOfObjectWriter(
          ProtoWriteSupport<?>.ArrayWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.arrayWriter = fieldWriter;
        this.elementWriter = (ProtoWriteSupport<?>.IntWriter) fieldWriter.fieldWriter;
        this.getRepeatedFieldSize =
            getRepeatedFieldSize(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getListElement =
            getGetIntListElement(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        int size = getRepeatedFieldSize.getSize(object);
        if (size == 0) {
          return;
        }
        arrayWriter.writeBeforeAll();
        for (int i = 0; i < size; i++) {
          arrayWriter.writeBeforeElement();
          int value = getListElement.getElement(object, i);
          elementWriter.writeRawValuePrimitive(value);
          arrayWriter.writeAfterElement();
        }
        arrayWriter.writeAfterAll();
      }
    }

    static class LongFieldOfObjectWriter implements FieldOfObjectWriter {
      final LongValueGetter getValue;
      final FieldHasValue fieldHasValue;

      final ProtoWriteSupport<?>.LongWriter fieldWriter;

      LongFieldOfObjectWriter(
          ProtoWriteSupport<?>.LongWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldWriter = fieldWriter;
        this.fieldHasValue = getHasValueOrNull(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getValue = getGetLongValue(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
          return;
        }
        fieldWriter.writeBeforeAll();
        fieldWriter.writeRawValuePrimitive(getValue.getValue(object));
        fieldWriter.writeAfterAll();
      }
    }

    static class LongListFieldOfObjectWriter implements FieldOfObjectWriter {
      final GetRepeatedFieldSize getRepeatedFieldSize;
      final LongListElementGetter getListElement;

      final ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      final ProtoWriteSupport<?>.LongWriter elementWriter;

      LongListFieldOfObjectWriter(
          ProtoWriteSupport<?>.ArrayWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.arrayWriter = fieldWriter;
        this.elementWriter = (ProtoWriteSupport<?>.LongWriter) fieldWriter.fieldWriter;
        this.getRepeatedFieldSize =
            getRepeatedFieldSize(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getListElement =
            getGetLongListElement(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        int size = getRepeatedFieldSize.getSize(object);
        if (size == 0) {
          return;
        }
        arrayWriter.writeBeforeAll();
        for (int i = 0; i < size; i++) {
          arrayWriter.writeBeforeElement();
          long value = getListElement.getElement(object, i);
          elementWriter.writeRawValuePrimitive(value);
          arrayWriter.writeAfterElement();
        }
        arrayWriter.writeAfterAll();
      }
    }

    static class MapFieldOfObjectWriter implements FieldOfObjectWriter {
      final GetRepeatedFieldSize getRepeatedFieldSize;
      final ObjectValueGetter getMap;

      ProtoWriteSupport<?>.MapWriter mapWriter;
      ProtoWriteSupport<?>.FieldWriter keyWriter;
      ProtoWriteSupport<?>.FieldWriter valueWriter;

      MapFieldOfObjectWriter(
          ProtoWriteSupport<?>.MapWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.mapWriter = fieldWriter;
        this.keyWriter = fieldWriter.keyWriter;
        this.valueWriter = (ProtoWriteSupport<?>.FieldWriter) fieldWriter.valueWriter;
        this.getRepeatedFieldSize =
            getRepeatedFieldSize(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getMap = getGetObjectValue(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        int size = getRepeatedFieldSize.getSize(object);
        if (size == 0) {
          return;
        }
        mapWriter.writeBeforeAll();
        Map map = (Map) getMap.getValue(object);
        map.forEach((k, v) -> {
          mapWriter.writeBeforeElement();
          keyWriter.writeField(k);
          valueWriter.writeField(v);
          mapWriter.writeAfterElement();
        });
        mapWriter.writeAfterAll();
      }
    }

    static class FloatFieldOfObjectWriter implements FieldOfObjectWriter {
      final FloatValueGetter getValue;
      final FieldHasValue fieldHasValue;

      final ProtoWriteSupport<?>.FloatWriter fieldWriter;

      FloatFieldOfObjectWriter(
          ProtoWriteSupport<?>.FloatWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldWriter = fieldWriter;
        this.fieldHasValue = getHasValueOrNull(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getValue = getGetFloatValue(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
          return;
        }
        fieldWriter.writeBeforeAll();
        fieldWriter.writeRawValuePrimitive(getValue.getValue(object));
        fieldWriter.writeAfterAll();
      }
    }

    static class FloatListFieldOfObjectWriter implements FieldOfObjectWriter {
      final GetRepeatedFieldSize getRepeatedFieldSize;
      final FloatListElementGetter getListElement;

      final ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      final ProtoWriteSupport<?>.FloatWriter elementWriter;

      FloatListFieldOfObjectWriter(
          ProtoWriteSupport<?>.ArrayWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.arrayWriter = fieldWriter;
        this.elementWriter = (ProtoWriteSupport<?>.FloatWriter) fieldWriter.fieldWriter;
        this.getRepeatedFieldSize =
            getRepeatedFieldSize(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getListElement =
            getGetFloatListElement(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        int size = getRepeatedFieldSize.getSize(object);
        if (size == 0) {
          return;
        }
        arrayWriter.writeBeforeAll();
        for (int i = 0; i < size; i++) {
          arrayWriter.writeBeforeElement();
          float value = getListElement.getElement(object, i);
          elementWriter.writeRawValuePrimitive(value);
          arrayWriter.writeAfterElement();
        }
        arrayWriter.writeAfterAll();
      }
    }

    static class DoubleFieldOfObjectWriter implements FieldOfObjectWriter {
      final DoubleValueGetter getValue;
      final FieldHasValue fieldHasValue;

      final ProtoWriteSupport<?>.DoubleWriter fieldWriter;

      DoubleFieldOfObjectWriter(
          ProtoWriteSupport<?>.DoubleWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldWriter = fieldWriter;
        this.fieldHasValue = getHasValueOrNull(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getValue = getGetDoubleValue(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
          return;
        }
        fieldWriter.writeBeforeAll();
        fieldWriter.writeRawValuePrimitive(getValue.getValue(object));
        fieldWriter.writeAfterAll();
      }
    }

    static class DoubleListFieldOfObjectWriter implements FieldOfObjectWriter {
      final GetRepeatedFieldSize getRepeatedFieldSize;
      final DoubleListElementGetter getListElement;

      final ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      final ProtoWriteSupport<?>.DoubleWriter elementWriter;

      DoubleListFieldOfObjectWriter(
          ProtoWriteSupport<?>.ArrayWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.arrayWriter = fieldWriter;
        this.elementWriter = (ProtoWriteSupport<?>.DoubleWriter) fieldWriter.fieldWriter;
        this.getRepeatedFieldSize =
            getRepeatedFieldSize(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getListElement =
            getGetDoubleListElement(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        int size = getRepeatedFieldSize.getSize(object);
        if (size == 0) {
          return;
        }
        arrayWriter.writeBeforeAll();
        for (int i = 0; i < size; i++) {
          arrayWriter.writeBeforeElement();
          double value = getListElement.getElement(object, i);
          elementWriter.writeRawValuePrimitive(value);
          arrayWriter.writeAfterElement();
        }
        arrayWriter.writeAfterAll();
      }
    }

    static class EnumFieldOfObjectWriter implements FieldOfObjectWriter {
      final IntValueGetter getEnumValue;
      final FieldHasValue fieldHasValue;
      final Descriptors.EnumDescriptor enumDescriptor;
      final List<Descriptors.EnumValueDescriptor> enumValues;
      final IntFunction<? extends ProtocolMessageEnum> forNumber;

      final ProtoWriteSupport<?>.EnumWriter fieldWriter;

      EnumFieldOfObjectWriter(
          ProtoWriteSupport<?>.EnumWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldWriter = fieldWriter;
        this.fieldHasValue = getHasValueOrNull(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getEnumValue = getGetIntValue(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.enumDescriptor = fieldWriter.fieldDescriptor.getEnumType();
        this.enumValues = enumDescriptor.getValues();
        this.forNumber =
            getEnumForNumber((Class<? extends ProtocolMessageEnum>) fieldWriter.getFieldReflectionType());
      }

      @Override
      public void writeFieldOfObject(Object object) {
        if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
          return;
        }
        fieldWriter.writeBeforeAll();

        int enumNumber = getEnumValue.getValue(object);
        ProtocolMessageEnum enum_ = forNumber.apply(enumNumber);
        Enum<?> javaEnum = (Enum<?>) enum_;
        Descriptors.EnumValueDescriptor enumValueDescriptor;
        if (javaEnum != null) {
          enumValueDescriptor = enumValues.get(javaEnum.ordinal());
        } else {
          enumValueDescriptor = enumDescriptor.findValueByNumberCreatingIfUnknown(enumNumber);
        }
        fieldWriter.writeRawValue(enumValueDescriptor);

        fieldWriter.writeAfterAll();
      }
    }

    static class EnumListFieldOfObjectWriter implements FieldOfObjectWriter {
      final GetRepeatedFieldSize getRepeatedFieldSize;
      final IntListElementGetter getEnumValueListElement;
      final Descriptors.EnumDescriptor enumDescriptor;
      final List<Descriptors.EnumValueDescriptor> enumValues;
      final IntFunction<? extends ProtocolMessageEnum> forNumber;

      final ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      final ProtoWriteSupport<?>.EnumWriter elementWriter;

      EnumListFieldOfObjectWriter(
          ProtoWriteSupport<?>.ArrayWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.arrayWriter = fieldWriter;
        this.elementWriter = (ProtoWriteSupport<?>.EnumWriter) fieldWriter.fieldWriter;
        this.getRepeatedFieldSize =
            getRepeatedFieldSize(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getEnumValueListElement =
            getGetIntListElement(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.enumDescriptor = fieldWriter.fieldDescriptor.getEnumType();
        this.enumValues = enumDescriptor.getValues();
        this.forNumber =
            getEnumForNumber((Class<? extends ProtocolMessageEnum>) elementWriter.getFieldReflectionType());
      }

      @Override
      public void writeFieldOfObject(Object object) {
        int size = getRepeatedFieldSize.getSize(object);
        if (size == 0) {
          return;
        }
        arrayWriter.writeBeforeAll();
        for (int i = 0; i < size; i++) {
          arrayWriter.writeBeforeElement();
          int enumNumber = getEnumValueListElement.getElement(object, i);
          ProtocolMessageEnum enum_ = forNumber.apply(enumNumber);
          Enum<?> javaEnum = (Enum<?>) enum_;
          Descriptors.EnumValueDescriptor enumValueDescriptor;
          if (javaEnum != null) {
            enumValueDescriptor = enumValues.get(javaEnum.ordinal());
          } else {
            enumValueDescriptor = enumDescriptor.findValueByNumberCreatingIfUnknown(enumNumber);
          }
          elementWriter.writeRawValue(enumValueDescriptor);
          arrayWriter.writeAfterElement();
        }
        arrayWriter.writeAfterAll();
      }
    }

    static class MapEnumFieldOfObjectWriter implements FieldOfObjectWriter {
      final GetRepeatedFieldSize getRepeatedFieldSize;
      final ObjectValueGetter getMap;
      final Descriptors.EnumDescriptor enumDescriptor;
      final List<Descriptors.EnumValueDescriptor> enumValues;
      final IntFunction<? extends ProtocolMessageEnum> forNumber;

      final ProtoWriteSupport<?>.MapWriter mapWriter;
      final ProtoWriteSupport<?>.FieldWriter keyWriter;
      final ProtoWriteSupport<?>.EnumWriter valueWriter;

      MapEnumFieldOfObjectWriter(
          ProtoWriteSupport<?>.MapWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.mapWriter = fieldWriter;
        this.keyWriter = fieldWriter.keyWriter;
        this.valueWriter = (ProtoWriteSupport<?>.EnumWriter) fieldWriter.valueWriter;
        this.getRepeatedFieldSize =
            getRepeatedFieldSize(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getMap = getGetObjectValue(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.enumDescriptor = valueWriter.fieldDescriptor.getEnumType();
        this.enumValues = enumDescriptor.getValues();
        this.forNumber =
            getEnumForNumber((Class<? extends ProtocolMessageEnum>) valueWriter.getFieldReflectionType());
      }

      @Override
      public void writeFieldOfObject(Object object) {
        int size = getRepeatedFieldSize.getSize(object);
        if (size == 0) {
          return;
        }
        mapWriter.writeBeforeAll();
        Map<?, Integer> map = (Map) getMap.getValue(object);
        map.forEach((k, v) -> {
          mapWriter.writeBeforeElement();
          keyWriter.writeField(k);
          ProtocolMessageEnum enum_ = forNumber.apply(v);
          Enum<?> javaEnum = (Enum<?>) enum_;
          Descriptors.EnumValueDescriptor enumValueDescriptor;
          if (javaEnum != null) {
            enumValueDescriptor = enumValues.get(javaEnum.ordinal());
          } else {
            enumValueDescriptor = enumDescriptor.findValueByNumberCreatingIfUnknown(v);
          }
          valueWriter.writeField(enumValueDescriptor);
          mapWriter.writeAfterElement();
        });
        mapWriter.writeAfterAll();
      }
    }

    static class BooleanFieldOfObjectWriter implements FieldOfObjectWriter {
      final BooleanValueGetter getValue;
      final FieldHasValue fieldHasValue;

      final ProtoWriteSupport<?>.BooleanWriter fieldWriter;

      BooleanFieldOfObjectWriter(
          ProtoWriteSupport<?>.BooleanWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldWriter = fieldWriter;
        this.fieldHasValue = getHasValueOrNull(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getValue = getGetBooleanValue(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
          return;
        }
        fieldWriter.writeBeforeAll();
        fieldWriter.writeRawValuePrimitive(getValue.getValue(object));
        fieldWriter.writeAfterAll();
      }
    }

    static class BooleanListFieldOfObjectWriter implements FieldOfObjectWriter {
      final GetRepeatedFieldSize getRepeatedFieldSize;
      final BooleanListElementGetter getListElement;

      final ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      final ProtoWriteSupport<?>.BooleanWriter elementWriter;

      BooleanListFieldOfObjectWriter(
          ProtoWriteSupport<?>.ArrayWriter fieldWriter,
          Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.arrayWriter = fieldWriter;
        this.elementWriter = (ProtoWriteSupport<?>.BooleanWriter) fieldWriter.fieldWriter;
        this.getRepeatedFieldSize =
            getRepeatedFieldSize(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getListElement =
            getGetBooleanListElement(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void writeFieldOfObject(Object object) {
        int size = getRepeatedFieldSize.getSize(object);
        if (size == 0) {
          return;
        }
        arrayWriter.writeBeforeAll();
        for (int i = 0; i < size; i++) {
          arrayWriter.writeBeforeElement();
          boolean value = getListElement.getElement(object, i);
          elementWriter.writeRawValuePrimitive(value);
          arrayWriter.writeAfterElement();
        }
        arrayWriter.writeAfterAll();
      }
    }
  }

  public static class FastMessageWriterCreationException extends RuntimeException {
    public FastMessageWriterCreationException(String message, Throwable cause) {
      super(message, cause);
    }

    public FastMessageWriterCreationException(String message) {
      super(message);
    }

    public FastMessageWriterCreationException(Throwable cause) {
      super(cause);
    }
  }
}
