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
import java.lang.reflect.ParameterizedType;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import net.bytebuddy.implementation.bytecode.constant.TextConstant;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
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
        FieldWriter writer = createWriter(
            fieldDescriptor, type, ReflectionUtil.getFieldMessageType(fieldDescriptor, protoMessageClass));

        if (fieldDescriptor.isRepeated() && !fieldDescriptor.isMapField()) {
          writer = new ArrayWriter(writer);
        }

        writer.setFieldName(name);
        writer.setIndex(schema.getFieldIndex(name));
        writer.setFieldDescriptor(fieldDescriptor);

        fieldWriters[fieldDescriptor.getIndex()] = writer;
      }
    }

    private FieldWriter createWriter(
        FieldDescriptor fieldDescriptor, Type type, Class<? extends Message> fieldMessageType) {

      switch (fieldDescriptor.getJavaType()) {
        case STRING:
          return new StringWriter();
        case MESSAGE:
          return createMessageWriter(fieldDescriptor, type, fieldMessageType);
        case INT:
          return new IntWriter();
        case LONG:
          return new LongWriter();
        case FLOAT:
          return new FloatWriter();
        case DOUBLE:
          return new DoubleWriter();
        case ENUM:
          return new EnumWriter(fieldDescriptor.getEnumType());
        case BOOLEAN:
          return new BooleanWriter();
        case BYTE_STRING:
          return new BinaryWriter();
      }

      return unknownType(fieldDescriptor); // should not be executed, always throws exception.
    }

    private FieldWriter createMessageWriter(
        FieldDescriptor fieldDescriptor, Type type, Class<? extends Message> fieldMessageType) {
      if (fieldDescriptor.isMapField()) {
        return createMapWriter(fieldDescriptor, type, fieldMessageType);
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
        return new BinaryWriter();
      }

      return new MessageWriter(fieldDescriptor.getMessageType(), getGroupType(type), fieldMessageType);
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
        FieldDescriptor fieldDescriptor, Type type, Class<? extends Message> valueProto3MessageClass) {
      List<FieldDescriptor> fields = fieldDescriptor.getMessageType().getFields();
      if (fields.size() != 2) {
        throw new UnsupportedOperationException(
            "Expected two fields for the map (key/value), but got: " + fields);
      }

      // KeyFieldWriter
      FieldDescriptor keyProtoField = fields.get(0);
      FieldWriter keyWriter = createWriter(keyProtoField, type, null);
      keyWriter.setFieldName(keyProtoField.getName());
      keyWriter.setIndex(0);

      // ValueFieldWriter
      FieldDescriptor valueProtoField = fields.get(1);
      FieldWriter valueWriter = createWriter(
          valueProtoField,
          writeSpecsCompliant ? type : type.asGroupType().getType("value"),
          valueProto3MessageClass);
      valueWriter.setFieldName(valueProtoField.getName());
      valueWriter.setIndex(1);

      return new MapWriter(keyWriter, valueWriter);
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

    ArrayWriter(FieldWriter fieldWriter) {
      this.fieldWriter = fieldWriter;
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
  }

  class IntWriter extends FieldWriter {
    final void writeRawValuePrimitive(int value) {
      recordConsumer.addInteger(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((int) value);
    }
  }

  class LongWriter extends FieldWriter {

    final void writeRawValuePrimitive(long value) {
      recordConsumer.addLong(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((long) value);
    }
  }

  class MapWriter extends RepeatedFieldWriter {

    private final FieldWriter keyWriter;
    private final FieldWriter valueWriter;

    public MapWriter(FieldWriter keyWriter, FieldWriter valueWriter) {
      super();
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
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

  class FloatWriter extends FieldWriter {

    final void writeRawValuePrimitive(float value) {
      recordConsumer.addFloat(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((float) value);
    }
  }

  class DoubleWriter extends FieldWriter {
    final void writeRawValuePrimitive(double value) {
      recordConsumer.addDouble(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((double) value);
    }
  }

  class EnumWriter extends FieldWriter {
    Map<String, Integer> enumNameNumberPairs;

    public EnumWriter(Descriptors.EnumDescriptor enumType) {
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
  }

  class BooleanWriter extends FieldWriter {

    final void writeRawValuePrimitive(boolean value) {
      recordConsumer.addBoolean(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((boolean) value);
    }
  }

  class BinaryWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      // Non-ByteString values can happen when recursions gets truncated.
      ByteString byteString = value instanceof ByteString
          ? (ByteString) value
          // TODO: figure out a way to use MessageOrBuilder
          : value instanceof Message
              ? ((Message) value).toByteString()
              // Worst-case, just dump as plain java string.
              : ByteString.copyFromUtf8(value.toString());
      Binary binary = Binary.fromConstantByteArray(byteString.toByteArray());
      recordConsumer.addBinary(binary);
    }
  }

  class TimestampWriter extends FieldWriter {
    @Override
    void writeRawValue(Object value) {
      Timestamp timestamp = (Timestamp) value;
      recordConsumer.addLong(Timestamps.toNanos(timestamp));
    }
  }

  class DateWriter extends FieldWriter {
    @Override
    void writeRawValue(Object value) {
      Date date = (Date) value;
      LocalDate localDate = LocalDate.of(date.getYear(), date.getMonth(), date.getDay());
      recordConsumer.addInteger((int) localDate.toEpochDay());
    }
  }

  class TimeWriter extends FieldWriter {
    @Override
    void writeRawValue(Object value) {
      TimeOfDay timeOfDay = (TimeOfDay) value;
      LocalTime localTime = LocalTime.of(
          timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds(), timeOfDay.getNanos());
      recordConsumer.addLong(localTime.toNanoOfDay());
    }
  }

  class DoubleValueWriter extends FieldWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addDouble(((DoubleValue) value).getValue());
    }
  }

  class FloatValueWriter extends FieldWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addFloat(((FloatValue) value).getValue());
    }
  }

  class Int64ValueWriter extends FieldWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addLong(((Int64Value) value).getValue());
    }
  }

  class UInt64ValueWriter extends FieldWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addLong(((UInt64Value) value).getValue());
    }
  }

  class Int32ValueWriter extends FieldWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addInteger(((Int32Value) value).getValue());
    }
  }

  class UInt32ValueWriter extends FieldWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addLong(((UInt32Value) value).getValue());
    }
  }

  class BoolValueWriter extends FieldWriter {
    @Override
    void writeRawValue(Object value) {
      recordConsumer.addBoolean(((BoolValue) value).getValue());
    }
  }

  class StringValueWriter extends FieldWriter {
    @Override
    void writeRawValue(Object value) {
      Binary binaryString = Binary.fromString(((StringValue) value).getValue());
      recordConsumer.addBinary(binaryString);
    }
  }

  class BytesValueWriter extends FieldWriter {
    @Override
    void writeRawValue(Object value) {
      byte[] byteArray = ((BytesValue) value).getValue().toByteArray();
      Binary binary = Binary.fromConstantByteArray(byteArray);
      recordConsumer.addBinary(binary);
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

    private ResolvedReflection() {}
  }

  static class ReflectionUtil {

    static Optional<? extends Class<? extends MessageOrBuilder>> getProto3MessageOrBuilderInterface(
        Class<? extends Message> messageClass) {
      return Stream.of(messageClass)
          .filter(Objects::nonNull)
          .filter(GeneratedMessageV3.class::isAssignableFrom)
          .filter(x -> Protobufs.getMessageDescriptor(x).getFile().getSyntax()
              == Descriptors.FileDescriptor.Syntax.PROTO3)
          .flatMap(x -> Arrays.stream(x.getInterfaces()))
          .filter(MessageOrBuilder.class::isAssignableFrom)
          .map(x -> (Class<? extends MessageOrBuilder>) x)
          .findFirst();
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

    static Class<? extends Message> getFieldMessageType(
        FieldDescriptor fieldDescriptor, Class<? extends Message> protoMessageClass) {
      if (protoMessageClass == null) {
        return null;
      }
      if (fieldDescriptor.isMapField()) {
        try {
          Method getter = protoMessageClass.getMethod("get" + getFieldNameForMethod(fieldDescriptor) + "Map");
          ParameterizedType mapKV = (ParameterizedType) getter.getGenericReturnType();
          Class<? extends Message> valueClass =
              (Class<? extends Message>) mapKV.getActualTypeArguments()[1]; // value
          return valueClass;
        } catch (Exception e) {
          throw new UnsupportedOperationException(
              "could not find a getter method for a map field: " + fieldDescriptor, e);
        }
      } else if (fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
        if (fieldDescriptor.isRepeated()) {
          try {
            Method getter =
                protoMessageClass.getMethod("get" + getFieldNameForMethod(fieldDescriptor), int.class);
            Class<? extends Message> elementClass =
                (Class<? extends Message>) getter.getReturnType(); // element
            return elementClass;
          } catch (Exception e) {
            throw new UnsupportedOperationException(
                "could not find a getter method for a repeated field: " + fieldDescriptor, e);
          }
        } else {
          try {
            Method getter = protoMessageClass.getMethod("get" + getFieldNameForMethod(fieldDescriptor));
            Class<? extends Message> fieldValueClass = (Class<? extends Message>) getter.getReturnType();
            return fieldValueClass;
          } catch (Exception e) {
            throw new UnsupportedOperationException(
                "could not find a getter method for a non-repeated field: " + fieldDescriptor, e);
          }
        }
      } else {
        return null;
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

      ByteBuddyProto3FastMessageWriter(ProtoWriteSupport<?> protoWriteSupport) {
        this.protoWriteSupport = protoWriteSupport;
      }

      /**
       * A helper used by ByteBuddy generated bytecode to get access to {@link #recordConsumer}
       * @return recordConsumer
       */
      RecordConsumer getRecordConsumer() {
        return protoWriteSupport.recordConsumer;
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
      DynamicType.Builder<ByteBuddyProto3FastMessageWriter> classBuilder =
          new ByteBuddy().subclass(ByteBuddyProto3FastMessageWriter.class);
      Queue<ProtoWriteSupport<?>.MessageWriter> immediateMessageWriters = new LinkedList<>();
      DynamicType.Builder.MethodDefinition.ReceiverTypeDefinition<ByteBuddyProto3FastMessageWriter>
          writeAllFields = classBuilder
              .method(ElementMatchers.named("writeAllFields"))
              .intercept(generateWriteAllFieldsMethod(
                  messageWriter, immediateMessageWriters, furtherFieldWriters));
      while (!immediateMessageWriters.isEmpty()) {
        ProtoWriteSupport<?>.MessageWriter mw = immediateMessageWriters.poll();
        Class<? extends MessageOrBuilder> paramType = ReflectionUtil.getProto3MessageOrBuilderInterface(
                messageWriter.protoMessageClass)
            .get();
        writeAllFields = writeAllFields
            .defineMethod(
                "writeAllFields" + ReflectionUtil.getFieldNameForMethod(mw.fieldDescriptor),
                boolean.class)
            .withParameters(paramType)
            .intercept(generateWriteAllFieldsMethod(mw, immediateMessageWriters, furtherFieldWriters));
      }

      DynamicType.Unloaded<ByteBuddyProto3FastMessageWriter> unloaded = writeAllFields
          .name(ProtoWriteSupport.class.getName() + "$bb" + BYTE_BUDDY_CLASS_SEQUENCE.incrementAndGet())
          .make();

      return ReflectionUtil.newInstance(
          ReflectionUtil.getConstructor(
              unloaded.load(null, ClassLoadingStrategy.UsingLookup.of(MethodHandles.lookup()))
                  .getLoaded(),
              ProtoWriteSupport.class),
          protoWriteSupport);
    }

    @Override
    void assertOptimizationAvailable() {
      if (!isByteBuddyAvailable()) {
        throw new FastMessageWriterCreationException("ByteBuddy optimization for proto3 is not available");
      }
    }

    static final Implementation NOOP = new Implementation.Simple(
        (methodVisitor, implementationContext, instrumentedMethod) -> new ByteCodeAppender.Size(0, 0));

    Implementation generateWriteAllFieldsMethod(
        ProtoWriteSupport<?>.MessageWriter messageWriter,
        Queue<ProtoWriteSupport<?>.MessageWriter> immediateMessageWriters,
        Queue<ProtoWriteSupport<?>.FieldWriter> furtherMessageWriters) {
      List<Implementation> steps = new ArrayList<>();
      steps.add(new Implementation.Simple(new ByteCodeAppender() {
        @Override
        public Size apply(
            MethodVisitor methodVisitor,
            Implementation.Context implementationContext,
            MethodDescription instrumentedMethod) {
          try {
            return new Size(
                new StackManipulation.Compound(
                        MethodVariableAccess.loadThis(),
                        MethodInvocation.invoke(new MethodDescription.ForLoadedMethod(
                            Reflection.ByteBuddyProto3FastMessageWriter.getRecordConsumer)),
                        MethodVariableAccess.REFERENCE.storeAt(2))
                    .apply(methodVisitor, implementationContext)
                    .getMaximalSize(),
                instrumentedMethod.getStackSize() + 1);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }));
      Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface =
          ReflectionUtil.getProto3MessageOrBuilderInterface(messageWriter.protoMessageClass)
              .get();
      for (ProtoWriteSupport<?>.FieldWriter fieldWriter : messageWriter.fieldWriters) {
        if (fieldWriter instanceof ProtoWriteSupport.LongWriter && !fieldWriter.fieldDescriptor.hasPresence()) {
          steps.add(new Implementation.Simple(new ByteCodeAppender() {
            @Override
            public Size apply(
                MethodVisitor methodVisitor,
                Implementation.Context implementationContext,
                MethodDescription instrumentedMethod) {
              try {
                return new Size(
                    new StackManipulation.Compound(
                            MethodVariableAccess.REFERENCE.loadFrom(2),
                            new TextConstant(fieldWriter.fieldName),
                            IntegerConstant.forValue(fieldWriter.index),
                            MethodInvocation.invoke(new MethodDescription.ForLoadedMethod(
                                Reflection.RecordConsumer.startField)),
                            MethodVariableAccess.REFERENCE.loadFrom(2),
                            MethodVariableAccess.REFERENCE.loadFrom(1),
                            MethodInvocation.invoke(new MethodDescription.ForLoadedMethod(
                                JavaReflectionProto3FastMessageWriters
                                    .getGetLongValueMethod(
                                        fieldWriter.fieldDescriptor,
                                        proto3MessageOrBuilderInterface))),
                            MethodInvocation.invoke(new MethodDescription.ForLoadedMethod(
                                Reflection.RecordConsumer.addLong)),
                            MethodVariableAccess.REFERENCE.loadFrom(2),
                            new TextConstant(fieldWriter.fieldName),
                            IntegerConstant.forValue(fieldWriter.index),
                            MethodInvocation.invoke(new MethodDescription.ForLoadedMethod(
                                Reflection.RecordConsumer.endField)))
                        .apply(methodVisitor, implementationContext)
                        .getMaximalSize(),
                    instrumentedMethod.getStackSize());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          }));
          continue;
        }

        JavaReflectionProto3FastMessageWriters.FieldOfObjectWriter fieldOfObjectWriter =
            JavaReflectionProto3FastMessageWriters.createFieldOfObjectWriter(
                fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        fieldOfObjectWriter.setFieldWriter(fieldWriter);

        steps.add(MethodCall.invoke(Reflection.FieldOfObjectWriter.writeFieldOfObject)
            .on(fieldOfObjectWriter)
            .withAllArguments()
            .andThen(NOOP));

        if (fieldWriter instanceof ProtoWriteSupport.MessageWriter) {
          furtherMessageWriters.add(fieldWriter);
        } else if (fieldWriter instanceof ProtoWriteSupport.MapWriter) {
          furtherMessageWriters.add(fieldWriter);
        } else if (fieldWriter instanceof ProtoWriteSupport.ArrayWriter) {
          furtherMessageWriters.add(fieldWriter);
        }
      }
      steps.add(FixedValue.value(true));

      return new Implementation.Compound(steps);
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
          ? "Map"
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
            createFieldOfObjectWriter(fieldWriter.fieldDescriptor, proto3MessageOrBuilderInterface);
        fieldOfObjectWriter.setFieldWriter(fieldWriter);
        fieldOfObjectWriters[i] = fieldOfObjectWriter;
      }

      return new JavaReflectionProto3FastMessageWriter(proto3MessageOrBuilderInterface, fieldOfObjectWriters);
    }

    static FieldOfObjectWriter createFieldOfObjectWriter(
        FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface) {
      try {
        switch (fieldDescriptor.getJavaType()) {
          case INT:
            return fieldDescriptor.isRepeated()
                ? new IntListFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface)
                : new IntFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface);
          case LONG:
            return fieldDescriptor.isRepeated()
                ? new LongListFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface)
                : new LongFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface);
          case FLOAT:
            return fieldDescriptor.isRepeated()
                ? new FloatListFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface)
                : new FloatFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface);
          case DOUBLE:
            return fieldDescriptor.isRepeated()
                ? new DoubleListFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface)
                : new DoubleFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface);
          case BOOLEAN:
            return fieldDescriptor.isRepeated()
                ? new BooleanListFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface)
                : new BooleanFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface);
          case STRING:
          case BYTE_STRING:
            return fieldDescriptor.isRepeated()
                ? new ObjectListFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface)
                : new ObjectFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface);
          case ENUM:
            return fieldDescriptor.isRepeated()
                ? new EnumListFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface)
                : new EnumFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface);
          case MESSAGE:
            if (fieldDescriptor.isMapField()) {
              return new MapFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface);
            } else {
              return fieldDescriptor.isRepeated()
                  ? new ObjectListFieldOfObjectWriter(
                      fieldDescriptor, proto3MessageOrBuilderInterface)
                  : new ObjectFieldOfObjectWriter(fieldDescriptor, proto3MessageOrBuilderInterface);
            }
        }
      } catch (Throwable t) {
        throw new FastMessageWriterCreationException(
            "was not able to initialize field-of-object writer for the field " + fieldDescriptor, t);
      }
      throw new FastMessageWriterCreationException("unsupported JavaType of the field " + fieldDescriptor);
    }

    interface FieldOfObjectWriter {
      void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter);

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

      ProtoWriteSupport<?>.FieldWriter fieldWriter;

      ObjectFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldHasValue = getHasValueOrNull(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getValue = getGetObjectValue(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.fieldWriter = fieldWriter;
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

      ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      ProtoWriteSupport<?>.FieldWriter elementWriter;

      ObjectListFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.getRepeatedFieldSize = getRepeatedFieldSize(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getListElement = getGetObjectListElement(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.arrayWriter = (ProtoWriteSupport<?>.ArrayWriter) fieldWriter;
        this.elementWriter = arrayWriter.fieldWriter;
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

      ProtoWriteSupport<?>.IntWriter fieldWriter;

      IntFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldHasValue = getHasValueOrNull(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getValue = getGetIntValue(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.fieldWriter = (ProtoWriteSupport<?>.IntWriter) fieldWriter;
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

      ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      ProtoWriteSupport<?>.IntWriter elementWriter;

      IntListFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.getRepeatedFieldSize = getRepeatedFieldSize(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getListElement = getGetIntListElement(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.arrayWriter = (ProtoWriteSupport<?>.ArrayWriter) fieldWriter;
        this.elementWriter = (ProtoWriteSupport<?>.IntWriter) arrayWriter.fieldWriter;
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

      ProtoWriteSupport<?>.LongWriter fieldWriter;

      LongFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldHasValue = getHasValueOrNull(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getValue = getGetLongValue(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.fieldWriter = (ProtoWriteSupport<?>.LongWriter) fieldWriter;
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

      ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      ProtoWriteSupport<?>.LongWriter elementWriter;

      LongListFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.getRepeatedFieldSize = getRepeatedFieldSize(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getListElement = getGetLongListElement(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.arrayWriter = (ProtoWriteSupport<?>.ArrayWriter) fieldWriter;
        this.elementWriter = (ProtoWriteSupport<?>.LongWriter) arrayWriter.fieldWriter;
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
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.getRepeatedFieldSize = getRepeatedFieldSize(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getMap = getGetObjectValue(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.mapWriter = (ProtoWriteSupport<?>.MapWriter) fieldWriter;
        this.keyWriter = mapWriter.keyWriter;
        this.valueWriter = mapWriter.valueWriter;
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

      ProtoWriteSupport<?>.FloatWriter fieldWriter;

      FloatFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldHasValue = getHasValueOrNull(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getValue = getGetFloatValue(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.fieldWriter = (ProtoWriteSupport<?>.FloatWriter) fieldWriter;
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

      ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      ProtoWriteSupport<?>.FloatWriter elementWriter;

      FloatListFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.getRepeatedFieldSize = getRepeatedFieldSize(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getListElement = getGetFloatListElement(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.arrayWriter = (ProtoWriteSupport<?>.ArrayWriter) fieldWriter;
        this.elementWriter = (ProtoWriteSupport<?>.FloatWriter) arrayWriter.fieldWriter;
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

      ProtoWriteSupport<?>.DoubleWriter fieldWriter;

      DoubleFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldHasValue = getHasValueOrNull(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getValue = getGetDoubleValue(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.fieldWriter = (ProtoWriteSupport<?>.DoubleWriter) fieldWriter;
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

      ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      ProtoWriteSupport<?>.DoubleWriter elementWriter;

      DoubleListFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.getRepeatedFieldSize = getRepeatedFieldSize(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getListElement = getGetDoubleListElement(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.arrayWriter = (ProtoWriteSupport<?>.ArrayWriter) fieldWriter;
        this.elementWriter = (ProtoWriteSupport<?>.DoubleWriter) arrayWriter.fieldWriter;
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
      final ObjectValueGetter getEnum;
      final IntValueGetter getEnumValue;
      final FieldHasValue fieldHasValue;
      final Descriptors.EnumDescriptor enumDescriptor;
      final List<Descriptors.EnumValueDescriptor> enumValues;

      ProtoWriteSupport<?>.EnumWriter fieldWriter;

      EnumFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldHasValue = getHasValueOrNull(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getEnum = getGetObjectValue(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getEnumValue = getGetIntValue(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.enumDescriptor = fieldDescriptor.getEnumType();
        this.enumValues = enumDescriptor.getValues();
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.fieldWriter = (ProtoWriteSupport<?>.EnumWriter) fieldWriter;
      }

      @Override
      public void writeFieldOfObject(Object object) {
        if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
          return;
        }
        fieldWriter.writeBeforeAll();

        ProtocolMessageEnum enum_ = (ProtocolMessageEnum) getEnum.getValue(object);
        Enum<?> javaEnum = (Enum<?>) enum_;
        Descriptors.EnumValueDescriptor enumValueDescriptor;
        if (javaEnum.ordinal() < enumValues.size()) {
          enumValueDescriptor = enumValues.get(javaEnum.ordinal());
        } else {
          enumValueDescriptor =
              enumDescriptor.findValueByNumberCreatingIfUnknown(getEnumValue.getValue(object));
        }
        fieldWriter.writeRawValue(enumValueDescriptor);

        fieldWriter.writeAfterAll();
      }
    }

    static class EnumListFieldOfObjectWriter implements FieldOfObjectWriter {
      final GetRepeatedFieldSize getRepeatedFieldSize;
      final ObjectListElementGetter getEnumListElement;
      final IntListElementGetter getEnumValueListElement;
      final Descriptors.EnumDescriptor enumDescriptor;
      final List<Descriptors.EnumValueDescriptor> enumValues;

      ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      ProtoWriteSupport<?>.EnumWriter elementWriter;

      EnumListFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.getRepeatedFieldSize = getRepeatedFieldSize(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getEnumListElement = getGetObjectListElement(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getEnumValueListElement = getGetIntListElement(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.enumDescriptor = fieldDescriptor.getEnumType();
        this.enumValues = enumDescriptor.getValues();
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.arrayWriter = (ProtoWriteSupport<?>.ArrayWriter) fieldWriter;
        this.elementWriter = (ProtoWriteSupport<?>.EnumWriter) arrayWriter.fieldWriter;
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
          ProtocolMessageEnum enum_ = (ProtocolMessageEnum) getEnumListElement.getElement(object, i);
          Enum<?> javaEnum = (Enum<?>) enum_;
          Descriptors.EnumValueDescriptor enumValueDescriptor;
          if (javaEnum.ordinal() < enumValues.size()) {
            enumValueDescriptor = enumValues.get(javaEnum.ordinal());
          } else {
            enumValueDescriptor = enumDescriptor.findValueByNumberCreatingIfUnknown(
                getEnumValueListElement.getElement(object, i));
          }
          elementWriter.writeRawValue(enumValueDescriptor);
          arrayWriter.writeAfterElement();
        }
        arrayWriter.writeAfterAll();
      }
    }

    static class BooleanFieldOfObjectWriter implements FieldOfObjectWriter {
      final BooleanValueGetter getValue;
      final FieldHasValue fieldHasValue;

      ProtoWriteSupport<?>.BooleanWriter fieldWriter;

      BooleanFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.fieldHasValue = getHasValueOrNull(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getValue = getGetBooleanValue(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.fieldWriter = (ProtoWriteSupport<?>.BooleanWriter) fieldWriter;
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

      ProtoWriteSupport<?>.ArrayWriter arrayWriter;
      ProtoWriteSupport<?>.BooleanWriter elementWriter;

      BooleanListFieldOfObjectWriter(
          FieldDescriptor fieldDescriptor, Class<? extends MessageOrBuilder> proto3MessageOrBuilderInterface)
          throws Throwable {
        this.getRepeatedFieldSize = getRepeatedFieldSize(fieldDescriptor, proto3MessageOrBuilderInterface);
        this.getListElement = getGetBooleanListElement(fieldDescriptor, proto3MessageOrBuilderInterface);
      }

      @Override
      public void setFieldWriter(ProtoWriteSupport<?>.FieldWriter fieldWriter) {
        this.arrayWriter = (ProtoWriteSupport<?>.ArrayWriter) fieldWriter;
        this.elementWriter = (ProtoWriteSupport<?>.BooleanWriter) arrayWriter.fieldWriter;
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
