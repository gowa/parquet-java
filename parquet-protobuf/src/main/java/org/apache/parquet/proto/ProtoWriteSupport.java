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

import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.util.Timestamps;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import com.twitter.elephantbird.util.Protobufs;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
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

  private boolean writeSpecsCompliant = false;
  private boolean unwrapProtoWrappers = false;
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
    MessageType rootSchema = new ProtoSchemaConverter(configuration).convert(descriptor);
    validatedMapping(descriptor, rootSchema);

    this.messageWriter = new MessageWriter(descriptor, rootSchema);

    ByteBuddyCodeGen.Introspector introspector = null;
    if (protoMessage != null) {
      introspector = new ByteBuddyCodeGen.Introspector(this.messageWriter, descriptor, protoMessage, false);
    }


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

    void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }

    /**
     * sets index of field inside parquet message.
     */
    void setIndex(int index) {
      this.index = index;
    }

    /**
     * Used for writing repeated fields
     */
    void writeRawValue(Object value) {}

    /**
     * Used for writing nonrepeated (optional, required) fields
     */
    void writeField(Object value) {
      if (!(this instanceof ProtoWriteSupport.MapWriter)) {
        recordConsumer.startField(fieldName, index);
      }
      writeRawValue(value);
      if (!(this instanceof ProtoWriteSupport.MapWriter)) {
        recordConsumer.endField(fieldName, index);
      }
    }
  }

  class MessageWriter extends FieldWriter {

    final FieldWriter[] fieldWriters;

    @SuppressWarnings("unchecked")
    MessageWriter(Descriptor descriptor, GroupType schema) {
      List<FieldDescriptor> fields = descriptor.getFields();
      fieldWriters = (FieldWriter[]) Array.newInstance(FieldWriter.class, fields.size());

      for (FieldDescriptor fieldDescriptor : fields) {
        String name = fieldDescriptor.getName();
        Type type = schema.getType(name);
        FieldWriter writer = createWriter(fieldDescriptor, type);

        if (writeSpecsCompliant && fieldDescriptor.isRepeated() && !fieldDescriptor.isMapField()) {
          writer = new ArrayWriter(writer);
        } else if (!writeSpecsCompliant && fieldDescriptor.isRepeated()) {
          // the old schemas style used to write maps as repeated fields instead of wrapping them in a LIST
          writer = new RepeatedWriter(writer);
        }

        writer.setFieldName(name);
        writer.setIndex(schema.getFieldIndex(name));

        fieldWriters[fieldDescriptor.getIndex()] = writer;
      }
    }

    private FieldWriter createWriter(FieldDescriptor fieldDescriptor, Type type) {

      switch (fieldDescriptor.getJavaType()) {
        case STRING:
          return new StringWriter();
        case MESSAGE:
          return createMessageWriter(fieldDescriptor, type);
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

    private FieldWriter createMessageWriter(FieldDescriptor fieldDescriptor, Type type) {
      if (fieldDescriptor.isMapField() && writeSpecsCompliant) {
        return createMapWriter(fieldDescriptor, type);
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

      return new MessageWriter(fieldDescriptor.getMessageType(), getGroupType(type));
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

    private MapWriter createMapWriter(FieldDescriptor fieldDescriptor, Type type) {
      List<FieldDescriptor> fields = fieldDescriptor.getMessageType().getFields();
      if (fields.size() != 2) {
        throw new UnsupportedOperationException(
            "Expected two fields for the map (key/value), but got: " + fields);
      }

      // KeyFieldWriter
      FieldDescriptor keyProtoField = fields.get(0);
      FieldWriter keyWriter = createWriter(keyProtoField, type);
      keyWriter.setFieldName(keyProtoField.getName());
      keyWriter.setIndex(0);

      // ValueFieldWriter
      FieldDescriptor valueProtoField = fields.get(1);
      FieldWriter valueWriter = createWriter(valueProtoField, type);
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

    /**
     * Used for writing nonrepeated (optional, required) fields
     */
    @Override
    final void writeField(Object value) {
      recordConsumer.startField(fieldName, index);
      writeRawValue(value);
      recordConsumer.endField(fieldName, index);
    }

    private void writeAllFields(MessageOrBuilder pb) {
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
  }

  class ArrayWriter extends FieldWriter {
    final FieldWriter fieldWriter;

    ArrayWriter(FieldWriter fieldWriter) {
      this.fieldWriter = fieldWriter;
    }

    @Override
    final void writeRawValue(Object value) {
      throw new UnsupportedOperationException("Array has no raw value");
    }

    @Override
    final void writeField(Object value) {
      List<?> list = (List<?>) value;
      if (list.isEmpty()) {
        return;
      }

      recordConsumer.startField(fieldName, index);
      recordConsumer.startGroup();

      recordConsumer.startField("list", 0); // This is the wrapper group for the array field
      for (Object listEntry : list) {
        recordConsumer.startGroup();
        recordConsumer.startField("element", 0); // This is the mandatory inner field

        fieldWriter.writeRawValue(listEntry);

        recordConsumer.endField("element", 0);
        recordConsumer.endGroup();
      }
      recordConsumer.endField("list", 0);

      recordConsumer.endGroup();
      recordConsumer.endField(fieldName, index);
    }
  }

  /**
   * The RepeatedWriter is used to write collections (lists and maps) using the old style (without LIST and MAP
   * wrappers).
   */
  class RepeatedWriter extends FieldWriter {
    final FieldWriter fieldWriter;

    RepeatedWriter(FieldWriter fieldWriter) {
      this.fieldWriter = fieldWriter;
    }

    @Override
    final void writeRawValue(Object value) {
      throw new UnsupportedOperationException("Array has no raw value");
    }

    @Override
    final void writeField(Object value) {
      List<?> list = (List<?>) value;
      if (list.isEmpty()) {
        return;
      }

      recordConsumer.startField(fieldName, index);

      for (Object listEntry : list) {
        fieldWriter.writeRawValue(listEntry);
      }

      recordConsumer.endField(fieldName, index);
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
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addInteger((Integer) value);
    }
  }

  class LongWriter extends FieldWriter {

    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addLong((Long) value);
    }
  }

  class MapWriter extends FieldWriter {

    private final FieldWriter keyWriter;
    private final FieldWriter valueWriter;

    public MapWriter(FieldWriter keyWriter, FieldWriter valueWriter) {
      super();
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
    }

    @Override
    final void writeRawValue(Object value) {
      Collection<Message> collection = (Collection<Message>) value;
      if (collection.isEmpty()) {
        return;
      }
      recordConsumer.startField(fieldName, index);
      recordConsumer.startGroup();

      recordConsumer.startField("key_value", 0); // This is the wrapper group for the map field
      for (Message msg : collection) {
        recordConsumer.startGroup();

        final Descriptor descriptorForType = msg.getDescriptorForType();
        final FieldDescriptor keyDesc = descriptorForType.findFieldByName("key");
        final FieldDescriptor valueDesc = descriptorForType.findFieldByName("value");

        keyWriter.writeField(msg.getField(keyDesc));
        valueWriter.writeField(msg.getField(valueDesc));

        recordConsumer.endGroup();
      }

      recordConsumer.endField("key_value", 0);

      recordConsumer.endGroup();
      recordConsumer.endField(fieldName, index);
    }
  }

  class FloatWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addFloat((Float) value);
    }
  }

  class DoubleWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addDouble((Double) value);
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
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addBoolean((Boolean) value);
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
      com.google.type.TimeOfDay timeOfDay = (com.google.type.TimeOfDay) value;
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

  // --------- ByteBuddy CodeGen -----------
  static class ByteBuddyCodeGen {
    static class CodeGenException extends RuntimeException {
      public CodeGenException() {
        super();
      }

      public CodeGenException(String message) {
        super(message);
      }

      public CodeGenException(String message, Throwable cause) {
        super(message, cause);
      }

      public CodeGenException(Throwable cause) {
        super(cause);
      }
    }

    static class ReflectionUtil {
      static Method getDeclaredMethod(Class<?> clazz, String name, Class<?>... parameterTypes) {
        try {
          return clazz.getDeclaredMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
          throw new CodeGenException(e);
        }
      }

      static Method getDeclaredMethod(
          Class<?> protoClazz,
          FieldDescriptor fieldDescriptor,
          String name,
          Class<?>... parameters) {
        return getDeclaredMethod(
            protoClazz,
            name.replace("{}", getFieldNameForMethod(fieldDescriptor)),
            parameters);
      }

      static Method getDeclaredMethodByName(Class<?> clazz, String name) {
        for (Method method : clazz.getDeclaredMethods()) {
          if (name.equals(method.getName())) {
            return method;
          }
        }
        throw new CodeGenException("no such method on class " + clazz + ": " + name);
      }

      static Method getDeclaredMethodByName(Class<?> clazz, FieldDescriptor fieldDescriptor, String name) {
        return getDeclaredMethodByName(clazz, name.replace("{}", getFieldNameForMethod(fieldDescriptor)));
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
    }

    static class Field {
      private final Field parent;
      private final ProtoWriteSupport<?>.FieldWriter fieldWriter;

      private final FieldDescriptor fieldDescriptor; // can be null for root MessageWriter
      private final Descriptor messageType; // filled for Message fields (incl. Map)

      private java.lang.reflect.Type reflectionType;

      private Field(Field parent, ProtoWriteSupport<?>.FieldWriter fieldWriter, FieldDescriptor fieldDescriptor) {
        this.parent = parent;
        this.fieldWriter = fieldWriter;
        this.fieldDescriptor = fieldDescriptor;
        this.messageType = fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE ? fieldDescriptor.getMessageType() : null;
      }

      public Field(ProtoWriteSupport<?>.MessageWriter messageWriter, Class<? extends Message> protoMessage, Descriptor messageType) {
        this.parent = null;
        this.fieldWriter = messageWriter;
        this.fieldDescriptor = null;
        this.messageType = messageType;
        this.reflectionType = protoMessage;
      }

      public Field getParent() {
        return parent;
      }

      // helps codegen to deal with particular java getter for a proto field
      public java.lang.reflect.Type getReflectionType() {
        if (reflectionType == null) {
          reflectionType = initReflectionType();
        }
        return reflectionType;
      }

      private java.lang.reflect.Type initReflectionType() {
        // parent is always not null here
        if (isMap()) {
          return initMapReflectionType();
        } else if (parent.isMap()) {
          MapReflectionType mapReflectionType = (MapReflectionType) parent.getReflectionType();
          return fieldDescriptor.getNumber() == 0 ? mapReflectionType.key() : mapReflectionType.value();
        } else {
          return initRegularFieldReflectionType();
        }
      }

      private java.lang.reflect.Type initRegularFieldReflectionType() {
        Class<?> clazz;
        Class<?> parentProtoMessage = (Class<?>) parent.getReflectionType();
        if (fieldDescriptor.isRepeated()) {
          clazz = ReflectionUtil.getDeclaredMethod(parentProtoMessage, fieldDescriptor, "get{}", int.class).getReturnType();
        } else {
          clazz = ReflectionUtil.getDeclaredMethod(parentProtoMessage, fieldDescriptor, "get{}").getReturnType();
        }
        if (fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.ENUM) {
          return new EnumReflectionType(clazz, fieldDescriptor);
        }
        return clazz;
      }

      private java.lang.reflect.Type initMapReflectionType() {
        Class<?> parentProtoMessage = (Class<?>) parent.getReflectionType();
        Method method = ReflectionUtil.getDeclaredMethodByName(parentProtoMessage, fieldDescriptor, "get{}OrThrow");
        FieldDescriptor valueFieldDescriptor = fieldDescriptor.getMessageType().getFields().get(1);
        java.lang.reflect.Type valueType;
        if (valueFieldDescriptor.getJavaType() == FieldDescriptor.JavaType.ENUM) {
          valueType = new EnumReflectionType(method.getReturnType(), valueFieldDescriptor);
        } else {
          valueType = method.getReturnType();
        }
        return new MapReflectionType(method.getParameterTypes()[0], valueType);
      }

      // helps codegen to identify unique methods and supporting fields to write messages, map entries and enums
      public java.lang.reflect.Type getCodeGenerationKey() {
        java.lang.reflect.Type myReflectionType = getReflectionType();
        if (isMessage() || (isMap() && getChildren().get(1).isMessage())) {
          Field f = this.getParent();
          int depth = 0;
          while (f != null) {
            if (Objects.equals(myReflectionType, f.getReflectionType())) {
              depth += 1;
            }
            f = f.getParent();
          }
          return new CodeGenerationKey(myReflectionType, depth);
        }
        if (isMap()) {
          return new CodeGenerationKey(myReflectionType, 0);
        }
        if (isEnumWriter()) {
          // for enums extra fields have to be prepared and their content depend on Enum type itself, not on the declaring message type
          return ((EnumReflectionType) myReflectionType).asCodeGenerationKey();
        }
        throw new CodeGenException("no code generation is allowed for this field");
      }

      public ProtoWriteSupport<?>.FieldWriter getFieldWriter() {
        return fieldWriter;
      }

      public FieldDescriptor getFieldDescriptor() {
        return fieldDescriptor;
      }

      public List<Field> getChildren() {
        if (isMessage()) {
          ProtoWriteSupport<?>.FieldWriter[] fieldWriters = asMessageWriter().fieldWriters;
          return resolveChildFields(fieldWriters);
        } else if (isMap()) {
          ProtoWriteSupport<?>.FieldWriter[] fieldWriters = new ProtoWriteSupport<?>.FieldWriter[]{getMapKeyWriter(), getMapValueWriter()};
          return resolveChildFields(fieldWriters);
        } else {
          return Collections.emptyList();
        }
      }

      private List<Field> resolveChildFields(ProtoWriteSupport<?>.FieldWriter[] fieldWriters) {
        List<FieldDescriptor> fieldDescriptors = messageType.getFields();
        int fieldsCount = fieldWriters.length;
        List<Field> result = new ArrayList<>(fieldsCount);
        for (int i = 0; i < fieldsCount; i++) {
          result.add(resolveField(fieldWriters[i], fieldDescriptors.get(i)));
        }
        return result;
      }

      public boolean isMessage() {
        // this does not include Map and Message fields written as binary
        return !isMap() && fieldWriter instanceof ProtoWriteSupport<?>.MessageWriter;
      }

      private ProtoWriteSupport<?>.MessageWriter asMessageWriter() {
        if (!isMessage()) {
          throw new CodeGenException();
        }
        return (ProtoWriteSupport<?>.MessageWriter) fieldWriter;
      }

      public boolean isProto2() {
        if (!isMessage()) {
          throw new CodeGenException();
        }
        return messageType.getFile().getSyntax() == Descriptors.FileDescriptor.Syntax.PROTO2;
      }

      public boolean isMap() {
        // fieldDescriptor is null for root message which is message, not map.
        return fieldDescriptor != null && fieldDescriptor.isMapField();
      }

      private ProtoWriteSupport<?>.FieldWriter getMapKeyWriter() {
        if (!isMap()) {
          throw new CodeGenException();
        }
        if (fieldWriter instanceof ProtoWriteSupport<?>.MessageWriter) {
          return ((ProtoWriteSupport<?>.MessageWriter) fieldWriter).fieldWriters[0];
        } else if (fieldWriter instanceof ProtoWriteSupport<?>.MapWriter) {
          return ((ProtoWriteSupport<?>.MapWriter) fieldWriter).keyWriter;
        } else {
          throw new CodeGenException();
        }
      }

      private ProtoWriteSupport<?>.FieldWriter getMapValueWriter() {
        if (!isMap()) {
          throw new CodeGenException();
        }
        if (fieldWriter instanceof ProtoWriteSupport<?>.MessageWriter) {
          return ((ProtoWriteSupport<?>.MessageWriter) fieldWriter).fieldWriters[1];
        } else if (fieldWriter instanceof ProtoWriteSupport<?>.MapWriter) {
          return ((ProtoWriteSupport<?>.MapWriter) fieldWriter).valueWriter;
        } else {
          throw new CodeGenException();
        }
      }

      public boolean isEnumWriter() {
        return fieldWriter instanceof ProtoWriteSupport<?>.EnumWriter;
      }

      public ProtoWriteSupport<?>.EnumWriter asEnumWriter() {
        if (!isEnumWriter()) {
          throw new CodeGenException();
        }
        return (ProtoWriteSupport<?>.EnumWriter) fieldWriter;
      }

      private Field resolveField(ProtoWriteSupport<?>.FieldWriter fieldWriter, FieldDescriptor fieldDescriptor) {
        if (fieldWriter instanceof ProtoWriteSupport<?>.ArrayWriter) {
          return resolveField(((ProtoWriteSupport<?>.ArrayWriter) fieldWriter).fieldWriter, fieldDescriptor);
        } else if (fieldWriter instanceof ProtoWriteSupport<?>.RepeatedWriter) {
          return resolveField(((ProtoWriteSupport<?>.RepeatedWriter) fieldWriter).fieldWriter, fieldDescriptor);
        } else {
          return new Field(this, fieldWriter, fieldDescriptor);
        }
      }
    }

    static final class MapReflectionType implements java.lang.reflect.Type {
      private final java.lang.reflect.Type key;
      private final java.lang.reflect.Type value;

      public MapReflectionType(java.lang.reflect.Type key, java.lang.reflect.Type value) {
        this.key = key;
        this.value = value;
      }

      public java.lang.reflect.Type key() {
        return key;
      }

      public java.lang.reflect.Type value() {
        return value;
      }

      @Override
      public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        MapReflectionType that = (MapReflectionType) o;
        return Objects.equals(key, that.key) && Objects.equals(value, that.value);
      }

      @Override
      public int hashCode() {
        return Objects.hash(key, value);
      }
    }

    static final class CodeGenerationKey implements java.lang.reflect.Type {
      private final java.lang.reflect.Type type;
      private final int depth;

      public CodeGenerationKey(java.lang.reflect.Type type, int depth) {
        this.type = type;
        this.depth = depth;
      }

      @Override
      public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        CodeGenerationKey that = (CodeGenerationKey) o;
        return depth == that.depth && Objects.equals(type, that.type);
      }

      @Override
      public int hashCode() {
        return Objects.hash(type, depth);
      }
    }

    static final class EnumReflectionType implements java.lang.reflect.Type {
      private final Class<?> clazz;
      private final boolean enumSupportsUnknownValues; // determines if Enum actually supports unknown values
      private final Boolean fieldSupportsUnknownValues; // only used to help identify which getter to use for enums

      public EnumReflectionType(Class<?> clazz, FieldDescriptor enumField) {
        this.clazz = clazz;
        this.enumSupportsUnknownValues = !enumField.getEnumType().isClosed();
        this.fieldSupportsUnknownValues = !enumField.legacyEnumFieldTreatedAsClosed();
      }

      private EnumReflectionType(Class<?> clazz, boolean enumSupportsUnknownValues) {
        this.clazz = clazz;
        this.enumSupportsUnknownValues = enumSupportsUnknownValues;
        this.fieldSupportsUnknownValues = null;
      }

      public EnumReflectionType asCodeGenerationKey() {
        return new EnumReflectionType(clazz, enumSupportsUnknownValues);
      }

      @Override
      public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        EnumReflectionType that = (EnumReflectionType) o;
        return enumSupportsUnknownValues == that.enumSupportsUnknownValues && Objects.equals(clazz, that.clazz) && Objects.equals(fieldSupportsUnknownValues, that.fieldSupportsUnknownValues);
      }

      @Override
      public int hashCode() {
        return Objects.hash(clazz, enumSupportsUnknownValues, fieldSupportsUnknownValues);
      }
    }

    private interface FieldVisitor {
      void visitField(Field field);
    }

    private static void scan(Field startField, FieldVisitor visitor) {
      Queue<Field> queue = new ArrayDeque<>();
      queue.add(startField);

      while (!queue.isEmpty()) {
        Field field = queue.poll();
        visitor.visitField(field);
        queue.addAll(field.getChildren());
      }
    }

    static class Introspector {
      private final Map<java.lang.reflect.Type, FieldIndex> proto2MessageWriters = new LinkedHashMap<>();
      private final Map<java.lang.reflect.Type, FieldIndex> codeGenMessageWriters = new LinkedHashMap<>();
      private final Map<java.lang.reflect.Type, FieldIndex> mapWriters = new LinkedHashMap<>();
      private final Map<java.lang.reflect.Type, FieldIndex> enumFields = new LinkedHashMap<>();

      private static class FieldIndex {
        private final Field field;
        private final int index;

        private FieldIndex(Field field, int index) {
          this.field = field;
          this.index = index;
        }
      }

      public Introspector(ProtoWriteSupport<?>.MessageWriter messageWriter,
                          Descriptors.Descriptor descriptor,
                          Class<? extends Message> protoMessage,
                          boolean proto2CodeGen) {
        scan(new Field(messageWriter, protoMessage, descriptor), new FieldVisitor() {
          @Override
          public void visitField(Field field) {
            if (field.isMessage() ) {
              java.lang.reflect.Type key = field.getCodeGenerationKey();
              addFieldIndex(key, field, !proto2CodeGen && field.isProto2() ? proto2MessageWriters : codeGenMessageWriters);
            } else if (field.isMap()) {
              java.lang.reflect.Type key = field.getCodeGenerationKey();
              addFieldIndex(key, field, mapWriters);
            } else if (field.isEnumWriter()) {
              java.lang.reflect.Type key = field.getCodeGenerationKey();
              addFieldIndex(key, field, enumFields);
            }
          }
        });
      }

      private void addFieldIndex(java.lang.reflect.Type key, Field field, Map<java.lang.reflect.Type, FieldIndex> registry) {
        if (registry.containsKey(key)) {
          return;
        }
        registry.put(key, new FieldIndex(field, registry.size()));
      }
    }
  }
}
