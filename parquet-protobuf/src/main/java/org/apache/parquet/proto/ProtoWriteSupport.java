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
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
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

  public static final String PB_USE_JAVA_REFLECTION = "parquet.proto.useJavaReflection";

  private boolean writeSpecsCompliant = false;
  private boolean unwrapProtoWrappers = false;
  private boolean useJavaReflection = true;
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

  public static void setUseJavaReflection(Configuration configuration, boolean useJavaReflection) {
    configuration.setBoolean(PB_USE_JAVA_REFLECTION, useJavaReflection);
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
    useJavaReflection = configuration.getBoolean(PB_USE_JAVA_REFLECTION, useJavaReflection);
    MessageType rootSchema = new ProtoSchemaConverter(configuration).convert(descriptor);
    validatedMapping(descriptor, rootSchema);

    Class<? extends MessageOrBuilder> messageOrBuilderInterface = protoMessage == null
            || !useJavaReflection
            || descriptor.getFile().getSyntax() != Descriptors.FileDescriptor.Syntax.PROTO3
        ? null
        : ProtobufJavaReflectionUtil.getMessageOrBuilderInterfaceOrNull(protoMessage);

    this.messageWriter = new MessageWriter(descriptor, rootSchema, messageOrBuilderInterface);

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

  abstract class FieldOfObjectWriter {
    abstract void setFieldWriter(FieldWriter fieldWriter);

    abstract void writeFieldOfObject(Object object);
  }

  class FieldWriter {
    String fieldName;
    int index = -1;
    FieldOfObjectWriter fieldOfObjectWriter;

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

    void createFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) {
      try {
        switch (fieldDescriptor.getJavaType()) {
          case INT:
            fieldOfObjectWriter = fieldDescriptor.isRepeated()
                ? new IntListFieldOfObjectWriter(fieldDescriptor, objectClass)
                : new IntFieldOfObjectWriter(fieldDescriptor, objectClass);
            break;
          case LONG:
            fieldOfObjectWriter = fieldDescriptor.isRepeated()
                ? new LongListFieldOfObjectWriter(fieldDescriptor, objectClass)
                : new LongFieldOfObjectWriter(fieldDescriptor, objectClass);
            break;
          case FLOAT:
            fieldOfObjectWriter = fieldDescriptor.isRepeated()
                ? new FloatListFieldOfObjectWriter(fieldDescriptor, objectClass)
                : new FloatFieldOfObjectWriter(fieldDescriptor, objectClass);
            break;
          case DOUBLE:
            fieldOfObjectWriter = fieldDescriptor.isRepeated()
                ? new DoubleListFieldOfObjectWriter(fieldDescriptor, objectClass)
                : new DoubleFieldOfObjectWriter(fieldDescriptor, objectClass);
            break;
          case BOOLEAN:
            fieldOfObjectWriter = fieldDescriptor.isRepeated()
                ? new BooleanListFieldOfObjectWriter(fieldDescriptor, objectClass)
                : new BooleanFieldOfObjectWriter(fieldDescriptor, objectClass);
            break;
          case STRING:
          case BYTE_STRING:
            fieldOfObjectWriter = fieldDescriptor.isRepeated()
                ? new ObjectListFieldOfObjectWriter(fieldDescriptor, objectClass)
                : new ObjectFieldOfObjectWriter(fieldDescriptor, objectClass);
            break;
          case ENUM:
            fieldOfObjectWriter = fieldDescriptor.isRepeated()
                ? new EnumListFieldOfObjectWriter(fieldDescriptor, objectClass)
                : new EnumFieldOfObjectWriter(fieldDescriptor, objectClass);
            break;
          case MESSAGE:
            if (fieldDescriptor.isMapField()) {
              fieldOfObjectWriter = new MapFieldOfObjectWriter(fieldDescriptor, objectClass);
            } else {
              fieldOfObjectWriter = fieldDescriptor.isRepeated()
                  ? new ObjectListFieldOfObjectWriter(fieldDescriptor, objectClass)
                  : new ObjectFieldOfObjectWriter(fieldDescriptor, objectClass);
            }
            break;
        }
        fieldOfObjectWriter.setFieldWriter(this);
      } catch (Throwable t) {
        throw new RuntimeException("was not able to initialize field-of-object writer", t);
      }
    }

    FieldOfObjectWriter getFieldOfObjectWriter() {
      return fieldOfObjectWriter;
    }
  }

  class MessageWriter extends FieldWriter {

    final FieldWriter[] fieldWriters;
    final Class<?> messageOrBuilderInterface;

    @SuppressWarnings("unchecked")
    MessageWriter(
        Descriptor descriptor, GroupType schema, Class<? extends MessageOrBuilder> messageOrBuilderInterface) {
      List<FieldDescriptor> fields = descriptor.getFields();
      fieldWriters = (FieldWriter[]) Array.newInstance(FieldWriter.class, fields.size());

      ProtobufJavaReflectionUtil.verifyMessageOrBuilderInterface(messageOrBuilderInterface);
      this.messageOrBuilderInterface = messageOrBuilderInterface;

      for (FieldDescriptor fieldDescriptor : fields) {
        String name = fieldDescriptor.getName();
        Type type = schema.getType(name);
        FieldWriter writer = createWriter(fieldDescriptor, type, messageOrBuilderInterface, null);

        if (fieldDescriptor.isRepeated() && !fieldDescriptor.isMapField()) {
          writer = new ArrayWriter(writer);
        }

        if (messageOrBuilderInterface != null) {
          writer.createFieldOfObjectWriter(fieldDescriptor, messageOrBuilderInterface);
        }

        writer.setFieldName(name);
        writer.setIndex(schema.getFieldIndex(name));

        fieldWriters[fieldDescriptor.getIndex()] = writer;
      }
    }

    private FieldWriter createWriter(
        FieldDescriptor fieldDescriptor,
        Type type,
        Class<?> objectClass,
        Class<? extends MessageOrBuilder> associatedMessageClass) {

      switch (fieldDescriptor.getJavaType()) {
        case STRING:
          return new StringWriter();
        case MESSAGE:
          return createMessageWriter(fieldDescriptor, type, objectClass, associatedMessageClass);
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
        FieldDescriptor fieldDescriptor,
        Type type,
        Class<?> objectClass,
        Class<? extends MessageOrBuilder> associatedMessageClass) {
      if (fieldDescriptor.isMapField()) {
        return createMapWriter(
            fieldDescriptor,
            type,
            ProtobufJavaReflectionUtil.extractAssociatedMessageOrBuilderInterfaceOfFieldValue(
                fieldDescriptor, objectClass));
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

      return new MessageWriter(
          fieldDescriptor.getMessageType(),
          getGroupType(type),
          associatedMessageClass != null
              ? associatedMessageClass
              : ProtobufJavaReflectionUtil.extractAssociatedMessageOrBuilderInterfaceOfFieldValue(
                  fieldDescriptor, objectClass));
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
        FieldDescriptor fieldDescriptor, Type type, Class<? extends MessageOrBuilder> valueMessageOrBuilder) {
      List<FieldDescriptor> fields = fieldDescriptor.getMessageType().getFields();
      if (fields.size() != 2) {
        throw new UnsupportedOperationException(
            "Expected two fields for the map (key/value), but got: " + fields);
      }

      // KeyFieldWriter
      FieldDescriptor keyProtoField = fields.get(0);
      FieldWriter keyWriter = createWriter(keyProtoField, type, null, null);
      keyWriter.setFieldName(keyProtoField.getName());
      keyWriter.setIndex(0);

      // ValueFieldWriter
      FieldDescriptor valueProtoField = fields.get(1);
      FieldWriter valueWriter = createWriter(
          valueProtoField,
          writeSpecsCompliant ? type : type.asGroupType().getType("value"),
          null,
          valueMessageOrBuilder);
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
      if (messageOrBuilderInterface != null && messageOrBuilderInterface.isInstance(pb)) {
        for (FieldWriter fieldWriter : fieldWriters) {
          fieldWriter.getFieldOfObjectWriter().writeFieldOfObject(pb);
        }
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
  }

  class RepeatedFieldWriter extends FieldWriter {

    @Override
    final void writeRawValue(Object value) {
      throw new UnsupportedOperationException("raw value is not supported");
    }

    @Override
    final void writeField(Object value) {
      List<?> list = (List<?>) value;
      if (list.isEmpty()) {
        return;
      }

      writeBeforeAll();
      for (Object element : list) {
        writeBeforeElement();
        writeElement(element);
        writeAfterElement();
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

  static class ProtobufJavaReflectionUtil {

    static Class<? extends MessageOrBuilder> getMessageOrBuilderInterfaceOrNull(Class<?> messageClass) {
      return Stream.of(messageClass)
          .filter(x -> GeneratedMessageV3.class.isAssignableFrom(x))
          .flatMap(x -> Arrays.stream(x.getInterfaces()))
          .filter(MessageOrBuilder.class::isAssignableFrom)
          .map(x -> (Class<? extends MessageOrBuilder>) x)
          .findFirst()
          .orElse(null);
    }

    // almost the same as com.google.protobuf.Descriptors.FieldDescriptor#fieldNameToJsonName
    // but capitalizing the first letter after each last digit
    static String getFieldNameForMethod(FieldDescriptor fieldDescriptor) {
      String name = fieldDescriptor.getName();
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

    static void verifyMessageOrBuilderInterface(Class<?> objectClass) {
      if (objectClass == null) {
        return;
      }
      if (!objectClass.isInterface() || !MessageOrBuilder.class.isAssignableFrom(objectClass)) {
        throw new IllegalStateException(
            "a sub-interface of MessageOrBuilder interface is expected, but got " + objectClass);
      }
    }

    static FieldHasValue getHasValueOrNull(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      if (fieldDescriptor.isRepeated()) {
        throw new IllegalStateException("not supported for repeated fields");
      }
      verifyMessageOrBuilderInterface(objectClass);
      if (!fieldDescriptor.hasPresence()) {
        return null;
      }
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (FieldHasValue) LambdaMetafactory.metafactory(
              lookup,
              "hasValue",
              MethodType.methodType(FieldHasValue.class),
              MethodType.methodType(boolean.class, Object.class),
              lookup.findVirtual(
                  objectClass,
                  "has" + getFieldNameForMethod(fieldDescriptor),
                  MethodType.methodType(boolean.class)),
              MethodType.methodType(boolean.class, objectClass))
          .getTarget()
          .invokeExact();
    }

    static GetRepeatedFieldSize getRepeatedFieldSize(FieldDescriptor fieldDescriptor, Class<?> objectClass)
        throws Throwable {
      if (!fieldDescriptor.isRepeated()) {
        throw new IllegalStateException("not supported for non-repeated fields");
      }
      verifyMessageOrBuilderInterface(objectClass);
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (GetRepeatedFieldSize) LambdaMetafactory.metafactory(
              lookup,
              "getSize",
              MethodType.methodType(GetRepeatedFieldSize.class),
              MethodType.methodType(int.class, Object.class),
              lookup.findVirtual(
                  objectClass,
                  "get" + getFieldNameForMethod(fieldDescriptor) + "Count",
                  MethodType.methodType(int.class)),
              MethodType.methodType(int.class, objectClass))
          .getTarget()
          .invokeExact();
    }

    static ObjectValueGetter getGetObjectValue(FieldDescriptor fieldDescriptor, Class<?> objectClass)
        throws Throwable {
      if (fieldDescriptor.isRepeated() && !fieldDescriptor.isMapField()) {
        throw new IllegalStateException("not supported for repeated fields");
      }
      verifyMessageOrBuilderInterface(objectClass);
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      String mapOrBuilder = fieldDescriptor.isMapField()
          ? "Map"
          : fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE ? "OrBuilder" : "";
      return (ObjectValueGetter) LambdaMetafactory.metafactory(
              lookup,
              "getValue",
              MethodType.methodType(ObjectValueGetter.class),
              MethodType.methodType(Object.class, Object.class),
              lookup.unreflect(objectClass.getMethod(
                  "get" + getFieldNameForMethod(fieldDescriptor) + mapOrBuilder)),
              MethodType.methodType(Object.class, objectClass))
          .getTarget()
          .invokeExact();
    }

    static IntValueGetter getGetIntValue(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      if (fieldDescriptor.isRepeated()
          || !(fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.INT
              || fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.ENUM)) {
        throw new IllegalStateException("not supported for repeated fields or non-int or non-enums");
      }
      verifyMessageOrBuilderInterface(objectClass);
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      String enumValueSuffix = fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.ENUM ? "Value" : "";
      return (IntValueGetter) LambdaMetafactory.metafactory(
              lookup,
              "getValue",
              MethodType.methodType(IntValueGetter.class),
              MethodType.methodType(int.class, Object.class),
              lookup.findVirtual(
                  objectClass,
                  "get" + getFieldNameForMethod(fieldDescriptor) + enumValueSuffix,
                  MethodType.methodType(int.class)),
              MethodType.methodType(int.class, objectClass))
          .getTarget()
          .invokeExact();
    }

    static LongValueGetter getGetLongValue(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      if (fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.LONG) {
        throw new IllegalStateException("not supported for repeated fields or non-long");
      }
      verifyMessageOrBuilderInterface(objectClass);
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (LongValueGetter) LambdaMetafactory.metafactory(
              lookup,
              "getValue",
              MethodType.methodType(LongValueGetter.class),
              MethodType.methodType(long.class, Object.class),
              lookup.findVirtual(
                  objectClass,
                  "get" + getFieldNameForMethod(fieldDescriptor),
                  MethodType.methodType(long.class)),
              MethodType.methodType(long.class, objectClass))
          .getTarget()
          .invokeExact();
    }

    static DoubleValueGetter getGetDoubleValue(FieldDescriptor fieldDescriptor, Class<?> objectClass)
        throws Throwable {
      if (fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.DOUBLE) {
        throw new IllegalStateException("not supported for repeated fields or non-double");
      }
      verifyMessageOrBuilderInterface(objectClass);
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (DoubleValueGetter) LambdaMetafactory.metafactory(
              lookup,
              "getValue",
              MethodType.methodType(DoubleValueGetter.class),
              MethodType.methodType(double.class, Object.class),
              lookup.findVirtual(
                  objectClass,
                  "get" + getFieldNameForMethod(fieldDescriptor),
                  MethodType.methodType(double.class)),
              MethodType.methodType(double.class, objectClass))
          .getTarget()
          .invokeExact();
    }

    static FloatValueGetter getGetFloatValue(FieldDescriptor fieldDescriptor, Class<?> objectClass)
        throws Throwable {
      if (fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.FLOAT) {
        throw new IllegalStateException("not supported for repeated fields or non-float");
      }
      verifyMessageOrBuilderInterface(objectClass);
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (FloatValueGetter) LambdaMetafactory.metafactory(
              lookup,
              "getValue",
              MethodType.methodType(FloatValueGetter.class),
              MethodType.methodType(float.class, Object.class),
              lookup.findVirtual(
                  objectClass,
                  "get" + getFieldNameForMethod(fieldDescriptor),
                  MethodType.methodType(float.class)),
              MethodType.methodType(float.class, objectClass))
          .getTarget()
          .invokeExact();
    }

    static BooleanValueGetter getGetBooleanValue(FieldDescriptor fieldDescriptor, Class<?> objectClass)
        throws Throwable {
      if (fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.BOOLEAN) {
        throw new IllegalStateException("not supported for repeated fields or non-boolean");
      }
      verifyMessageOrBuilderInterface(objectClass);
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (BooleanValueGetter) LambdaMetafactory.metafactory(
              lookup,
              "getValue",
              MethodType.methodType(BooleanValueGetter.class),
              MethodType.methodType(boolean.class, Object.class),
              lookup.findVirtual(
                  objectClass,
                  "get" + getFieldNameForMethod(fieldDescriptor),
                  MethodType.methodType(boolean.class)),
              MethodType.methodType(boolean.class, objectClass))
          .getTarget()
          .invokeExact();
    }

    static ObjectListElementGetter getGetObjectListElement(FieldDescriptor fieldDescriptor, Class<?> objectClass)
        throws Throwable {
      if (!fieldDescriptor.isRepeated() || fieldDescriptor.isMapField()) {
        throw new IllegalStateException("not supported for non-repeated fields or maps");
      }
      verifyMessageOrBuilderInterface(objectClass);
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      String orBuilder =
          !fieldDescriptor.isMapField() && fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE
              ? "OrBuilder"
              : "";
      return (ObjectListElementGetter) LambdaMetafactory.metafactory(
              lookup,
              "getElement",
              MethodType.methodType(ObjectListElementGetter.class),
              MethodType.methodType(Object.class, Object.class, int.class),
              lookup.unreflect(objectClass.getMethod(
                  "get" + getFieldNameForMethod(fieldDescriptor) + orBuilder, int.class)),
              MethodType.methodType(Object.class, objectClass, int.class))
          .getTarget()
          .invokeExact();
    }

    static IntListElementGetter getGetIntListElement(FieldDescriptor fieldDescriptor, Class<?> objectClass)
        throws Throwable {
      if (!fieldDescriptor.isRepeated()
          || !(fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.INT
              || fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.ENUM)) {
        throw new IllegalStateException("not supported for non-repeated fields or non-ints or non-enums");
      }
      verifyMessageOrBuilderInterface(objectClass);
      String enumValueSuffix = fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.ENUM ? "Value" : "";
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (IntListElementGetter) LambdaMetafactory.metafactory(
              lookup,
              "getElement",
              MethodType.methodType(IntListElementGetter.class),
              MethodType.methodType(int.class, Object.class, int.class),
              lookup.unreflect(objectClass.getMethod(
                  "get" + getFieldNameForMethod(fieldDescriptor) + enumValueSuffix, int.class)),
              MethodType.methodType(int.class, objectClass, int.class))
          .getTarget()
          .invokeExact();
    }

    static LongListElementGetter getGetLongListElement(FieldDescriptor fieldDescriptor, Class<?> objectClass)
        throws Throwable {
      if (!fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.LONG) {
        throw new IllegalStateException("not supported for non-repeated fields or non-longs");
      }
      verifyMessageOrBuilderInterface(objectClass);
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (LongListElementGetter) LambdaMetafactory.metafactory(
              lookup,
              "getElement",
              MethodType.methodType(LongListElementGetter.class),
              MethodType.methodType(long.class, Object.class, int.class),
              lookup.unreflect(
                  objectClass.getMethod("get" + getFieldNameForMethod(fieldDescriptor), int.class)),
              MethodType.methodType(long.class, objectClass, int.class))
          .getTarget()
          .invokeExact();
    }

    static DoubleListElementGetter getGetDoubleListElement(FieldDescriptor fieldDescriptor, Class<?> objectClass)
        throws Throwable {
      if (!fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.DOUBLE) {
        throw new IllegalStateException("not supported for non-repeated fields or non-doubles");
      }
      verifyMessageOrBuilderInterface(objectClass);
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (DoubleListElementGetter) LambdaMetafactory.metafactory(
              lookup,
              "getElement",
              MethodType.methodType(DoubleListElementGetter.class),
              MethodType.methodType(double.class, Object.class, int.class),
              lookup.unreflect(
                  objectClass.getMethod("get" + getFieldNameForMethod(fieldDescriptor), int.class)),
              MethodType.methodType(double.class, objectClass, int.class))
          .getTarget()
          .invokeExact();
    }

    static FloatListElementGetter getGetFloatListElement(FieldDescriptor fieldDescriptor, Class<?> objectClass)
        throws Throwable {
      if (!fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.FLOAT) {
        throw new IllegalStateException("not supported for non-repeated fields or non-floats");
      }
      verifyMessageOrBuilderInterface(objectClass);
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (FloatListElementGetter) LambdaMetafactory.metafactory(
              lookup,
              "getElement",
              MethodType.methodType(FloatListElementGetter.class),
              MethodType.methodType(float.class, Object.class, int.class),
              lookup.unreflect(
                  objectClass.getMethod("get" + getFieldNameForMethod(fieldDescriptor), int.class)),
              MethodType.methodType(float.class, objectClass, int.class))
          .getTarget()
          .invokeExact();
    }

    static BooleanListElementGetter getGetBooleanListElement(FieldDescriptor fieldDescriptor, Class<?> objectClass)
        throws Throwable {
      if (!fieldDescriptor.isRepeated() || fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.BOOLEAN) {
        throw new IllegalStateException("not supported for non-repeated fields or non-booleans");
      }
      verifyMessageOrBuilderInterface(objectClass);
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return (BooleanListElementGetter) LambdaMetafactory.metafactory(
              lookup,
              "getElement",
              MethodType.methodType(BooleanListElementGetter.class),
              MethodType.methodType(boolean.class, Object.class, int.class),
              lookup.unreflect(
                  objectClass.getMethod("get" + getFieldNameForMethod(fieldDescriptor), int.class)),
              MethodType.methodType(boolean.class, objectClass, int.class))
          .getTarget()
          .invokeExact();
    }

    static Class<? extends MessageOrBuilder> extractAssociatedMessageOrBuilderInterfaceOfFieldValue(
        FieldDescriptor fieldDescriptor, Class<?> objectClass) {
      if (objectClass == null) {
        return null;
      }
      if (fieldDescriptor.isMapField()) {
        try {
          Method getter = objectClass.getMethod("get" + getFieldNameForMethod(fieldDescriptor) + "Map");
          ParameterizedType mapKV = (ParameterizedType) getter.getGenericReturnType();
          Class<?> valueClass = (Class<?>) mapKV.getActualTypeArguments()[1]; // value
          return ProtobufJavaReflectionUtil.getMessageOrBuilderInterfaceOrNull(valueClass);
        } catch (Exception e) {
          throw new UnsupportedOperationException(
              "could not find a getter method for a map field: " + fieldDescriptor, e);
        }
      } else if (fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
        if (fieldDescriptor.isRepeated()) {
          try {
            Method getter = objectClass.getMethod(
                "get" + getFieldNameForMethod(fieldDescriptor) + "OrBuilder", int.class);
            Class<?> elementClass = getter.getReturnType(); // element
            verifyMessageOrBuilderInterface(elementClass);
            return (Class<? extends MessageOrBuilder>) elementClass;
          } catch (Exception e) {
            throw new UnsupportedOperationException(
                "could not find a getter method for a repeated field: " + fieldDescriptor, e);
          }
        } else {
          try {
            Method getter =
                objectClass.getMethod("get" + getFieldNameForMethod(fieldDescriptor) + "OrBuilder");
            Class<?> elementClass = getter.getReturnType();
            verifyMessageOrBuilderInterface(elementClass);
            return (Class<? extends MessageOrBuilder>) elementClass;
          } catch (Exception e) {
            throw new UnsupportedOperationException(
                "could not find a getter method for a non-repeated field: " + fieldDescriptor, e);
          }
        }
      } else {
        return null;
      }
    }
  }

  class ObjectFieldOfObjectWriter extends FieldOfObjectWriter {
    final ObjectValueGetter getValue;
    final FieldHasValue fieldHasValue;

    FieldWriter fieldWriter;

    ObjectFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.fieldHasValue = ProtobufJavaReflectionUtil.getHasValueOrNull(fieldDescriptor, objectClass);
      this.getValue = ProtobufJavaReflectionUtil.getGetObjectValue(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.fieldWriter = fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
      if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
        return;
      }
      fieldWriter.writeBeforeAll();
      fieldWriter.writeRawValue(getValue.getValue(object));
      fieldWriter.writeAfterAll();
    }
  }

  class ObjectListFieldOfObjectWriter extends FieldOfObjectWriter {
    final GetRepeatedFieldSize getRepeatedFieldSize;
    final ObjectListElementGetter getListElement;

    ArrayWriter arrayWriter;
    FieldWriter elementWriter;

    ObjectListFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.getRepeatedFieldSize = ProtobufJavaReflectionUtil.getRepeatedFieldSize(fieldDescriptor, objectClass);
      this.getListElement = ProtobufJavaReflectionUtil.getGetObjectListElement(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.arrayWriter = (ArrayWriter) fieldWriter;
      this.elementWriter = arrayWriter.fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
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

  class StringWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      Binary binaryString = Binary.fromString((String) value);
      recordConsumer.addBinary(binaryString);
    }
  }

  class IntFieldOfObjectWriter extends FieldOfObjectWriter {
    final IntValueGetter getValue;
    final FieldHasValue fieldHasValue;

    IntWriter fieldWriter;

    IntFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.fieldHasValue = ProtobufJavaReflectionUtil.getHasValueOrNull(fieldDescriptor, objectClass);
      this.getValue = ProtobufJavaReflectionUtil.getGetIntValue(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.fieldWriter = (IntWriter) fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
      if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
        return;
      }
      fieldWriter.writeBeforeAll();
      fieldWriter.writeRawValuePrimitive(getValue.getValue(object));
      fieldWriter.writeAfterAll();
    }
  }

  class IntListFieldOfObjectWriter extends FieldOfObjectWriter {
    final GetRepeatedFieldSize getRepeatedFieldSize;
    final IntListElementGetter getListElement;

    ArrayWriter arrayWriter;
    IntWriter elementWriter;

    IntListFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.getRepeatedFieldSize = ProtobufJavaReflectionUtil.getRepeatedFieldSize(fieldDescriptor, objectClass);
      this.getListElement = ProtobufJavaReflectionUtil.getGetIntListElement(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.arrayWriter = (ArrayWriter) fieldWriter;
      this.elementWriter = (IntWriter) arrayWriter.fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
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

  class IntWriter extends FieldWriter {
    final void writeRawValuePrimitive(int value) {
      recordConsumer.addInteger(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((int) value);
    }
  }

  class LongFieldOfObjectWriter extends FieldOfObjectWriter {
    final LongValueGetter getValue;
    final FieldHasValue fieldHasValue;

    LongWriter fieldWriter;

    LongFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.fieldHasValue = ProtobufJavaReflectionUtil.getHasValueOrNull(fieldDescriptor, objectClass);
      this.getValue = ProtobufJavaReflectionUtil.getGetLongValue(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.fieldWriter = (LongWriter) fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
      if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
        return;
      }
      fieldWriter.writeBeforeAll();
      fieldWriter.writeRawValuePrimitive(getValue.getValue(object));
      fieldWriter.writeAfterAll();
    }
  }

  class LongListFieldOfObjectWriter extends FieldOfObjectWriter {
    final GetRepeatedFieldSize getRepeatedFieldSize;
    final LongListElementGetter getListElement;

    ArrayWriter arrayWriter;
    LongWriter elementWriter;

    LongListFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.getRepeatedFieldSize = ProtobufJavaReflectionUtil.getRepeatedFieldSize(fieldDescriptor, objectClass);
      this.getListElement = ProtobufJavaReflectionUtil.getGetLongListElement(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.arrayWriter = (ArrayWriter) fieldWriter;
      this.elementWriter = (LongWriter) arrayWriter.fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
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

  class LongWriter extends FieldWriter {

    final void writeRawValuePrimitive(long value) {
      recordConsumer.addLong(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((long) value);
    }
  }

  class MapFieldOfObjectWriter extends FieldOfObjectWriter {
    final GetRepeatedFieldSize getRepeatedFieldSize;
    final ObjectValueGetter getMap;

    MapWriter mapWriter;
    FieldWriter keyWriter;
    FieldWriter valueWriter;

    MapFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.getRepeatedFieldSize = ProtobufJavaReflectionUtil.getRepeatedFieldSize(fieldDescriptor, objectClass);
      this.getMap = ProtobufJavaReflectionUtil.getGetObjectValue(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.mapWriter = (MapWriter) fieldWriter;
      this.keyWriter = mapWriter.keyWriter;
      this.valueWriter = mapWriter.valueWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
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

  class FloatFieldOfObjectWriter extends FieldOfObjectWriter {
    final FloatValueGetter getValue;
    final FieldHasValue fieldHasValue;

    FloatWriter fieldWriter;

    FloatFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.fieldHasValue = ProtobufJavaReflectionUtil.getHasValueOrNull(fieldDescriptor, objectClass);
      this.getValue = ProtobufJavaReflectionUtil.getGetFloatValue(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.fieldWriter = (FloatWriter) fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
      if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
        return;
      }
      fieldWriter.writeBeforeAll();
      fieldWriter.writeRawValuePrimitive(getValue.getValue(object));
      fieldWriter.writeAfterAll();
    }
  }

  class FloatListFieldOfObjectWriter extends FieldOfObjectWriter {
    final GetRepeatedFieldSize getRepeatedFieldSize;
    final FloatListElementGetter getListElement;

    ArrayWriter arrayWriter;
    FloatWriter elementWriter;

    FloatListFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.getRepeatedFieldSize = ProtobufJavaReflectionUtil.getRepeatedFieldSize(fieldDescriptor, objectClass);
      this.getListElement = ProtobufJavaReflectionUtil.getGetFloatListElement(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.arrayWriter = (ArrayWriter) fieldWriter;
      this.elementWriter = (FloatWriter) arrayWriter.fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
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

  class FloatWriter extends FieldWriter {

    final void writeRawValuePrimitive(float value) {
      recordConsumer.addFloat(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((float) value);
    }
  }

  class DoubleFieldOfObjectWriter extends FieldOfObjectWriter {
    final DoubleValueGetter getValue;
    final FieldHasValue fieldHasValue;

    DoubleWriter fieldWriter;

    DoubleFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.fieldHasValue = ProtobufJavaReflectionUtil.getHasValueOrNull(fieldDescriptor, objectClass);
      this.getValue = ProtobufJavaReflectionUtil.getGetDoubleValue(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.fieldWriter = (DoubleWriter) fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
      if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
        return;
      }
      fieldWriter.writeBeforeAll();
      fieldWriter.writeRawValuePrimitive(getValue.getValue(object));
      fieldWriter.writeAfterAll();
    }
  }

  class DoubleListFieldOfObjectWriter extends FieldOfObjectWriter {
    final GetRepeatedFieldSize getRepeatedFieldSize;
    final DoubleListElementGetter getListElement;

    ArrayWriter arrayWriter;
    DoubleWriter elementWriter;

    DoubleListFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.getRepeatedFieldSize = ProtobufJavaReflectionUtil.getRepeatedFieldSize(fieldDescriptor, objectClass);
      this.getListElement = ProtobufJavaReflectionUtil.getGetDoubleListElement(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.arrayWriter = (ArrayWriter) fieldWriter;
      this.elementWriter = (DoubleWriter) arrayWriter.fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
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

  class DoubleWriter extends FieldWriter {
    final void writeRawValuePrimitive(double value) {
      recordConsumer.addDouble(value);
    }

    @Override
    final void writeRawValue(Object value) {
      writeRawValuePrimitive((double) value);
    }
  }

  class EnumFieldOfObjectWriter extends FieldOfObjectWriter {
    final ObjectValueGetter getEnum;
    final IntValueGetter getEnumValue;
    final FieldHasValue fieldHasValue;
    final Descriptors.EnumDescriptor enumDescriptor;
    final List<Descriptors.EnumValueDescriptor> enumValues;

    EnumWriter fieldWriter;

    EnumFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.fieldHasValue = ProtobufJavaReflectionUtil.getHasValueOrNull(fieldDescriptor, objectClass);
      this.getEnum = ProtobufJavaReflectionUtil.getGetObjectValue(fieldDescriptor, objectClass);
      this.getEnumValue = ProtobufJavaReflectionUtil.getGetIntValue(fieldDescriptor, objectClass);
      this.enumDescriptor = fieldDescriptor.getEnumType();
      this.enumValues = enumDescriptor.getValues();
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.fieldWriter = (EnumWriter) fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
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
        enumValueDescriptor = enumDescriptor.findValueByNumberCreatingIfUnknown(getEnumValue.getValue(object));
      }
      fieldWriter.writeRawValue(enumValueDescriptor);

      fieldWriter.writeAfterAll();
    }
  }

  class EnumListFieldOfObjectWriter extends FieldOfObjectWriter {
    final GetRepeatedFieldSize getRepeatedFieldSize;
    final ObjectListElementGetter getEnumListElement;
    final IntListElementGetter getEnumValueListElement;
    final Descriptors.EnumDescriptor enumDescriptor;
    final List<Descriptors.EnumValueDescriptor> enumValues;

    ArrayWriter arrayWriter;
    EnumWriter elementWriter;

    EnumListFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.getRepeatedFieldSize = ProtobufJavaReflectionUtil.getRepeatedFieldSize(fieldDescriptor, objectClass);
      this.getEnumListElement = ProtobufJavaReflectionUtil.getGetObjectListElement(fieldDescriptor, objectClass);
      this.getEnumValueListElement =
          ProtobufJavaReflectionUtil.getGetIntListElement(fieldDescriptor, objectClass);
      this.enumDescriptor = fieldDescriptor.getEnumType();
      this.enumValues = enumDescriptor.getValues();
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.arrayWriter = (ArrayWriter) fieldWriter;
      this.elementWriter = (EnumWriter) arrayWriter.fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
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

  class BooleanFieldOfObjectWriter extends FieldOfObjectWriter {
    final BooleanValueGetter getValue;
    final FieldHasValue fieldHasValue;

    BooleanWriter fieldWriter;

    BooleanFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.fieldHasValue = ProtobufJavaReflectionUtil.getHasValueOrNull(fieldDescriptor, objectClass);
      this.getValue = ProtobufJavaReflectionUtil.getGetBooleanValue(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.fieldWriter = (BooleanWriter) fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
      if (fieldHasValue != null && !fieldHasValue.hasValue(object)) {
        return;
      }
      fieldWriter.writeBeforeAll();
      fieldWriter.writeRawValuePrimitive(getValue.getValue(object));
      fieldWriter.writeAfterAll();
    }
  }

  class BooleanListFieldOfObjectWriter extends FieldOfObjectWriter {
    final GetRepeatedFieldSize getRepeatedFieldSize;
    final BooleanListElementGetter getListElement;

    ArrayWriter arrayWriter;
    BooleanWriter elementWriter;

    BooleanListFieldOfObjectWriter(FieldDescriptor fieldDescriptor, Class<?> objectClass) throws Throwable {
      this.getRepeatedFieldSize = ProtobufJavaReflectionUtil.getRepeatedFieldSize(fieldDescriptor, objectClass);
      this.getListElement = ProtobufJavaReflectionUtil.getGetBooleanListElement(fieldDescriptor, objectClass);
    }

    @Override
    void setFieldWriter(FieldWriter fieldWriter) {
      this.arrayWriter = (ArrayWriter) fieldWriter;
      this.elementWriter = (BooleanWriter) arrayWriter.fieldWriter;
    }

    @Override
    void writeFieldOfObject(Object object) {
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
}
