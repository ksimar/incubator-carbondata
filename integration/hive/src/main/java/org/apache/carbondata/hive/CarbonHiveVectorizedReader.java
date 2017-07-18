/*
* Licensed to the Apache Software Foundation (ASF) under one or more
        * contributor license agreements.  See the NOTICE file distributed with
        * this work for additional information regarding copyright ownership.
        * The ASF licenses this file to You under the Apache License, Version 2.0
        * (the "License"); you may not use this file except in compliance with
        * the License.  You may obtain a copy of the License at
        *
        *    http://www.apache.org/licenses/LICENSE-2.0
        *
        * Unless required by applicable law or agreed to in writing, software
        * distributed under the License is distributed on an "AS IS" BASIS,
        * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        * See the License for the specific language governing permissions and
        * limitations under the License.
        */
        package org.apache.carbondata.hive;

        import java.io.IOException;
        import java.util.ArrayList;
        import java.util.Arrays;
        import java.util.List;
        import java.util.Map;

        import org.apache.carbondata.core.cache.dictionary.Dictionary;
        import org.apache.carbondata.core.datastore.block.TableBlockInfo;
        import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
        import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
        import org.apache.carbondata.core.metadata.datatype.DataType;
        import org.apache.carbondata.core.metadata.encoder.Encoding;
        import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
        import org.apache.carbondata.core.scan.executor.QueryExecutor;
        import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
        import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
        import org.apache.carbondata.core.scan.model.QueryDimension;
        import org.apache.carbondata.core.scan.model.QueryMeasure;
        import org.apache.carbondata.core.scan.model.QueryModel;
        import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
        import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
        import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
        import org.apache.carbondata.core.stats.QueryStatistic;
        import org.apache.carbondata.core.stats.QueryStatisticsConstants;
        import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
        import org.apache.carbondata.core.util.CarbonUtil;
        import org.apache.carbondata.spark.util.CarbonScalaUtil;
        import org.apache.carbondata.spark.vectorreader.ColumnarVectorWrapper;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
        import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
        import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
        import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
        import org.apache.hadoop.hive.ql.exec.vector.VectorColumnAssign;
        import org.apache.hadoop.hive.ql.exec.vector.VectorColumnAssignFactory;
        import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
        import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
        import org.apache.hadoop.hive.ql.metadata.HiveException;
        import org.apache.hadoop.hive.serde.serdeConstants;
        import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
        import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
        import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
        import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
        import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
        import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
        import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
        import org.apache.hadoop.io.ArrayWritable;
        import org.apache.hadoop.io.NullWritable;
        import org.apache.hadoop.io.Writable;
        import org.apache.hadoop.mapred.FileSplit;
        import org.apache.hadoop.mapred.InputSplit;
        import org.apache.hadoop.mapred.JobConf;
        import org.apache.hadoop.mapred.RecordReader;
        import org.apache.spark.memory.MemoryMode;
        import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
        import org.apache.spark.sql.types.DecimalType;
        import org.apache.spark.sql.types.StructField;
        import org.apache.spark.sql.types.StructType;

public class CarbonHiveVectorizedReader
        implements RecordReader<NullWritable, VectorizedRowBatch> {

  private ArrayWritable valueObj ;
  private CarbonObjectInspector objInspector;
  private long recordReaderCounter = 0;
  private int[] columnIds;
  private VectorizedRowBatchCtx rbCtx;
  private long rowReadCount = 0;
  private int batchIdx = 0;

  private int numBatched = 0;

  //private ColumnarBatch columnarBatch;

  private VectorizedRowBatch columnarBatch;


  private CarbonColumnarBatch carbonColumnarBatch;

  private int rowCount = 0;

  private VectorColumnAssign[] assigners;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  /**
   * The default config on whether columnarBatch should be offheap.
   */
  private static final MemoryMode DEFAULT_MEMORY_MODE = MemoryMode.OFF_HEAP;

  protected QueryModel queryModel;

  protected AbstractDetailQueryResultIterator iterator;

  protected QueryExecutor queryExecutor;


  public CarbonHiveVectorizedReader(QueryModel queryModel, InputSplit inputSplit, JobConf jobConf)
          throws IOException, InterruptedException, UnsupportedOperationException {
    initialize(inputSplit, jobConf, queryModel);
    try {
      rbCtx = new VectorizedRowBatchCtx();
      rbCtx.init(jobConf, (FileSplit) inputSplit);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  private void initialize(InputSplit inputSplit, Configuration conf, QueryModel queryModel)
          throws IOException, InterruptedException, UnsupportedOperationException {
    // The input split can contain single HDFS block or multiple blocks, so firstly get all the
    // blocks and then set them in the query model.
    List<CarbonHiveInputSplit> splitList;
    if (inputSplit instanceof CarbonHiveInputSplit) {
      splitList = new ArrayList<>(1);
      splitList.add((CarbonHiveInputSplit) inputSplit);
    } else {
      throw new RuntimeException("unsupported input split type: " + inputSplit);
    }
    List<TableBlockInfo> tableBlockInfoList = CarbonHiveInputSplit.createBlocks(splitList);
    queryModel.setTableBlockInfos(tableBlockInfoList);
    queryModel.setVectorReader(true);
    this.queryModel = queryModel;
    try {
      queryExecutor = QueryExecutorFactory.getQueryExecutor(queryModel);
      iterator = (AbstractDetailQueryResultIterator) queryExecutor.execute(queryModel);
    } catch (QueryExecutionException e) {
      throw new InterruptedException(e.getMessage());
    }

    final TypeInfo rowTypeInfo;
    final List<String> columnNames;
    List<TypeInfo> columnTypes;
    // Get column names and sort order
    final String colIds = conf.get("hive.io.file.readcolumn.ids");
    final String columnNameProperty = conf.get(serdeConstants.LIST_COLUMNS);
    final String columnTypeProperty = conf.get(serdeConstants.LIST_COLUMN_TYPES);

    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    if (valueObj == null) {
      valueObj = new ArrayWritable(Writable.class, new Writable[columnTypes.size()]);
    }

    if (!colIds.equals("")) {
      String[] arraySelectedColId = colIds.split(",");
      List<TypeInfo> reqColTypes = new ArrayList<TypeInfo>();
      columnIds = new int[arraySelectedColId.length];
      int columnId = 0;
      for (int j = 0; j < arraySelectedColId.length; j++) {
        columnId = Integer.parseInt(arraySelectedColId[j]);
        columnIds[j] = columnId;
      }
    }

    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    this.objInspector = new CarbonObjectInspector((StructTypeInfo) rowTypeInfo);
  }



  @Override public boolean next(NullWritable key, VectorizedRowBatch outputBatch) throws IOException {
    try {
      if (assigners != null) {
        assert(outputBatch.numCols == assigners.length);
      }
      outputBatch.reset();
      int maxSize = outputBatch.getMaxSize();

      if (nextKeyValue()) {
        Object obj = getCurrentValue();
        while (outputBatch.size < maxSize) {
          // if (false == internalReader.next(internalKey, internalValues)) {
          //  outputBatch.endOfFile = true;
          //  break;
          // }
          Writable[] writables = null; //internalValues.get();

          if (null == assigners) {
            // Normally we'd build the assigners from the rbCtx.rowOI, but with Parquet
            // we have a discrepancy between the metadata type (Eg. tinyint -> BYTE) and
            // the writable value (IntWritable). see Parquet's ETypeConverter class.
            assigners = VectorColumnAssignFactory.buildAssigners(outputBatch, writables);
          }

          for (int i = 0; i < writables.length; ++i) {
            assigners[i].assignObjectValue(writables[i], outputBatch.size);
          }
          ++outputBatch.size;
        }
        return outputBatch.size > 0;
      }
    }catch (HiveException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return outputBatch.size > 0;

  }

  @Override public NullWritable createKey() {
    return null;
  }

  @Override public VectorizedRowBatch createValue() {
    VectorizedRowBatch outputBatch = null;
    try {
      outputBatch = rbCtx.createVectorizedRowBatch();
      //internalValues = internalReader.createValue();
    } catch (HiveException e) {
      throw new RuntimeException("Error creating a batch", e);
    }
    return outputBatch;
  }

  @Override public long getPos() throws IOException {
    return rowReadCount;
  }



  @Override public void close() throws IOException {
    logStatistics(rowCount, queryModel.getStatisticsRecorder());
    if (columnarBatch != null) {
      columnarBatch.reset();
      columnarBatch = null;
    }
    // clear dictionary cache
    Map<String, Dictionary> columnToDictionaryMapping = queryModel.getColumnToDictionaryMapping();
    if (null != columnToDictionaryMapping) {
      for (Map.Entry<String, Dictionary> entry : columnToDictionaryMapping.entrySet()) {
        CarbonUtil.clearDictionaryCache(entry.getValue());
      }
    }
    try {
      queryExecutor.finish();
    } catch (QueryExecutionException e) {
      throw new IOException(e);
    }
  }

  public boolean nextKeyValue() throws IOException, InterruptedException {
    resultBatch();

    if (returnColumnarBatch) return nextBatch();

    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  public Object getCurrentValue() throws IOException, InterruptedException {
    if (returnColumnarBatch) {
      rowCount += columnarBatch.count();
      return columnarBatch;
    }
    rowCount += 1;
    return columnarBatch.projectedColumns;
  }

  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override public float getProgress() throws IOException {
    // TODO : Implement it based on total number of rows it is going to retrive.
    return 0;
  }



  private void initBatch(MemoryMode memMode) {
    List<QueryDimension> queryDimension = queryModel.getQueryDimension();
    List<QueryMeasure> queryMeasures = queryModel.getQueryMeasures();
    StructField[] fields = new StructField[queryDimension.size() + queryMeasures.size()];
    for (int i = 0; i < queryDimension.size(); i++) {
      QueryDimension dim = queryDimension.get(i);
      if (dim.getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        DirectDictionaryGenerator generator = DirectDictionaryKeyGeneratorFactory
                .getDirectDictionaryGenerator(dim.getDimension().getDataType());
        fields[dim.getQueryOrder()] = new StructField(dim.getColumnName(),
                CarbonScalaUtil.convertCarbonToSparkDataType(generator.getReturnType()), true, null);
      } else if (!dim.getDimension().hasEncoding(Encoding.DICTIONARY)) {
        fields[dim.getQueryOrder()] = new StructField(dim.getColumnName(),
                CarbonScalaUtil.convertCarbonToSparkDataType(dim.getDimension().getDataType()), true,
                null);
      } else if (dim.getDimension().isComplex()) {
        fields[dim.getQueryOrder()] = new StructField(dim.getColumnName(),
                CarbonScalaUtil.convertCarbonToSparkDataType(dim.getDimension().getDataType()), true,
                null);
      } else {
        fields[dim.getQueryOrder()] = new StructField(dim.getColumnName(),
                CarbonScalaUtil.convertCarbonToSparkDataType(DataType.INT), true, null);
      }
    }

    for (int i = 0; i < queryMeasures.size(); i++) {
      QueryMeasure msr = queryMeasures.get(i);
      switch (msr.getMeasure().getDataType()) {
        case SHORT:
        case INT:
        case LONG:
          fields[msr.getQueryOrder()] = new StructField(msr.getColumnName(),
                  CarbonScalaUtil.convertCarbonToSparkDataType(msr.getMeasure().getDataType()), true,
                  null);
          break;
        case DECIMAL:
          fields[msr.getQueryOrder()] = new StructField(msr.getColumnName(),
                  new DecimalType(msr.getMeasure().getPrecision(),
                          msr.getMeasure().getScale()), true, null);
          break;
        default:
          fields[msr.getQueryOrder()] = new StructField(msr.getColumnName(),
                  CarbonScalaUtil.convertCarbonToSparkDataType(DataType.DOUBLE), true, null);
      }
    }

    try {
      columnarBatch = createVectorizedRowBatch(objInspector);
    } catch (HiveException e) {
      e.printStackTrace();
    }
    CarbonColumnVector[] vectors = new CarbonColumnVector[fields.length];
    boolean[] filteredRows = new boolean[columnarBatch.getMaxSize()];
    for (int i = 0; i < fields.length; i++) {
      vectors[i] = new CarbonColumnarVectorWrapper(columnarBatch.cols[i], filteredRows);
    }
    carbonColumnarBatch = new CarbonColumnarBatch(vectors, columnarBatch.getMaxSize(), filteredRows);
  }

  private void initBatch() {
    initBatch(DEFAULT_MEMORY_MODE);
  }

  private VectorizedRowBatch resultBatch() {
    if (columnarBatch == null) initBatch();
    return columnarBatch;
  }

  /*
   * Can be called before any rows are returned to enable returning columnar batches directly.
   */
  private void enableReturningBatches() {
    returnColumnarBatch = true;
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  private boolean nextBatch() {
    columnarBatch.reset();
    carbonColumnarBatch.reset();
    if (iterator.hasNext()) {
      iterator.processNextBatch(carbonColumnarBatch);
      int actualSize = carbonColumnarBatch.getActualSize();
      numBatched = actualSize;
      batchIdx = 0;
      return true;
    }
    return false;
  }


  /**
   * This method will log query result count and querytime
   * @param recordCount
   * @param recorder
   */
  public void logStatistics(int recordCount, QueryStatisticsRecorder recorder) {
    // result size
    QueryStatistic queryStatistic = new QueryStatistic();
    queryStatistic.addCountStatistic(QueryStatisticsConstants.RESULT_SIZE, recordCount);
    recorder.recordStatistics(queryStatistic);
    // print executor query statistics for each task_id
    recorder.logStatisticsAsTableExecutor();
  }

  /**
   * Creates a Vectorized row batch and the column vectors.
   *
   * @return VectorizedRowBatch
   * @throws HiveException
   */
  public VectorizedRowBatch createVectorizedRowBatch( CarbonObjectInspector carbonObjectInspector) throws HiveException
  {
    final List<? extends org.apache.hadoop.hive.serde2.objectinspector.StructField> fieldRefs = carbonObjectInspector.getAllStructFieldRefs();
    VectorizedRowBatch result = new VectorizedRowBatch(fieldRefs.size());
    for (int j = 0; j < fieldRefs.size(); j++) {
      switch (fieldRefs.get(j).getFieldObjectInspector().getTypeName()) {
        case "int" :
        case "long" :
        case "short":
        case "date":
        case "timestamp":
          result.cols[j] = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
          break;
        case "float":
        case "double":
          result.cols[j] = new DoubleColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
          break;
        case "string":
        case "char":
          result.cols[j] = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
          break;

      }
      // If the column is included in the include list or if the column is a
      // partition column then create the column vector. Also note that partition columns are not
      // in the included list.
     /* ObjectInspector foi = fieldRefs.get(j).getFieldObjectInspector();
        switch (foi.getCategory()) {
          case PRIMITIVE: {
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector) foi;
            // Vectorization currently only supports the following data types:
            // BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, BINARY, STRING, CHAR, VARCHAR, TIMESTAMP,
            // DATE and DECIMAL
            switch (poi.getPrimitiveCategory()) {
              case BOOLEAN:
              case BYTE:
              case SHORT:
              case INT:
              case LONG:
              case TIMESTAMP:
              case DATE:
              case INTERVAL_YEAR_MONTH:
              case INTERVAL_DAY_TIME:
                result.cols[j] = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
                break;
              case FLOAT:
              case DOUBLE:
                result.cols[j] = new DoubleColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
                break;
              case BINARY:
              case STRING:
              case CHAR:
              case VARCHAR:
                result.cols[j] = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
                break;
              case DECIMAL:
                DecimalTypeInfo tInfo = (DecimalTypeInfo) poi.getTypeInfo();
                result.cols[j] = new DecimalColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
                    tInfo.precision(), tInfo.scale());
                break;
              default:
                throw new RuntimeException("Vectorizaton is not supported for datatype:"
                    + poi.getPrimitiveCategory());
            }
            break;
          }
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
            throw new HiveException("Vectorizaton is not supported for datatype:"
                + foi.getCategory());
          default:
            throw new HiveException("Unknown ObjectInspector category!");
        }*/

    }
    result.numCols = fieldRefs.size();
    result.reset();
    return result;
  }


}