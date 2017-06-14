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

package org.apache.carbondata.flink;

import com.google.gson.Gson;
import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.*;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriterImpl;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfo;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfoPreparator;
import org.apache.carbondata.hadoop.util.SchemaReader;
import org.apache.carbondata.processing.api.dataloader.SchemaInfo;
import org.apache.carbondata.processing.constants.TableOptionConstant;
import org.apache.carbondata.processing.csvload.BlockDetails;
import org.apache.carbondata.processing.csvload.CSVInputFormat;
import org.apache.carbondata.processing.csvload.CSVRecordReaderIterator;
import org.apache.carbondata.processing.csvload.StringArrayWritable;
import org.apache.carbondata.processing.model.CarbonDataLoadSchema;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.newflow.DataLoadExecutor;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CarbondataStoreCreator {
    private final static Logger LOGGER = Logger.getLogger(CarbondataStoreCreator.class.getName());
    private final static String MAX_COLUMNS = "2000";
    private static int DECIMAL_SCALE = 10;
    private static int DECIMAL_PRECISION = 20;

    public void createCarbonStore(AbsoluteTableIdentifier absoluteTableIdentifier, String columnString, String[]
            columnNames, String[] columnTypes, String factFilePath, String[] dimensionColumns) {
        try {
            CarbonProperties.getInstance().addProperty(CarbonCommonConstants.STORE_LOCATION_HDFS,
                    absoluteTableIdentifier.getStorePath());

            CarbonTable table;
            if (checkIfTableExists(absoluteTableIdentifier)) {
                LOGGER.warning("Table Already Exists" + absoluteTableIdentifier.getCarbonTableIdentifier()
                        .getTableUniqueName());
                table = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier);
            } else {
                LOGGER.info("Creating Table" + absoluteTableIdentifier.getCarbonTableIdentifier()
                        .getTableUniqueName());
                table = createTable(absoluteTableIdentifier, columnNames, columnTypes, dimensionColumns);
            }
            CarbonLoadModel loadModel = initializeLoadModel(table, absoluteTableIdentifier, factFilePath, columnString);
            writeDictionary(factFilePath, table, absoluteTableIdentifier, dimensionColumns);
            executeGraph(loadModel, absoluteTableIdentifier.getStorePath());
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    private boolean checkIfTableExists(AbsoluteTableIdentifier absoluteTableIdentifier) throws IOException {
        CarbonTablePath carbonTablePath = CarbonStorePath.getCarbonTablePath(absoluteTableIdentifier);
        String schemaFilePath = carbonTablePath.getSchemaFilePath();
        return FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.LOCAL) ||
                FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.HDFS) ||
                FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.VIEWFS);
    }

    private CarbonTable createTable(AbsoluteTableIdentifier absoluteTableIdentifier, String[] columnNames, String[]
            columnTypes, String[] dimensionColumns) throws Exception {
        TableInfo tableInfo = configureTableInfo(absoluteTableIdentifier, columnNames, columnTypes, dimensionColumns);
        CarbonTablePath carbonTablePath = CarbonStorePath
                .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
                        absoluteTableIdentifier.getCarbonTableIdentifier());
        String schemaFilePath = carbonTablePath.getSchemaFilePath();
        String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath);
        tableInfo.setMetaDataFilepath(schemaMetadataPath);

        CarbonMetadata.getInstance().loadTableMetadata(tableInfo);
        SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
        org.apache.carbondata.format.TableInfo thriftTableInfo = schemaConverter.fromWrapperToExternalTableInfo(tableInfo,
                tableInfo.getDatabaseName(), tableInfo.getFactTable().getTableName());
        org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry =
                new org.apache.carbondata.format.SchemaEvolutionEntry(tableInfo.getLastUpdatedTime());
        thriftTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history()
                .add(schemaEvolutionEntry);

        FileFactory.FileType fileType = FileFactory.getFileType(schemaMetadataPath);
        if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
            FileFactory.mkdirs(schemaMetadataPath, fileType);
        }

        ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
        thriftWriter.open();
        thriftWriter.write(thriftTableInfo);
        thriftWriter.close();
        return CarbonMetadata.getInstance().getCarbonTable(tableInfo.getTableUniqueName());
    }

    private TableInfo configureTableInfo(AbsoluteTableIdentifier absoluteTableIdentifier, String[] columnNames,
                                         String[] columnTypes, String[] dimensionColumns) throws Exception {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setStorePath(absoluteTableIdentifier.getStorePath());
        tableInfo.setDatabaseName(absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName());
        TableSchema tableSchema = new TableSchema();
        tableSchema.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
        List<ColumnSchema> columnSchemas = new ArrayList();

        for (int i = 0; i < columnNames.length; i++) {
            DataType type = convertType(columnTypes[i]);
            String colName = columnNames[i];
            ColumnSchema column = new ColumnSchema();
            column.setColumnName(colName);
            column.setColumnar(true);
            column.setDataType(type);
            column.setColumnUniqueId(UUID.randomUUID().toString());
            if (isDimensionColumn(dimensionColumns, colName)) {
                column.setDimensionColumn(true);
            } else {
                column.setDimensionColumn(false);
            }
            if (column.getDataType() == org.apache.carbondata.core.metadata.datatype.DataType.DECIMAL) {
                column.setPrecision(DECIMAL_PRECISION);
                column.setScale(DECIMAL_SCALE);
            }
            setEncodings(column, type);
            column.setColumnGroup(i + 1);
            columnSchemas.add(column);
        }

        tableSchema.setListOfColumns(columnSchemas);
        SchemaEvolution schemaEvol = new SchemaEvolution();
        schemaEvol.setSchemaEvolutionEntryList(new ArrayList<SchemaEvolutionEntry>());
        tableSchema.setSchemaEvalution(schemaEvol);
        tableSchema.setTableId(UUID.randomUUID().toString());
        tableInfo.setTableUniqueName(
                absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName() + "_"
                        + absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
        tableInfo.setLastUpdatedTime(System.currentTimeMillis());
        tableInfo.setFactTable(tableSchema);
        tableInfo.setAggregateTableList(new ArrayList<TableSchema>());
        return tableInfo;
    }

    private void writeDictionary(String factFilePath, CarbonTable table, AbsoluteTableIdentifier absoluteTableIdentifier, String[] dimensionColumns) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(factFilePath));
        List<CarbonColumn> allCols = new ArrayList<CarbonColumn>();
        List<CarbonDimension> dimensions = table.getDimensionByTableName(table.getFactTableName());
        allCols.addAll(dimensions);
        List<CarbonMeasure> measures = table.getMeasureByTableName(table.getFactTableName());
        allCols.addAll(measures);

        Set<String>[] dimensionSet = createDimensionDataSet(dimensions, dimensionColumns, reader, factFilePath);
        // writeDictionaryToFile
        for (int i = 0; i < dimensionSet.length; i++) {
            org.apache.carbondata.core.cache.dictionary.Dictionary dictionary = null;
            DataType dimensionDatatype = dimensions.get(i).getDataType();
            ColumnIdentifier columnIdentifier = new ColumnIdentifier(dimensions.get(i).getColumnId(), null, null);
            CarbonDictionaryWriter writer = new CarbonDictionaryWriterImpl(absoluteTableIdentifier.getStorePath(),
                    absoluteTableIdentifier.getCarbonTableIdentifier(), columnIdentifier);
            if(!isDictionaryFileExists(absoluteTableIdentifier, columnIdentifier, table)) {
                LOGGER.info("------ Dictionary file does not exist : First Time Load ------");
                for (String value : dimensionSet[i]) {
                    writer.write(value);
                }
                writer.close();
                writer.commit();
                Cache <DictionaryColumnUniqueIdentifier, org.apache.carbondata.core.cache.dictionary.Dictionary > dictCache =
                        CacheProvider.getInstance().createCache(CacheType.REVERSE_DICTIONARY, absoluteTableIdentifier.getStorePath());
                dictionary = dictCache.get(new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier
                        .getCarbonTableIdentifier(), columnIdentifier, dimensionDatatype));
            } else {
                LOGGER.info("------ Dictionary file exists : Multi-Load Scenario ------");
                for (String value : dimensionSet[i]) {
                    dictionary = getDictionary(table.getCarbonTableIdentifier(), columnIdentifier, absoluteTableIdentifier
                            .getStorePath(), dimensionDatatype);
                    if (dictionary.getSurrogateKey(value) == CarbonCommonConstants.INVALID_SURROGATE_KEY) {
                        writer.write(value);
                    }
                }
                writer.close();
                writer.commit();
            }
            // SortIndexWriter
            writeSortIndex(absoluteTableIdentifier, columnIdentifier, dimensionDatatype , dictionary);
        }
        reader.close();
    }

    private org.apache.carbondata.core.cache.dictionary.Dictionary getDictionary(
            CarbonTableIdentifier tableIdentifier, ColumnIdentifier columnIdentifier, String
            carbonStorePath, DataType dataType) throws IOException {
        DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
                new DictionaryColumnUniqueIdentifier(tableIdentifier, columnIdentifier, dataType);
        Cache<DictionaryColumnUniqueIdentifier, Dictionary> dictCache =
                CacheProvider.getInstance().createCache(CacheType.REVERSE_DICTIONARY, carbonStorePath);
        return dictCache.get(dictionaryColumnUniqueIdentifier);
    }

    private boolean isDictionaryFileExists(AbsoluteTableIdentifier absoluteTableIdentifier, ColumnIdentifier
            columnIdentifier, CarbonTable table) throws IOException {
        String storeLocation = absoluteTableIdentifier.getStorePath();
        String databaseName = table.getDatabaseName();
        String tableName = table.getTableUniqueName().replace(databaseName+ "_", "");
        String dictionaryPath = storeLocation + "/" + databaseName + "/" + tableName + "/Metadata/" + columnIdentifier.getColumnId() + ".dict";
        return CarbonUtil.isFileExists(dictionaryPath);
    }

    private void writeSortIndex(AbsoluteTableIdentifier absoluteTableIdentifier, ColumnIdentifier columnIdentifier,
                                DataType dimensionDatatype,Dictionary dictionary) throws IOException {
        CarbonDictionarySortInfoPreparator preparator = new CarbonDictionarySortInfoPreparator();
        List<String> newDistinctValues = new ArrayList<>();
        CarbonDictionarySortInfo dictionarySortInfo = preparator.getDictionarySortInfo(newDistinctValues, dictionary,
                dimensionDatatype);
        CarbonDictionarySortIndexWriter carbonDictionaryWriter = new CarbonDictionarySortIndexWriterImpl(
                absoluteTableIdentifier.getCarbonTableIdentifier(), columnIdentifier,
                absoluteTableIdentifier.getStorePath());
        try {
            carbonDictionaryWriter.writeSortIndex(dictionarySortInfo.getSortIndex());
            carbonDictionaryWriter.writeInvertedSortIndex(dictionarySortInfo.getSortIndexInverted());
        } finally {
            carbonDictionaryWriter.close();
        }
    }

    private CarbonLoadModel initializeLoadModel(CarbonTable table, AbsoluteTableIdentifier absoluteTableIdentifier,
                                                String factFilePath, String columnString) {
        CarbonDataLoadSchema schema = new CarbonDataLoadSchema(table);
        CarbonLoadModel loadModel = new CarbonLoadModel();
        loadModel.setCarbonDataLoadSchema(schema);
        loadModel.setDatabaseName(absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName());
        loadModel.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
        loadModel.setFactFilePath(factFilePath);
        loadModel.setLoadMetadataDetails(new ArrayList<LoadMetadataDetails>());
        loadModel.setStorePath(absoluteTableIdentifier.getStorePath());
        loadModel.setDateFormat(null);
        loadModel.setDefaultTimestampFormat(CarbonProperties.getInstance().getProperty(
                CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
        loadModel.setDefaultDateFormat(null);
        loadModel.setSerializationNullFormat(TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName() + "," + "\\N");
        loadModel.setBadRecordsLoggerEnable(TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName() + "," + "false");
        loadModel.setBadRecordsAction(TableOptionConstant.BAD_RECORDS_ACTION.getName() + "," + "FAIL");
        loadModel.setIsEmptyDataBadRecord(DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + "," + "false");
        loadModel.setCsvHeader(columnString);
        loadModel.setCsvHeaderColumns(loadModel.getCsvHeader().split(","));
        loadModel.setTaskNo("0");
        loadModel.setSegmentId("0");
        loadModel.setPartitionId("0");
        loadModel.setFactTimeStamp(System.currentTimeMillis());
        loadModel.setMaxColumns(MAX_COLUMNS);
        return loadModel;
    }

    public void executeGraph(CarbonLoadModel loadModel, String storeLocation) throws Exception {
        String outPutLoc = storeLocation + "/etl";
        String databaseName = loadModel.getDatabaseName();
        String tableName = loadModel.getTableName();
        addCarbonProperties(databaseName, tableName, storeLocation, outPutLoc);
        String graphPath = outPutLoc + File.separator + loadModel.getDatabaseName() + File.separator + tableName
                + File.separator + 0 + File.separator + 1 + File.separator + tableName + ".ktr";
        File path = new File(graphPath);
        if (path.exists()) {
            path.delete();
        }

        SchemaInfo info = new SchemaInfo();
        BlockDetails blockDetails = new BlockDetails(new Path(loadModel.getFactFilePath()),
                0, new File(loadModel.getFactFilePath()).length(), new String[]{"localhost"});
        Configuration configuration = new Configuration();
        CSVInputFormat format = getCsvInputFormat(configuration, loadModel);
        TaskAttemptContextImpl hadoopAttemptContext = new TaskAttemptContextImpl(configuration,
                new TaskAttemptID("", 1, TaskType.MAP, 0, 0));
        RecordReader<NullWritable, StringArrayWritable> recordReader = format.createRecordReader(blockDetails,
                hadoopAttemptContext);

        CSVRecordReaderIterator readerIterator = new CSVRecordReaderIterator(recordReader, blockDetails,
                hadoopAttemptContext);
        FileFactory.FileType storeFileType = FileFactory.getFileType(storeLocation);

        if (storeFileType.equals(FileFactory.FileType.HDFS)) {
            new DataLoadExecutor().execute(loadModel,
                    "/tmp" + '/' + System.nanoTime(),                                    //tmp location
                    new CarbonIterator[]{readerIterator});
        } else {
            new DataLoadExecutor().execute(loadModel, storeLocation, new CarbonIterator[]{readerIterator});
        }

        info.setDatabaseName(databaseName);
        info.setTableName(tableName);
        writeLoadMetadata(loadModel.getCarbonDataLoadSchema(), new ArrayList<LoadMetadataDetails>());
        performFactFilesRenamingAndCleanUp(storeFileType, storeLocation, databaseName, tableName);
    }

    public void writeLoadMetadata(CarbonDataLoadSchema schema, List<LoadMetadataDetails> listOfLoadFolderDetails)
            throws IOException {
        LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
        loadMetadataDetails.setLoadEndTime(System.currentTimeMillis());
        loadMetadataDetails.setLoadStatus("SUCCESS");
        loadMetadataDetails.setLoadName(String.valueOf(0));
        loadMetadataDetails.setLoadStartTime(loadMetadataDetails.getTimeStamp(readCurrentTime()));
        listOfLoadFolderDetails.add(loadMetadataDetails);

        String dataLoadLocation = schema.getCarbonTable().getMetaDataFilepath() + File.separator
                + CarbonCommonConstants.LOADMETADATA_FILENAME;

        DataOutputStream dataOutputStream;
        Gson gsonObjectToWrite = new Gson();
        BufferedWriter brWriter = null;

        AtomicFileOperations writeOperation =
                new AtomicFileOperationsImpl(dataLoadLocation, FileFactory.getFileType(dataLoadLocation));

        try {
            dataOutputStream = writeOperation.openForWrite(FileWriteOperation.OVERWRITE);
            brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
                    Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
            String metadataInstance = gsonObjectToWrite.toJson(listOfLoadFolderDetails.toArray());
            brWriter.write(metadataInstance);
        } finally {
            try {
                if (null != brWriter) {
                    brWriter.flush();
                }
            } catch (Exception exception) {
                throw exception;
            }
            CarbonUtil.closeStreams(brWriter);
        }
        writeOperation.close();
    }

    private DataType convertType(String type) throws Exception {
        switch (type.toLowerCase()) {
            case "int":
                return DataType.INT;
            case "char":
            case "string":
                return DataType.STRING;
            case "double":
                return DataType.DOUBLE;
            case "float":
                return DataType.DOUBLE;
            case "timestamp":
                return DataType.TIMESTAMP;
            case "date":
                return DataType.DATE;
            case "boolean":
                return DataType.BOOLEAN;
            case "bigint":
            case "long":
                return DataType.LONG;
            default:
                if (type.toLowerCase().startsWith("decimal")) {
                    try {
                        String precisionAndScaleStr = type.toLowerCase().substring(8, type.length() - 1);
                        String[] precisionAndScale = precisionAndScaleStr.split(",");
                        DECIMAL_PRECISION = Integer.parseInt(precisionAndScale[0]);
                        DECIMAL_SCALE = Integer.parseInt(precisionAndScale[1]);
                    } catch (ArrayIndexOutOfBoundsException exception) {
                        throw new Exception("Decimal Scale or precision is not specified");
                    }
                    return DataType.DECIMAL;
                } else {
                    return DataType.NULL;
                }
        }
    }

    private boolean isDimensionColumn(String[] dimensionColumns, String column) {
        boolean isDimension = false;
        for (int iterator = 0; iterator < dimensionColumns.length; iterator++) {
            if (dimensionColumns[iterator].equals(column))
                isDimension = true;
        }
        return isDimension;
    }

    private void setEncodings(ColumnSchema column, DataType type) {
        switch (type.toString().toLowerCase()) {
            case "int":
            case "float":
            case "long":
            case "double":
                ArrayList<Encoding> emptyEncodings = new ArrayList();
                column.setEncodingList(emptyEncodings);
                break;
            case "char":
            case "string":
                ArrayList<Encoding> encodings = new ArrayList();
                encodings.add(Encoding.DICTIONARY);
                encodings.add(Encoding.INVERTED_INDEX);
                column.setEncodingList(encodings);
                break;
            case "date":
            case "timestamp":
                ArrayList<Encoding> dateEncodings = new ArrayList();
                dateEncodings.add(Encoding.DICTIONARY);
                dateEncodings.add(Encoding.DIRECT_DICTIONARY);
                dateEncodings.add(Encoding.INVERTED_INDEX);
                column.setEncodingList(dateEncodings);
                break;
            default:
                ArrayList<Encoding> emptyEncoding = new ArrayList<>();
                column.setEncodingList(emptyEncoding);
        }
    }

    private int[] getDimensionsHeadersIndex(String[] headerSplit, String[] dimensionColumns) {
        if (headerSplit == null || dimensionColumns == null) {
            LOGGER.log(Level.WARNING, "Either of dimension Columns or headers is null");
            return null;
        } else {
            int[] dimensionIndexValues = new int[dimensionColumns.length];
            int count = 0;

            for (int dimColumnIndex = 0; dimColumnIndex < dimensionColumns.length; dimColumnIndex++) {
                String dimColumn = dimensionColumns[dimColumnIndex];
                for (int headerIndex = 0; headerIndex < headerSplit.length; headerIndex++) {
                    if (dimColumn.equals(headerSplit[headerIndex])) {
                        dimensionIndexValues[count++] = headerIndex;
                    }
                }
            }
            return dimensionIndexValues;
        }
    }

    private Set<String>[] createDimensionDataSet(List<CarbonDimension> dimensions, String[] dimensionColumns,
                                                 BufferedReader reader, String factFilePath) throws IOException {
        String header = reader.readLine();
        String[] headerSplit = header.split(",");

        Set<String>[] set = new HashSet[dimensions.size()];
        for (int i = 0; i < set.length; i++) {
            set[i] = new HashSet<>();
        }

        int[] dimensionHeaderIndex = getDimensionsHeadersIndex(headerSplit, dimensionColumns);
        if (dimensionHeaderIndex != null) {
            int count = 0;
            for (int iterator = 0; iterator < dimensionHeaderIndex.length; iterator++) {
                String line = reader.readLine();
                while (line != null) {
                    String[] data = line.split(",");
                    set[count].add(data[dimensionHeaderIndex[iterator]]);
                    line = reader.readLine();
                }
                count++;
                reader = new BufferedReader(new FileReader(factFilePath));
            }
        }
        return set;
    }

    private CSVInputFormat getCsvInputFormat(Configuration configuration, CarbonLoadModel loadModel) {
        CSVInputFormat.setCommentCharacter(configuration, loadModel.getCommentChar());
        CSVInputFormat.setCSVDelimiter(configuration, loadModel.getCsvDelimiter());
        CSVInputFormat.setEscapeCharacter(configuration, loadModel.getEscapeChar());
        CSVInputFormat.setHeaderExtractionEnabled(configuration, true);
        CSVInputFormat.setQuoteCharacter(configuration, loadModel.getQuoteChar());
        CSVInputFormat.setReadBufferSize(configuration, CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE,
                        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT));
        CSVInputFormat.setNumberOfColumns(configuration, String.valueOf(loadModel.getCsvHeaderColumns().length));
        CSVInputFormat.setMaxColumns(configuration, MAX_COLUMNS);
        CSVInputFormat format = new CSVInputFormat();
        return format;
    }

    private void addCarbonProperties(String databaseName, String tableName, String storeLocation, String outputLoc) {
        String tempLocationKey = databaseName + '_' + tableName + "_1";
        CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation);
        CarbonProperties.getInstance().addProperty("store_output_location", outputLoc);
        CarbonProperties.getInstance().addProperty("send.signal.load", "false");
        CarbonProperties.getInstance().addProperty("carbon.is.columnar.storage", "true");
        CarbonProperties.getInstance().addProperty("carbon.dimension.split.value.in.columnar", "1");
        CarbonProperties.getInstance().addProperty("carbon.is.fullyfilled.bits", "true");
        CarbonProperties.getInstance().addProperty("is.int.based.indexer", "true");
        CarbonProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true");
        CarbonProperties.getInstance().addProperty("high.cardinality.value", "100000");
        CarbonProperties.getInstance().addProperty("is.compressed.keyblock", "false");
        CarbonProperties.getInstance().addProperty("carbon.leaf.node.size", "120000");
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd");
    }

    private void performFactFilesRenamingAndCleanUp(FileFactory.FileType storeFileType, String storeLocation,
                                                    String databaseName, String tableName) throws IOException,
            InterruptedException {
        if (storeFileType.equals(FileFactory.FileType.LOCAL)) {
            String segLocation = storeLocation + "/" + databaseName + "/" + tableName + "/Fact/Part0/Segment_0";
            File file = new File(segLocation);
            File factFile = null;
            File[] folderList = file.listFiles();
            File folder = null;
            for (int i = 0; i < folderList.length; i++) {
                if (folderList[i].isDirectory()) {
                    folder = folderList[i];
                }
            }
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();
                for (int i = 0; i < files.length; i++) {
                    if (!files[i].isDirectory() && files[i].getName().startsWith("part")) {
                        factFile = files[i];
                        break;
                    }
                }
                factFile.renameTo(new File(segLocation + "/" + factFile.getName()));
                CarbonUtil.deleteFoldersAndFiles(folder);
            }
        }
    }

    public String readCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
        return sdf.format(new Date());
    }
}
