package com.demo.dataeng;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import com.google.rpc.Status;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BigTableSink implements Sink<BigTableSinkMessage> {

    private static final int  MAX_BATCHS_IN_PROGRESS = 10;

    private static final ObjectMapper _OM = new ObjectMapper();

    private Logger logger;
    private BigtableDataClient dataClient;
    private String tableName;
    private LinkedBlockingQueue<Record<BigTableSinkMessage>> queue;
    private ScheduledExecutorService executorService;
    private long lastWriteTime;
    private String credentialJsonString;
    private BigtableSession bigtableSession;
    private String fqtn;
    private Set<String> columnFamilies;
    private Integer mutationBatchSize;
    private Long timeBetweenWritesMilliseconds;
    private Long batchWriterPeriodicityMilliseconds;
    private Long batchWriterInitialDelayMilliseconds;
    private String userAgent;
    private BigtableTableAdminClient bigtableTableAdminClient;

    private AtomicInteger batchCounter = new AtomicInteger(0);

    private ExecutorService batchExecutingService = Executors.newFixedThreadPool(10);

    private ListeningExecutorService listeningBatchExecutorService = MoreExecutors.listeningDecorator(batchExecutingService);
    private String appProfileId;

    @Override
    public void open(Map<String, Object> config, SinkContext context) throws Exception {

        this.logger = context.getLogger();
        this.logger.info("Initializing sink");
        // Bigtable configs
        String projectId = (String) config.get("projectId");
        String instanceId = (String) config.get("instanceId");
        this.tableName = (String) config.get("tableName");
        this.appProfileId = (String) config.getOrDefault("appProfileId", "default");
        this.columnFamilies = Arrays.stream(((String) config.get("tableColumnFamilies")).split(",")).collect(Collectors.toSet());
        this.credentialJsonString = (String) config.get("credentialJsonString");
        boolean autoCreateTable = (boolean) config.getOrDefault("tableAutoCreate", false);
        boolean autoAlterTableColumnFamilies = (boolean) config.getOrDefault("tableAutoAlterColumnFamilies", false);
        this.mutationBatchSize = (int) config.getOrDefault("batchSize", 1000);
        this.timeBetweenWritesMilliseconds = (long) config.getOrDefault("maxTimeBetweenWritesMilliseconds", 5_000l);
        this.batchWriterPeriodicityMilliseconds = (long) config.getOrDefault("batchWriterPeriodicityMilliseconds", 2_000l);
        this.batchWriterInitialDelayMilliseconds = (long) config.getOrDefault("batchWriterInitialDelayMilliseconds", 0l);
        int threadPoolSize = (int) config.getOrDefault("threadPoolSize", 1);
        this.userAgent = (String) config.getOrDefault("userAgent", "Pulsar BigTable Sink");
        this.fqtn = "projects/"+projectId+"/instances/"+instanceId+"/tables/" + tableName;

        GoogleCredentials credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(credentialJsonString.getBytes(StandardCharsets.UTF_8)));

        BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .build();
        this.bigtableTableAdminClient = com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient.create(adminSettings);

        BigtableOptions opts = BigtableOptions
                .builder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .setAppProfileId(this.appProfileId)
                .setUserAgent(this.userAgent)
                .setRetryOptions(RetryOptions.builder().setRetryOnDeadlineExceeded(true).setEnableRetries(true).build())
                .setCredentialOptions(CredentialOptions.credential(credentials))
                .build() ;

        this.bigtableSession = new BigtableSession(opts);
        // Initialize data client
        this.dataClient = this.bigtableSession.getDataClient();
        Optional<Table> tbl = getTable(tableName);

        if (autoCreateTable && (!tbl.isPresent())) {
            CreateTableRequest createTableRequest = this.columnFamilies.stream().reduce(
                    CreateTableRequest.of(tableName),
                    (createTableReq, colFamily) -> createTableReq.addFamily(colFamily, GCRules.GCRULES.defaultRule()),
                    (createTableReq, createTableRequest2) -> createTableReq
            );
            this.logger.info("Creating table");
            bigtableTableAdminClient.createTable(createTableRequest);
        }
        Preconditions.checkState(getTable(tableName).isPresent());

        if(autoAlterTableColumnFamilies) {
            Set<String> currentColumnFamilies = tbl.get().getColumnFamilies().stream().map(cf -> cf.getId()).collect(Collectors.toSet());
            Set<String> columnFamiliesToDelete = currentColumnFamilies.stream().filter(cf -> !this.columnFamilies.contains(cf)).collect(Collectors.toSet());
            Set<String> columnFamiliesToAdd = this.columnFamilies.stream().filter(cf -> !currentColumnFamilies.contains(cf)).collect(Collectors.toSet());
            if (columnFamiliesToDelete.size() > 0 || columnFamiliesToAdd.size() > 0) {
                ModifyColumnFamiliesRequest mcfr = ModifyColumnFamiliesRequest.of(this.tableName);
                columnFamiliesToDelete.stream().forEach(cf -> mcfr.dropFamily(cf));
                columnFamiliesToAdd.stream().forEach(cf -> mcfr.addFamily(cf, GCRules.GCRULES.defaultRule()));
                bigtableTableAdminClient.modifyFamilies(mcfr);
            }
        }

        this.logger.info("Table ready :: " + tableName);

        // Initialize the queue and executor service
        this.queue = new LinkedBlockingQueue<>();
        this.executorService = Executors.newScheduledThreadPool(threadPoolSize);
        this.lastWriteTime = System.currentTimeMillis();

        this.executorService.scheduleAtFixedRate(
                this::asyncWriteBatchToBigtable,
                this.batchWriterInitialDelayMilliseconds,
                this.batchWriterPeriodicityMilliseconds,
                TimeUnit.MILLISECONDS);
        this.logger.info("Sinkr open");
    }

    @Override
    public void write(Record<BigTableSinkMessage> record) {
        this.queue.offer(record);
        this.logger.debug("current batch count " + batchCounter.get() + ", current queue size " + queue.size());
    }
    private Optional<Table> getTable(String tableName) {
        try {

            return Optional.ofNullable(this.bigtableTableAdminClient.getTable(tableName));
        } catch(com.google.api.gax.rpc.NotFoundException e){
            e.printStackTrace();
            return Optional.empty();
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static BiFunction<String, Map.Entry<String, String>, Mutation> CREATE_UPSERT_MUTATION = (columnFamily, columnEntry) -> Mutation
            .newBuilder()
            .setSetCell(
                    Mutation.SetCell
                            .newBuilder()
                            .setFamilyName(columnFamily)
                            .setColumnQualifier(ByteString.copyFromUtf8(columnEntry.getKey()))
                            .setValue(ByteString.copyFromUtf8(columnEntry.getValue()))
                            .setTimestampMicros(System.currentTimeMillis() * 1000)
                            .build()
            )
            .build();
    private static BiFunction<String, Map.Entry<String, String>, Mutation> CREATE_DELETE_MUTATION = (columnFamily, columnEntry) ->
            Mutation
                .newBuilder()
                .setDeleteFromColumn(
                        Mutation.DeleteFromColumn.newBuilder()
                                .setFamilyName(columnFamily)
                                .setColumnQualifier(ByteString.copyFromUtf8(columnEntry.getKey()))
                ).build();
    private static BiFunction<String,Map.Entry<String, String>, Mutation> CREATE_MUTATION = (columnFamily, columnEntry) -> {
        if(columnEntry.getValue() != null){
            return CREATE_UPSERT_MUTATION.apply(columnFamily,columnEntry);
        } else {
            return CREATE_DELETE_MUTATION.apply(columnFamily,columnEntry);
        }
    };
    private static Function<Record<BigTableSinkMessage>, MutateRowsRequest.Entry> PULSAR_MSG_TO_MUTATE_ROWS_REQUEST_ENTRY = record ->
            record.getValue().getRow()
                .entrySet()
                .stream()
                .reduce(
                    MutateRowsRequest.Entry.newBuilder().setRowKey(ByteString.copyFromUtf8(record.getValue().getRowKey())),
                    (builder, columnFamilyEntry) -> {
                        String columnFamily = columnFamilyEntry.getKey();
                        columnFamilyEntry
                            .getValue()
                            .entrySet()
                            .forEach(columnEntry ->
                                builder.addMutations(CREATE_MUTATION.apply(columnFamily, columnEntry))
                            );
                        return builder;
                    },
                    (builder, na) -> builder
                ).build();


    private void asyncWriteBatchToBigtable() {
        if (this.queue.size() >= this.mutationBatchSize || (System.currentTimeMillis() - this.lastWriteTime) >= this.timeBetweenWritesMilliseconds) {
            if(batchCounter.intValue() < MAX_BATCHS_IN_PROGRESS) {
                if (this.queue.size() > 0) {
                    batchCounter.incrementAndGet();
                    List<Record<BigTableSinkMessage>> records = new ArrayList<>();
                    this.queue.drainTo(records, this.mutationBatchSize);
                    this.logger.info("Writing batch of size " + records.size());
                    MutateRowsRequest requests = records
                            .stream()
                            .map(PULSAR_MSG_TO_MUTATE_ROWS_REQUEST_ENTRY)
                            .reduce(
                                    MutateRowsRequest.newBuilder().setTableName(fqtn),
                                    (builder, rowMutationEntry) -> builder.addEntries(rowMutationEntry),
                                    (builder, builder2) -> builder
                            )
                            .build();

                    ListenableFuture<List<MutateRowsResponse>> listListenableFuture = dataClient.mutateRowsAsync(requests);
                    records.forEach(bigTableSinkMessageRecord -> bigTableSinkMessageRecord.ack());

                    Futures.addCallback(
                            listListenableFuture,
                            new FutureCallback<List<MutateRowsResponse>>() {
                                public void onSuccess(List<MutateRowsResponse> responses) {
                                    batchCounter.decrementAndGet();
                                    AtomicInteger i = new AtomicInteger(0);
                                    responses.stream().map(r -> Pair.of(i.getAndIncrement(), r)).forEach(
                                            pair -> {
                                                boolean allRowMutationsSuccess = pair.getRight().getEntriesList().stream().reduce(
                                                        Boolean.FALSE,
                                                        (aBoolean, entry) -> aBoolean || (entry.getStatus().getCode() == Code.OK_VALUE),
                                                        (aBoolean, aBoolean2) -> aBoolean && aBoolean2
                                                );
//                                                doAck(allRowMutationsSuccess, records.get(pair.getKey()));
                                            }
                                    );
                                }
                                private void doAck(boolean allRowMutationsSuccess, Record<?> stringRecord) {
                                    if(allRowMutationsSuccess){
                                        stringRecord.ack();
                                    } else {
                                        stringRecord.fail();
                                    }
                                }

                                public void onFailure(Throwable t) {
                                    // Handle failure
                                    batchCounter.decrementAndGet();
                                    t.printStackTrace();
                                }
                            },
                            listeningBatchExecutorService
                    );
                }
                else{
                    this.logger.info("Queue is empty");
                }
                this.lastWriteTime = System.currentTimeMillis();
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.logger.info("Shutting down sink");
        // Shut down the executor service
        this.executorService.shutdown();
        // Shut down the executor service for BigTable batch request
        this.batchExecutingService.shutdown();
        this.batchExecutingService.awaitTermination(60, TimeUnit.SECONDS);
        // Shut down the listening service for BigTable batch responses
        this.listeningBatchExecutorService.shutdown();
        this.listeningBatchExecutorService.awaitTermination(60, TimeUnit.SECONDS);
        // Close the Bigtable session
        this.bigtableSession.close();
        this.logger.info("Closed Sink");
    }
}