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

package org.apache.carbondata.core.datastore.impl;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import jersey.repackaged.com.google.common.collect.AbstractSequentialIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.apache.hadoop.util.Progressable;
import org.joda.time.Duration;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.toArray;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempFile;
import static java.util.Objects.requireNonNull;
import static jdk.nashorn.internal.ir.debug.ObjectSizeCalculator.getObjectSize;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.*;
import static org.apache.http.HttpStatus.*;

public class CarbonS3FileSystem extends FileSystem {
    private static final Duration BACKOFF_MIN_SLEEP = Duration.standardSeconds(1);
    private static LogService log =
            LogServiceFactory.getLogService(CarbonS3FileSystem.class.getCanonicalName());
    private final TransferManagerConfiguration transferConfig = new TransferManagerConfiguration();
    private URI uri;
    private Path workingDirectory;
    private AmazonS3 s3;
    private File stagingDirectory;
    private int maxAttempts;
    private Duration maxBackoffTime;
    private Duration maxRetryTime;
    private boolean useInstanceCredentials;
    private boolean pinS3ClientToCurrentRegion;
    private boolean sseEnabled;
    private CarbonS3SseType sseType;

    public enum CarbonS3SseType {
        KMS, S3
    }

    private static String keyFromPath(Path path) {
        checkArgument(path.isAbsolute(), "Path is not absolute: %s", path);
        String key = nullToEmpty(path.toUri().getPath());
        if (key.startsWith(PATH_SEPARATOR)) {
            key = key.substring(PATH_SEPARATOR.length());
        }
        if (key.endsWith(PATH_SEPARATOR)) {
            key = key.substring(0, key.length() - PATH_SEPARATOR.length());
        }
        return key;
    }

    private static long lastModifiedTime(ObjectMetadata metadata) {
        Date date = metadata.getLastModified();
        return (date != null) ? date.getTime() : 0;
    }

    private static boolean keysEqual(Path p1, Path p2) {
        return keyFromPath(p1).equals(keyFromPath(p2));
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        requireNonNull(uri, "uri is null");
        requireNonNull(conf, "conf is null");
        super.initialize(uri, conf);
        setConf(conf);

        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDirectory =
                new Path(PATH_SEPARATOR).makeQualified(this.uri, new Path(PATH_SEPARATOR));

        CarbonProperties defaults = CarbonProperties.getInstance();
        conf.set(S3_ACCESS_KEY, defaults.getProperty(S3_ACCESS_KEY));
        conf.set(S3_SECRET_KEY, defaults.getProperty(S3_SECRET_KEY));
        this.stagingDirectory =
                new File(conf.get(S3_STAGING_DIRECTORY, defaults.getProperty(S3_STAGING_DIRECTORY)));
        this.maxAttempts = conf.getInt(S3_MAX_CLIENT_RETRIES,
                Integer.parseInt(defaults.getProperty(S3_MAX_CLIENT_RETRIES))) + 1;
        this.maxBackoffTime = Duration.millis(1000);
        this.maxRetryTime = Duration.millis(1000);
        int maxErrorRetries = conf.getInt(S3_MAX_ERROR_RETRIES,
                Integer.parseInt(defaults.getProperty(S3_MAX_ERROR_RETRIES)));
        boolean sslEnabled =
                conf.getBoolean(S3_SSL_ENABLED, Boolean.getBoolean(defaults.getProperty(S3_SSL_ENABLED)));
        Duration connectTimeout = Duration.standardSeconds(30);
        Duration socketTimeout = Duration.standardSeconds(30);
        int maxConnections = 10;
        int minFileSize = 320000000;
        long minPartSize = 320000000;
        this.useInstanceCredentials = conf.getBoolean(S3_USE_INSTANCE_CREDENTIALS,
                Boolean.getBoolean(defaults.getProperty(S3_USE_INSTANCE_CREDENTIALS)));
        this.pinS3ClientToCurrentRegion = conf.getBoolean(S3_PIN_CLIENT_TO_CURRENT_REGION,
                Boolean.getBoolean(defaults.getProperty(S3_PIN_CLIENT_TO_CURRENT_REGION)));
        this.sseEnabled =
                conf.getBoolean(S3_SSE_ENABLED, Boolean.getBoolean(defaults.getProperty(S3_SSE_ENABLED)));
        this.sseType = CarbonS3SseType.valueOf(conf.get(S3_SSE_TYPE, CarbonS3SseType.S3.name()));
        String userAgentPrefix =
                conf.get(S3_USER_AGENT_PREFIX, defaults.getProperty(S3_USER_AGENT_PREFIX));

        ClientConfiguration configuration = new ClientConfiguration().withMaxErrorRetry(maxErrorRetries)
                .withProtocol(sslEnabled ? Protocol.HTTPS : Protocol.HTTP)
                .withConnectionTimeout(toIntExact(connectTimeout.getMillis()))
                .withSocketTimeout(toIntExact(socketTimeout.getMillis())).withMaxConnections(maxConnections);

        AWSCredentialsProviderChain credentials = new AWSCredentialsProviderChain(
                new BasicAWSCredentialsProvider(conf.get(S3_ACCESS_KEY), conf.get(S3_SECRET_KEY)),
                new InstanceProfileCredentialsProvider(),
                new AnonymousAWSCredentialsProvider()
        );

        this.s3 = new AmazonS3Client(credentials, configuration);
        transferConfig.setMultipartUploadThreshold(minFileSize);
        transferConfig.setMinimumUploadPartSize(minPartSize);
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        return new FSDataInputStream(new BufferedFSInputStream(
                new CarbonS3InputStream(s3, uri.getHost(), path, maxAttempts, maxBackoffTime, maxRetryTime),
                bufferSize));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progressable) throws IOException {
        if ((!overwrite) && exists(path)) {
            throw new IOException("File already exists:" + path);
        }

        if (!stagingDirectory.exists()) {
            createDirectories(stagingDirectory.toPath());
        }
        if (!stagingDirectory.isDirectory()) {
            throw new IOException("Configured staging path is not a directory: " + stagingDirectory);
        }
        File tempFile = createTempFile(stagingDirectory.toPath(), "carbon-s3-", ".tmp").toFile();

        String key = keyFromPath(qualifiedPath(path));
        return new FSDataOutputStream(
                new CarbonS3OutputStream(s3, transferConfig, uri.getHost(), key, tempFile, sseEnabled,
                        sseType), statistics);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable) throws IOException {
        try {
            String key = keyFromPath(qualifiedPath(path));
            if (!stagingDirectory.exists()) {
                createDirectories(stagingDirectory.toPath());
            }
            if (!stagingDirectory.isDirectory()) {
                throw new IOException("Configured staging path is not a directory: " + stagingDirectory);
            }
            File tempFile = createTempFile(stagingDirectory.toPath(), "carbon-s3-", ".tmp").toFile();

            if (exists(path)) {
                GetObjectRequest getObjectRequest = new GetObjectRequest(uri.getHost(), key);
                S3Object s3object = s3.getObject(getObjectRequest);
                InputStream stream = s3object.getObjectContent();
                byte[] content = new byte[bufferSize];

                BufferedOutputStream outputStream =
                        new BufferedOutputStream(new FileOutputStream(tempFile));
                int totalSize = 0;
                int bytesRead;
                while ((bytesRead = stream.read(content)) != -1) {
                    System.out.println(String.format("%d bytes read from stream", bytesRead));
                    outputStream.write(content, 0, bytesRead);
                    totalSize += bytesRead;
                }
                System.out.println("Total Size of file in bytes = " + totalSize);
                outputStream.close();
                return new FSDataOutputStream(
                        new CarbonS3OutputStream(s3, transferConfig, uri.getHost(), key, tempFile, sseEnabled,
                                sseType), statistics);
            } else {
                return new FSDataOutputStream(
                        new CarbonS3OutputStream(s3, transferConfig, uri.getHost(), key, tempFile, sseEnabled,
                                sseType), statistics);
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        boolean srcDirectory;
        try {
            srcDirectory = directory(src);
        } catch (FileNotFoundException e) {
            return false;
        }

        try {
            if (!directory(dst)) {
                // cannot copy a file to an existing file
                deleteObject(keyFromPath(dst));
                //   return keysEqual(src, dst);
            } else {
                // move source under destination directory
                dst = new Path(dst, src.getName());
            }
        } catch (FileNotFoundException e) {
            // destination does not exist
        }

        if (keysEqual(src, dst)) {
            return true;
        }

        if (srcDirectory) {
            for (FileStatus file : listStatus(src)) {
                rename(file.getPath(), new Path(dst, file.getPath().getName()));
            }
            deleteObject(keyFromPath(src) + DIRECTORY_SUFFIX);
        } else {
            s3.copyObject(uri.getHost(), keyFromPath(src), uri.getHost(), keyFromPath(dst));
            delete(src, true);
        }

        return true;
    }

    private boolean directory(Path path) throws IOException {
        return getFileStatus(path).isDirectory();
    }

    private boolean deleteObject(String key) {
        try {
            s3.deleteObject(uri.getHost(), key);
            return true;
        } catch (AmazonClientException e) {
            return false;
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        try {
            if (!directory(path)) {
                return deleteObject(keyFromPath(path));
            }
        } catch (FileNotFoundException e) {
            return false;
        }

        if (!recursive) {
            throw new IOException("Directory " + path + " is not empty");
        }

        for (FileStatus file : listStatus(path)) {
            delete(file.getPath(), true);
        }
        deleteObject(keyFromPath(path) + DIRECTORY_SUFFIX);

        return true;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
// STATS.newListStatusCall();
        List<LocatedFileStatus> list = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = listLocatedStatus(path);
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return toArray(list, LocatedFileStatus.class);
    }

    @Override
    public void setWorkingDirectory(Path path) {
        workingDirectory = path;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDirectory;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        // no need to do anything for S3
        return true;
    }

    @Override public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (s3 instanceof AmazonS3Client) {
                ((AmazonS3Client) s3).shutdown();
            }
        }
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        if (path.getName().isEmpty()) {
            // the bucket root requires special handling
            if (getS3ObjectMetadata(path) != null) {
                return new FileStatus(0, true, 1, 0, 0, qualifiedPath(path));
            }
            throw new FileNotFoundException("File does not exist: " + path);
        }

        ObjectMetadata metadata = getS3ObjectMetadata(path);

        if (metadata == null) {
            // check if this path is a directory
            Iterator<LocatedFileStatus> iterator = listPrefix(path);
            if (iterator.hasNext()) {
                return new FileStatus(0, true, 1, 0, 0, qualifiedPath(path));
            }
            throw new FileNotFoundException("File does not exist: " + path);
        }

        return new FileStatus(getObjectSize(metadata), false, 1, BLOCK_SIZE, lastModifiedTime(metadata),
                qualifiedPath(path));
    }

    private Iterator<LocatedFileStatus> listPrefix(Path path) {
        String key = keyFromPath(path);
        if (!key.isEmpty()) {
            key += PATH_SEPARATOR;
        }

        ListObjectsRequest request =
                new ListObjectsRequest().withBucketName(uri.getHost()).withPrefix(key)
                        .withDelimiter(PATH_SEPARATOR);

        Iterator<ObjectListing> listings =
                new AbstractSequentialIterator<ObjectListing>(s3.listObjects(request)) {
                    @Override
                    protected ObjectListing computeNext(ObjectListing previous) {
                        if (!previous.isTruncated()) {
                            return null;
                        }
                        return s3.listNextBatchOfObjects(previous);
                    }
                };

        return Iterators.concat(Iterators.transform(listings, this::statusFromListing));
    }

    private Iterator<LocatedFileStatus> statusFromListing(ObjectListing listing) {
        return Iterators.concat(statusFromPrefixes(listing.getCommonPrefixes()),
                statusFromObjects(listing.getObjectSummaries()));
    }

    private Iterator<LocatedFileStatus> statusFromPrefixes(List<String> prefixes) {
        List<LocatedFileStatus> list = new ArrayList<>();
        for (String prefix : prefixes) {
            Path path = qualifiedPath(new Path(PATH_SEPARATOR + prefix));
            FileStatus status = new FileStatus(0, true, 1, 0, 0, path);
            list.add(createLocatedFileStatus(status));
        }
        return list.iterator();
    }

    private Iterator<LocatedFileStatus> statusFromObjects(List<S3ObjectSummary> objects) {
        // NOTE: for encrypted objects, S3ObjectSummary.size() used below is NOT correct,
        // however, to get the correct size we'd need to make an additional request to get
        // user metadata, and in this case it doesn't matter.
        return objects.stream().filter(object -> !object.getKey().endsWith(PATH_SEPARATOR)).map(
                object -> new FileStatus(object.getSize(), false, 1, BLOCK_SIZE,
                        object.getLastModified().getTime(),
                        qualifiedPath(new Path(PATH_SEPARATOR + object.getKey()))))
                .map(this::createLocatedFileStatus).iterator();
    }

    private Path qualifiedPath(Path path) {
        return path.makeQualified(this.uri, getWorkingDirectory());
    }

    private LocatedFileStatus createLocatedFileStatus(FileStatus status) {
        try {
            BlockLocation[] fakeLocation = getFileBlockLocations(status, 0, status.getLen());
            return new LocatedFileStatus(status, fakeLocation);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @VisibleForTesting
    ObjectMetadata getS3ObjectMetadata(Path path) throws IOException {
        try {
            //  STATS.newMetadataCall();
            return s3.getObjectMetadata(uri.getHost(), keyFromPath(path));
        } catch (RuntimeException e) {
            // STATS.newGetMetadataError();
            if (e instanceof AmazonS3Exception) {
                switch (((AmazonS3Exception) e).getStatusCode()) {
                    case SC_NOT_FOUND:
                        return null;
                    case SC_FORBIDDEN:
                    case SC_BAD_REQUEST:
                        throw new AmazonS3Exception(" Unrecoverable S3 Operation Exception", e);
                }
            }
            throw Throwables.propagate(e);
        }

    }

    public static class CarbonS3InputStream extends FSInputStream {
        private final AmazonS3 s3;
        private final String host;
        private final Path path;
        private final int maxAttempts;
        private final Duration maxBackoffTime;
        private final Duration maxRetryTime;

        private boolean closed;
        private InputStream in;
        private long streamPosition;
        private long nextReadPosition;

        public CarbonS3InputStream(AmazonS3 s3, String host, Path path, int maxAttempts,
                                   Duration maxBackoffTime, Duration maxRetryTime) {
            this.s3 = requireNonNull(s3, "s3 is null");
            this.host = requireNonNull(host, "host is null");
            this.path = requireNonNull(path, "path is null");

            checkArgument(maxAttempts >= 0, "maxAttempts cannot be negative");
            this.maxAttempts = maxAttempts;
            this.maxBackoffTime = requireNonNull(maxBackoffTime, "maxBackoffTime is null");
            this.maxRetryTime = requireNonNull(maxRetryTime, "maxRetryTime is null");
        }

        @Override
        public void close() {
            closed = true;
            closeStream();
        }

        private void closeStream() {
            if (in != null) {
                try {
                    if (in instanceof S3ObjectInputStream) {
                        ((S3ObjectInputStream) in).abort();
                    } else {
                        in.close();
                    }
                } catch (Exception ex) {
                    // thrown if the current thread is in the interrupted state
                }
                in = null;
            }
        }

        @Override
        public void seek(long pos) throws IOException {
            checkState(!closed, "already closed");
            checkArgument(pos >= 0, "position is negative: %s", pos);

            // this allows a seek beyond the end of the stream but the next read will fail
            nextReadPosition = pos;
        }

        @Override
        public long getPos() throws IOException {
            return nextReadPosition;
        }

        @Override
        public boolean seekToNewSource(long l) throws IOException {
            return false;
        }

        @Override
        public int read() throws IOException {
// This stream is wrapped with BufferedInputStream, so this method should never be called
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int bytesRead;
            try {
                seekStream();
                bytesRead = in.read(buffer, offset, length);
                if (bytesRead != -1) {
                    streamPosition += bytesRead;
                    nextReadPosition += bytesRead;
                }
            } catch (Exception e) {
                // STATS.newReadError(e);
                closeStream();
                throw e;
            }
            return bytesRead;

        }

        private void seekStream() throws IOException {
            if ((in != null) && (nextReadPosition == streamPosition)) {
                // already at specified position
                return;
            }

            if ((in != null) && (nextReadPosition > streamPosition)) {
                // seeking forwards
                long skip = nextReadPosition - streamPosition;
                if (skip <= max(in.available(), MAX_SKIP_SIZE)) {
                    // already buffered or seek is small enough
                    try {
                        if (in.skip(skip) == skip) {
                            streamPosition = nextReadPosition;
                            return;
                        }
                    } catch (IOException ignored) {
                        // will retry by re-opening the stream
                    }
                }
            }

            // close the stream and open at desired position
            streamPosition = nextReadPosition;
            closeStream();
            openStream();
        }

        private void openStream() throws IOException {
            if (in == null) {
                in = openStream(path, nextReadPosition);
                streamPosition = nextReadPosition;
            }
        }

        private InputStream openStream(Path path, long start) throws IOException {

            try {
                // boolean objectExists = s3.doesObjectExist(host, keyFromPath(path));
                // if(objectExists) {
                GetObjectRequest request =
                        new GetObjectRequest(host, keyFromPath(path)).withRange(start, Long.MAX_VALUE);
                return s3.getObject(request).getObjectContent();
      /*  } else {
          ObjectListing objectListing = s3.listObjects(path.toUri().getPath());
           String key = objectListing.getObjectSummaries().get(0).getKey();
          GetObjectRequest request =
              new GetObjectRequest(host, key).withRange(start, Long.MAX_VALUE);
          return s3.getObject(request).getObjectContent();
        }*/
            } catch (RuntimeException e) {
                if (e instanceof AmazonS3Exception) {
                    switch (((AmazonS3Exception) e).getStatusCode()) {
                        case SC_REQUESTED_RANGE_NOT_SATISFIABLE:
                            // ignore request for start past end of object
                            return new ByteArrayInputStream(new byte[0]);
                        case SC_FORBIDDEN:
                        case SC_NOT_FOUND:
                        case SC_BAD_REQUEST:
                            throw e;
                    }
                }
                throw Throwables.propagate(e);
            }
        }

    }

    public static class CarbonS3OutputStream extends FilterOutputStream {

        private final TransferManager transferManager;
        private final String host;
        private final String key;
        private final File tempFile;
        private final boolean sseEnabled;
        private final CarbonS3SseType sseType;

        private boolean closed;

        public CarbonS3OutputStream(AmazonS3 s3, TransferManagerConfiguration config, String host,
                                    String key, File tempFile, boolean sseEnabled, CarbonS3SseType sseType)
                throws IOException {
            super(new BufferedOutputStream(
                    new FileOutputStream(requireNonNull(tempFile, "tempFile is null"))));

            transferManager = new TransferManager(requireNonNull(s3, "s3 is null"));
            transferManager.setConfiguration(requireNonNull(config, "config is null"));

            this.host = requireNonNull(host, "host is null");
            this.key = requireNonNull(key, "key is null");
            this.tempFile = tempFile;
            this.sseEnabled = sseEnabled;
            this.sseType = requireNonNull(sseType, "sseType is null");

            log.debug("OutputStream for key = " + key + " using file: " + tempFile);
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;

            try {
                super.close();
                uploadObject();
            } finally {
                if (!tempFile.delete()) {
                    log.warn(String.format("Could not delete temporary file: %s", tempFile));
                }
                // close transfer manager but keep underlying S3 client open
                transferManager.shutdownNow(false);
            }
        }

        private void uploadObject() throws IOException {
            try {
                log.debug(String
                        .format("Starting upload for host: %s, key: %s, file: %s, size: %s", host, key,
                                tempFile, tempFile.length()));
                // STATS.uploadStarted();

                PutObjectRequest request = new PutObjectRequest(host, key, tempFile);

                Upload upload = transferManager.upload(request);

                upload.waitForCompletion();
                // STATS.uploadSuccessful();
                log.debug(String.format("Completed upload for host: %s, key: %s", host, key));
            } catch (AmazonClientException e) {
                // STATS.uploadFailed();
                throw new IOException(e);
            } catch (InterruptedException e) {
                // STATS.uploadFailed();
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
        }
    }

}
