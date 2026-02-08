package com.rustwebsvc.reqwest;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public final class ReqwestClient {
    private static final Executor DEFAULT_EXECUTOR = Executors.newCachedThreadPool();

    static {
        System.loadLibrary("rustwebsvc");
    }

    public String get(String url) {
        return getNative(require(url, "url"));
    }

    public String post(String url, String body, String contentType) {
        return postNative(require(url, "url"), require(body, "body"), require(contentType, "contentType"));
    }

    public String put(String url, String body, String contentType) {
        return putNative(require(url, "url"), require(body, "body"), require(contentType, "contentType"));
    }

    public String multipart(
            String url,
            String[] fieldNames,
            String[] fieldValues,
            String[] fileFieldNames,
            String[] filePaths
    ) {
        require(url, "url");
        ensureArrayPair(fieldNames, fieldValues, "fieldNames", "fieldValues");
        ensureArrayPair(fileFieldNames, filePaths, "fileFieldNames", "filePaths");
        return multipartNative(url, fieldNames, fieldValues, fileFieldNames, filePaths);
    }

    public CompletableFuture<String> getAsync(String url) {
        return getAsync(url, DEFAULT_EXECUTOR);
    }

    public CompletableFuture<String> getAsync(String url, Executor executor) {
        return CompletableFuture.supplyAsync(() -> get(url), executor);
    }

    public CompletableFuture<String> postAsync(String url, String body, String contentType) {
        return postAsync(url, body, contentType, DEFAULT_EXECUTOR);
    }

    public CompletableFuture<String> postAsync(String url, String body, String contentType, Executor executor) {
        return CompletableFuture.supplyAsync(() -> post(url, body, contentType), executor);
    }

    public CompletableFuture<String> putAsync(String url, String body, String contentType) {
        return putAsync(url, body, contentType, DEFAULT_EXECUTOR);
    }

    public CompletableFuture<String> putAsync(String url, String body, String contentType, Executor executor) {
        return CompletableFuture.supplyAsync(() -> put(url, body, contentType), executor);
    }

    public CompletableFuture<String> multipartAsync(
            String url,
            String[] fieldNames,
            String[] fieldValues,
            String[] fileFieldNames,
            String[] filePaths
    ) {
        return multipartAsync(url, fieldNames, fieldValues, fileFieldNames, filePaths, DEFAULT_EXECUTOR);
    }

    public CompletableFuture<String> multipartAsync(
            String url,
            String[] fieldNames,
            String[] fieldValues,
            String[] fileFieldNames,
            String[] filePaths,
            Executor executor
    ) {
        return CompletableFuture.supplyAsync(
                () -> multipart(url, fieldNames, fieldValues, fileFieldNames, filePaths),
                executor
        );
    }

    private static String require(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        return value;
    }

    private static void ensureArrayPair(
            String[] left,
            String[] right,
            String leftName,
            String rightName
    ) {
        Objects.requireNonNull(left, leftName + " must not be null");
        Objects.requireNonNull(right, rightName + " must not be null");
        if (left.length != right.length) {
            throw new IllegalArgumentException(leftName + " and " + rightName + " must have the same length");
        }
    }

    private static native String getNative(String url);

    private static native String postNative(String url, String body, String contentType);

    private static native String putNative(String url, String body, String contentType);

    private static native String multipartNative(
            String url,
            String[] fieldNames,
            String[] fieldValues,
            String[] fileFieldNames,
            String[] filePaths
    );
}
