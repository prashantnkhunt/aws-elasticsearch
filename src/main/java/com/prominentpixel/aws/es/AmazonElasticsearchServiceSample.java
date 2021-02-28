package com.prominentpixel.aws.es;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.client.*;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;


import java.io.IOException;

public class AmazonElasticsearchServiceSample {
    private static String serviceName = "es";
    private static String region = "eu-central-1";
//    private static String aesEndpoint = "search-presser-spark-test-5mk6tfxyztwfzixsm6o5r66xbi.eu-central-1.es.amazonaws.com";
    private static String aesEndpoint = "vpc-aws-spark-es-test-pm6xkkjluhxyoe6mlf5t7mji6i.ap-south-1.es.amazonaws.com";
    private static String index = "my-index";
    private static String type = "_doc";
    private static String id = "1";


    static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    public static void main(String[] args) throws IOException {

//        getAllIndicesUsingHTTPInterceptorClient();
        String[] indices = getAllIndices();
        for (String index : indices) {
            System.out.println(index);
        }
        
//        RestHighLevelClient esClient = getHTTPRequestIntenceptorESClient(serviceName, region);
//        RestClient esClient = getHTTPRequestInterceptorESClient(serviceName, region);

        // Create the document as a hash map
       /* Map<String, Object> document = new HashMap<>();
        document.put("title", "Walk the Line");
        document.put("director", "James Mangold");
        document.put("year", "2005");

        // Form the indexing request, send it, and print the response
        IndexRequest request = new IndexRequest(index, type, id).source(document);
        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());*/
        /*String[] result = getAllIndices();

        for (String s : result) {
            System.out.println("I:"+s);
        }*/

    }

    // Adds the interceptor to the ES REST client
    public static RestHighLevelClient esClient(String serviceName, String region) {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(serviceName);
        signer.setRegionName(region);
        HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
        return new RestHighLevelClient(RestClient.builder(
                new HttpHost(aesEndpoint,443,"https"))
                .setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)));
    }

    public static RestClient getHTTPRequestInterceptorESClient(String serviceName, String region) {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(serviceName);
        signer.setRegionName(region);
        HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
        return RestClient.builder(
                HttpHost.create(aesEndpoint))
                .setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)).build();
    }

    public static String[] getAllIndices(){

        String[] result = null;
        try{
            RestHighLevelClient esClient = esClient(serviceName, region);
            GetIndexRequest request = new GetIndexRequest("*");
            GetIndexResponse response = esClient.indices().get(request, RequestOptions.DEFAULT);
            result = response.getIndices();
        }catch(Exception exception){
            exception.printStackTrace();
        }
        return result;
    }

    public static String[] getAllIndicesUsingHTTPInterceptorClient(){

        String[] result = null;
        try{
            RestClient esClient = getHTTPRequestInterceptorESClient(serviceName, region);

            Request request = new Request("GET",aesEndpoint);
            Response response = esClient.performRequest(request);
            System.out.println(response.getStatusLine().getStatusCode());
        }catch(Exception exception){
            exception.printStackTrace();
        }
        return result;
    }
}

