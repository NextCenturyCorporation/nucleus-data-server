package com.ncc.neon.adapters.es;

import java.util.LinkedHashMap;
import java.util.Map;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.adapters.QueryAdapterFactory;
import com.ncc.neon.models.ConnectionInfo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@PropertySource(value="classpath:server.properties",ignoreResourceNotFound=true)
@Slf4j
public class ElasticsearchAdapterFactory implements QueryAdapterFactory {
    private Map<String, String> authCollection;

    public ElasticsearchAdapterFactory(final @Value("#{${elasticsearch.auth:{}}}") Map<String, String> authCollection) {
        if(authCollection == null) {
            this.authCollection = new LinkedHashMap<String, String>();
            log.debug("No elasticsearch.auth in server.properties file");
        }
        else {
            this.authCollection = authCollection;
            log.debug("Read elasticsearch.auth from server.properties file of size " + authCollection.size());
        }
    }

    @Override
    public String getName() {
        return "elasticsearchrest";
    }

    @Override
    public QueryAdapter initialize(ConnectionInfo cInfo) {
        String target = cInfo.getHost();
        String[] targetData = target.split("@");
        String[] hostData = targetData[(targetData.length > 1 ? 1 : 0)].split(":");
        String host = hostData[0];
        int port = hostData.length > 1 ? Integer.parseInt(hostData[1]) : 9200;
        String auth = targetData.length > 1 ? targetData[0] : (authCollection.containsKey(target) ? authCollection.get(target) : null);
        String authStatus = targetData.length > 1 ? "request" : (authCollection.containsKey(target) ? "configuration" : "no");
        String username = null;
        String password = null;
        if(auth != null) {
            String[] authData = auth.split(":");
            if(authData.length > 1) {
                username = authData[0];
                password = authData[1];
            }
        }
        log.debug("Initialize ES Adapter " + host + ":" + port + " with " + authStatus + " auth");
        return new ElasticsearchAdapter(host, port, username, password);
    }

}
