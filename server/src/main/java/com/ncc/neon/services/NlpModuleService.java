package com.ncc.neon.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.better.IENlpModule;
import com.ncc.neon.better.NlpModule;
import com.ncc.neon.better.PreprocessorNlpModule;
import com.ncc.neon.models.NlpModuleModel;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.HashMap;

/*
This class is responsible for deserializing NLP modules from the database to NLP Module POJOs.
 */
@Component
public class NlpModuleService {
    private HashMap<String, NlpModule> nlpModuleCache;
    private NlpModuleModel[] nlpModules;
    private QueryService queryService;
    private final String moduleIndex = "module";
    private final String moduleDataType = "module";
    private final RestHighLevelClient elasticSearchClient;

    @Autowired
    private Environment env;
    @Autowired
    private PreprocessorNlpModule preprocessorNlpModule;
    @Autowired
    private IENlpModule ieNlpModule;

    public NlpModuleService() {
        this.nlpModuleCache = new HashMap<>();

        String elasticHost = System.getenv().getOrDefault("ELASTIC_HOST", "localhost");
        this.elasticSearchClient = new RestHighLevelClient(RestClient.builder(
                new HttpHost(elasticHost, 9200, "http")
        ));
    }

    public Mono<NlpModule> getNlpModule(String name) throws IOException {
        if (nlpModuleCache.containsKey(name)) {
            return Mono.just(nlpModuleCache.get(name));
        }

        GetRequest gr = new GetRequest(moduleIndex, moduleDataType, name);

        return Mono.create(sink -> {
            try {
                GetResponse response = elasticSearchClient.get(gr, RequestOptions.DEFAULT);

                if (response.getSource() == null) {
                    sink.error(new Exception("Module " + name + " not found."));
                } else {
                    NlpModuleModel module = new ObjectMapper().readValue(response.getSourceAsString(), NlpModuleModel.class);
                    NlpModule res = null;
                    WebClient client = buildNlpWebClient(name);
                    // Build the concrete module based on the type.
                    switch(module.getType()) {
                        case PREPROCESSOR:
                            res = preprocessorNlpModule;
                            break;
                        case IE:
                            res = ieNlpModule;
                            break;
                    }

                    res.setName(name);
                    res.setClient(client);
                    res.setEndpoints(module.getEndpoints());
                    nlpModuleCache.put(name, res);
                    sink.success(res);
                }

            }
            catch (IOException e) {
                sink.error(e);
            }
        });
    }

    private WebClient buildNlpWebClient(String name) {
        String url = "http://";
        String host = System.getenv().getOrDefault(name.toUpperCase() + "_HOST", "localhost");
        String port = env.getProperty(name + ".port");

        url += host + ":" + port;

        return WebClient.create(url);
    }
}
