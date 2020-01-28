package com.ncc.neon.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.better.EvalNlpModule;
import com.ncc.neon.better.IENlpModule;
import com.ncc.neon.better.NlpModule;
import com.ncc.neon.better.PreprocessorNlpModule;
import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.NlpModuleModel;
import com.ncc.neon.models.queries.FieldClause;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.queries.SelectClause;
import com.ncc.neon.models.queries.SingularWhereClause;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.HashMap;

/*
This class is responsible for deserializing NLP modules from the database to NLP Module POJOs.
 */
@Component
public class NlpModuleService {
    private HashMap<String, NlpModule> nlpModuleCache;
    private NlpModuleModel[] nlpModules;
    private ConnectionInfo nlpModuleConnectionInfo;
    private final String moduleIndex = "module";
    private final String moduleDataType = "module";

    @Autowired
    private Environment env;
    @Autowired
    private PreprocessorNlpModule preprocessorNlpModule;
    @Autowired
    private IENlpModule ieNlpModule;
    @Autowired
    private EvalNlpModule evalNlpModule;
    @Autowired
    private QueryService queryService;

    @Autowired
    public NlpModuleService(@Value("${db_type}") String dbType,
                            @Value("${db_host}") String dbHost) {
        this.nlpModuleCache = new HashMap<>();
        nlpModuleConnectionInfo = new ConnectionInfo(dbType, dbHost);
    }

    public Mono<NlpModule> getNlpModule(String name) {
        if (nlpModuleCache.containsKey(name)) {
            return Mono.just(nlpModuleCache.get(name));
        }

        // Build query to get module by name.
        Query getModuleByNameQuery = new Query();
        getModuleByNameQuery.setSelectClause(new SelectClause(moduleIndex, moduleDataType));
        getModuleByNameQuery.setWhereClause(SingularWhereClause.fromString(new FieldClause(moduleIndex, moduleDataType, "name"), "=", name));

        return queryService.executeQuery(nlpModuleConnectionInfo, getModuleByNameQuery).flatMap(tabularQueryResult -> {
            NlpModuleModel module = new ObjectMapper().convertValue(tabularQueryResult.getFirstOrNull(), NlpModuleModel.class);
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
                case EVAL:
                    res = evalNlpModule;
                    break;
            }

            res.setName(name);
            res.setClient(client);
            res.setEndpoints(module.getEndpoints());
            nlpModuleCache.put(name, res);
            return Mono.just(res);
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
