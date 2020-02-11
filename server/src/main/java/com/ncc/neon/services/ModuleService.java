package com.ncc.neon.services;

import com.ncc.neon.better.EvalNlpModule;
import com.ncc.neon.better.IENlpModule;
import com.ncc.neon.better.NlpModule;
import com.ncc.neon.better.PreprocessorNlpModule;
import com.ncc.neon.models.ModuleStatus;
import com.ncc.neon.models.NlpModuleModel;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

@Component
public class ModuleService extends ElasticSearchService<NlpModuleModel> {
    private static final String index = "module";
    private static final String dataType = "module";
    private HashMap<String, NlpModule> nlpModuleCache;

    @Autowired
    private Environment env;
    @Autowired
    private DatasetService datasetService;
    @Autowired
    private BetterFileService betterFileService;
    @Autowired
    private FileShareService fileShareService;
    @Autowired
    private RunService runService;
    @Autowired
    private EvaluationService evaluationService;
    @Autowired
    private ModuleService moduleService;


    @Autowired
    ModuleService(DatasetService datasetService, @Value("${db_host}") String dbHost) {
        super(dbHost, index, dataType, NlpModuleModel.class, datasetService);
        nlpModuleCache = new HashMap<>();
    }

    public Mono<NlpModule> buildNlpModuleClient(String name) {
        if (nlpModuleCache.containsKey(name)) {
            return Mono.just(nlpModuleCache.get(name));
        }

        return getById(name).flatMap(moduleModel -> {
            NlpModule res = null;
            WebClient client = buildNlpWebClient(name);
            // Build the concrete module based on the type.
            switch(moduleModel.getType()) {
                case PREPROCESSOR:
                    res = new PreprocessorNlpModule(datasetService, fileShareService, betterFileService, moduleService);
                    break;
                case IE:
                    res = new IENlpModule(datasetService, fileShareService, betterFileService, runService, moduleService);
                    break;
                case EVALUATION:
                    res = new EvalNlpModule(datasetService, fileShareService, betterFileService, runService, evaluationService, moduleService);
                    break;
            }

            res.setName(name);
            res.setClient(client);
            res.setEndpoints(moduleModel.getEndpoints());
            nlpModuleCache.put(name, res);
            return Mono.just(res);
        });
    }

    public Mono<Integer> getJobCount(String moduleId) {
        return getById(moduleId).map(NlpModuleModel::getJobCount);
    }

    public Mono<RestStatus> incrementJobCount(String moduleId) {
        // First, get the current job count.
        return getJobCount(moduleId).flatMap(count -> {
            Map<String, Object> data = new HashMap<>();
            data.put("job_count", ++count);

            if (count > 0) {
                data.put("status", ModuleStatus.BUSY);
            }

            return updateAndRefresh(data, moduleId);
        });
    }

    public Mono<RestStatus> decrementJobCount(String moduleId) {
        return getJobCount(moduleId).flatMap(count -> {
            Map<String, Object> data = new HashMap<>();
            data.put("job_count", --count);

            if (count == 0) {
                data.put("status", ModuleStatus.IDLE);
            }

            return updateAndRefresh(data, moduleId);
        });
    }

    public Mono<RestStatus> setStatusToDown(String moduleId) {
        Map<String, Object> data = new HashMap<>();
        data.put("job_count", 0);
        data.put("status", ModuleStatus.DOWN);
        return updateAndRefresh(data, moduleId);
    }

    public Mono<RestStatus> checkAllConnections() {
        // Get all modules.
        // Make call to heartbeat endpoint.
        // Update status to down if any return connection exception.
    }

    private WebClient buildNlpWebClient(String name) {
        String url = "http://";
        String host = System.getenv().getOrDefault(name.toUpperCase() + "_HOST", "localhost");
        String port = env.getProperty(name + ".port");

        url += host + ":" + port;

        return WebClient.create(url);
    }
}
