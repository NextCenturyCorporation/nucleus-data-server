package com.ncc.neon.better;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.models.NlpModuleModel;
import com.ncc.neon.models.NlpModuleType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class NlpModuleDao {
    private static NlpModuleDao singletonInstance = null;
    private HashMap<String, NlpModule> nlpModuleCache;
    private NlpModuleModel[] nlpModules;

    @Value("${en.preprocessor.port}")
    private String enPreprocessorPort;
    @Value("${ar.preprocessor.port}")
    private String arPreprocessorPort;
    @Value("${bpe.port}")
    private String bpePort;
    @Value("${nmt.port}")
    private String nmtPort;
    @Value("${mbert.port}")
    private String mbertPort;

    @Autowired
    private PreprocessorNlpModule preprocessorNlpModule;
    @Autowired
    private IENlpModule ieNlpModule;

    private NlpModuleDao() throws IOException {
        this.nlpModuleCache = new HashMap<>();
        this.nlpModules = new ObjectMapper().readValue(new File("server\\src\\main\\java\\com\\ncc\\neon\\better\\modules.json"), NlpModuleModel[].class);
    }

    public static NlpModuleDao getInstance() throws IOException {
        if (singletonInstance == null) {
            singletonInstance = new NlpModuleDao();
        }

        return singletonInstance;
    }

    public NlpModule getNlpModule(String name) {
        if (nlpModuleCache.containsKey(name)) {
            return nlpModuleCache.get(name);
        }

        NlpModule res = null;
        for (NlpModuleModel module : nlpModules) {
            WebClient client = buildNlpWebClient(name);
            // Found a module in the database.
            if (module.getName().equals(name)) {
                // Build the concrete module based on the type.
                if (module.getType() == NlpModuleType.PREPROCESSOR) {
                    preprocessorNlpModule.setPreprocessEndpoint(module.getPreprocessEndpoint());
                    preprocessorNlpModule.setListEndpoint(module.getListEndpoint());
                    res = preprocessorNlpModule;
                }
                else {
                    ieNlpModule.setTrainEndpoint(module.getTrainEndpoint());
                    ieNlpModule.setTrainListEndpoint(module.getTrainListEndpoint());
                    ieNlpModule.setInfEndpoint(module.getInfEndpoint());
                    ieNlpModule.setInfEndpoint(module.getInfListEndpoint());
                    res = ieNlpModule;
                }

                res.setName(name);
                res.setClient(client);
            }
        }

        nlpModuleCache.put(name, res);
        return res;
    }

    private WebClient buildNlpWebClient(String name) {
        String url = "http://";
        String host = "";
        String port = "";

        switch (name) {
            case "mbert":
                host = System.getenv().getOrDefault("MBERT_HOST", "localhost");
                port = this.mbertPort;
                break;
            case "en-preprocessor":
                host = System.getenv().getOrDefault("EN_PREPROCESSOR_HOST", "localhost");
                port = enPreprocessorPort;
                break;
        }

        url += host + ":" + port;

        return WebClient.create(url);
    }
}
