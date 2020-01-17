package com.ncc.neon.better;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.models.NlpModuleModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

public class NlpModuleDao {
    private static NlpModuleDao singletonInstance = null;
    private HashMap<String, NlpModule> nlpModuleCache;
    private NlpModuleModel[] nlpModules;

    @Autowired
    private Environment env;
    @Autowired
    private PreprocessorNlpModule preprocessorNlpModule;
    @Autowired
    private IENlpModule ieNlpModule;

    private NlpModuleDao() throws IOException {
        this.nlpModuleCache = new HashMap<>();
        Resource modulesResource = new ClassPathResource("modules.json");
        this.nlpModules = new ObjectMapper().readValue(modulesResource.getFile(), NlpModuleModel[].class);
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
            // Found a module in the database.
            if (module.getName().equals(name)) {
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
            }
        }

        nlpModuleCache.put(name, res);
        return res;
    }

    private WebClient buildNlpWebClient(String name) {
        String url = "http://";
        String host = System.getenv().getOrDefault(name.toUpperCase() + "_HOST", "localhost");
        String port = env.getProperty(name + ".port");

        url += host + ":" + port;

        return WebClient.create(url);
    }
}
