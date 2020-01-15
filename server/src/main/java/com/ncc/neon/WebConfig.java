package com.ncc.neon;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.ncc.neon.better.NlpModuleDao;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory;
import org.springframework.boot.web.server.WebServer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.reactive.ContextPathCompositeHandler;
import org.springframework.http.server.reactive.HttpHandler;
import org.springframework.web.reactive.config.WebFluxConfigurer;

@Configuration
public class WebConfig implements WebFluxConfigurer {

    @Bean
    public NettyReactiveWebServerFactory nettyReactiveWebServerFactory(
            @Value("${server.servlet.context-path:/}") String contextPath) {

        return new NettyReactiveWebServerFactory() {

            @Override
            public WebServer getWebServer(HttpHandler httpHandler) {
                Map<String, HttpHandler> handlerMap = new HashMap<>();
                handlerMap.put(contextPath, httpHandler);
                return super.getWebServer(new ContextPathCompositeHandler(handlerMap));

            }
        };
    }

    @Bean
    public NlpModuleDao nlpModuleDao() throws IOException {
        return NlpModuleDao.getInstance();
    }
}
