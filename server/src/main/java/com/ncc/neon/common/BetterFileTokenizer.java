package com.ncc.neon.common;

import org.springframework.web.reactive.function.client.WebClient;

public class BetterFileTokenizer extends BetterFileOperationHandler {
    public BetterFileTokenizer(WebClient tokenizerClient) {
        super(tokenizerClient);
    }
}
