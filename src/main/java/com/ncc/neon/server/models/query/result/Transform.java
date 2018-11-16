package com.ncc.neon.server.models.query.result;

import java.util.List;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
public class Transform {
    String transformName;

    // TODO:???
    List<String> params;
}