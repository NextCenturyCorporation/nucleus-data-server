package com.ncc.neon.models;

import lombok.Data;

@Data
public class RelevanceJudgement {
    private long id;
    private String uuid;
    private boolean relevant;

    public String toString(){
        return "{id:"+this.id+", uuid:"+this.uuid+", relevant:"+this.relevant+"}";
    }
}
