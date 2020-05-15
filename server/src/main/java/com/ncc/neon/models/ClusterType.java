package com.ncc.neon.models;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum ClusterType {
    NUMBER ("number"),
    STRING ("string"),
    DATE ("date"),
    LAT_LON ("latlon");

    public String name;

    public String toString() {
        return this.name;
    }
}