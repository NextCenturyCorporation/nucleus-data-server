package com.ncc.neon.adapters.sql;

import java.util.Map;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.adapters.QueryAdapterFactory;

public abstract class SqlAdapterFactory extends QueryAdapterFactory {
    protected SqlType type;

    public SqlAdapterFactory(SqlType type, Map<String, String> authCollection) {
        super(type.prettyName, authCollection);
        this.type = type;
    }

    @Override
    public QueryAdapter buildAdapter(String host, String username, String password) {
        return new SqlAdapter(this.type, host, username, password);
    }

    @Override
    public String[] getNames() {
        return new String[] { type.configName };
    }
}
