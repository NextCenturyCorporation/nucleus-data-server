package com.ncc.neon.adapters;

import java.util.LinkedHashMap;
import java.util.Map;

import com.ncc.neon.models.ConnectionInfo;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public abstract class QueryAdapterFactory {

    protected String prettyName;
    protected Map<String, String> authCollection = new LinkedHashMap<String, String>();

    public QueryAdapterFactory(final String prettyName, final Map<String, String> authCollection) {
        this.prettyName = prettyName;
        if(authCollection == null) {
            log.debug(prettyName + " adapter did not find any auth in the server.properties file");
        }
        else {
            this.authCollection.putAll(authCollection);
            log.debug(prettyName + " adapter did find " + authCollection.size() +
                " auth definitions in the server.properties file");
        }
    }

    /**
     * Connects to the datastore.
     *
     * @param cInfo {@link ConnectionInfo}
     */
    public abstract QueryAdapter buildAdapter(String host, String username, String password);

    /**
     * Gets the config name(s) for the datastore type.
     */
    public abstract String[] getNames();

    /**
     * Connects to the datastore.
     * 
     * @param cInfo {@link ConnectionInfo}
     */
    public QueryAdapter initialize(ConnectionInfo cInfo) {
        String host = cInfo.getHost();
        String auth = this.authCollection.containsKey(host) ? this.authCollection.get(host) : null;
        String username = null;
        String password = null;
        if(auth != null) {
            String[] authData = auth.split(":");
            if(authData.length > 1) {
                username = authData[0];
                password = authData[1];
            }
        }
        return buildAdapter(host, username, password);
    }
}
