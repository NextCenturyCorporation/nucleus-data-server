package com.ncc.neon.better;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.models.Docfile;
import jdk.javadoc.internal.doclets.toolkit.util.DocFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.ncc.neon.controllers.BetterController.SHARE_PATH;

public class IRDocument {
    public String id;
    public String uuid;
    public String text;

    private ArrayList<DocFile> rawDocuments;

    public IRDocument(Docfile docfile) throws IOException {
        this.id = docfile.getId();
        this.uuid = docfile.getUuid();
        this.text = docfile.getText();

        String filepath = this.SHARE_PATH.resolve(filePath).toString();

        // Read document file.
        ObjectMapper objectMapper = new ObjectMapper();
        rawDocuments = objectMapper.readValue(filepath, ArrayList.class);
    }

}
