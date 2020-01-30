package com.ncc.neon.services;

import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

@Slf4j
@Component
public class FileShareService {
    private final Path SHARE_PATH;

    FileShareService() {
        String shareDir = System.getenv().getOrDefault("SHARE_DIR", "share");
        this.SHARE_PATH = Paths.get(".").resolve(shareDir);
    }

    public Path getSharePath() {
    	return SHARE_PATH;
    }

    public Mono<File> writeFilePart(FilePart filePart) {
        Path filePath = this.SHARE_PATH.resolve(Objects.requireNonNull(filePart.filename()));
        log.debug("writing to file: " + filePath.toAbsolutePath());
        return filePart.transferTo(filePath).thenReturn(new File(filePath.toString()));
    }

    public Flux<Boolean> deleteMany(String[] filesToDelete) {
        return Flux.fromArray(filesToDelete)
                .flatMap(this::delete);
    }

    public Mono<Boolean> delete(String fileToDelete) {
        // Append filename to share directory.
        String filepath = this.SHARE_PATH.resolve(fileToDelete).toString();
        return Mono.just(new File(filepath).delete());
    }
}
