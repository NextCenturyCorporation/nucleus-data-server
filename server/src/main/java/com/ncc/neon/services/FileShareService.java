package com.ncc.neon.services;

import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

@Component
public class FileShareService {
    public final Path sharePath;

    FileShareService() {
        String shareDir = System.getenv().getOrDefault("SHARE_DIR", "share");
        this.sharePath = Paths.get(".").resolve(shareDir);
    }

    public Mono<File> writeFilePart(FilePart filePart) {
        Path filePath = this.sharePath.resolve(Objects.requireNonNull(filePart.filename()));
        return filePart.transferTo(filePath).thenReturn(new File(filePath.toString()));
    }

    public Flux<Boolean> deleteMany(String[] filesToDelete) {
        return Flux.fromArray(filesToDelete)
                .flatMap(this::delete);
    }

    public Mono<Boolean> delete(String fileToDelete) {
        // Append filename to share directory.
        String filepath = this.sharePath.resolve(fileToDelete).toString();
        return Mono.just(new File(filepath).delete());
    }
}
