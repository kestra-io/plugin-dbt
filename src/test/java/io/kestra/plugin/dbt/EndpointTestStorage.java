package io.kestra.plugin.dbt;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import io.kestra.core.plugins.endpoint.ScopedStorage;
import io.kestra.core.storages.FileAttributes;

final class EndpointTestStorage {
    private EndpointTestStorage() {
    }

    /** Execution storage holding one file: any other URI is reported as missing. */
    static ScopedStorage singleFile(URI uri, String content) {
        return new ScopedStorage() {
            @Override
            public InputStream getFile(URI requested) throws FileNotFoundException {
                if (!requested.equals(uri)) {
                    throw new FileNotFoundException(requested.toString());
                }
                return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public List<FileAttributes> list(URI requested) {
                return List.of();
            }

            @Override
            public boolean exists(URI requested) {
                return requested.equals(uri);
            }

            @Override
            public URI putFile(URI requested, InputStream data) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean deleteFile(URI requested) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
