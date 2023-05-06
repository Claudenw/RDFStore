package org.xenei.rdfstore.disk;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.xenei.rdfstore.store.AbstractBitmapTest;
import org.xenei.rdfstore.store.Bitmap;

public class DiskBitmapTest extends AbstractBitmapTest {

    @Override
    protected Supplier<Bitmap> getSupplier() {
        Path path = Paths.get(FileUtils.getTempDirectory().getAbsolutePath(), UUID.randomUUID().toString());
        try {
            File dir = Files.createDirectories(path).toFile();
            return () -> new DiskBitmap(dir.getAbsolutePath(), true);
        } catch (IOException e) {
            throw new RuntimeException( e );
        }
    }

}
