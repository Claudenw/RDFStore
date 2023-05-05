package org.xenei.rdfstore.disk;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.xenei.rdfstore.store.AbstractBitmapTest;
import org.xenei.rdfstore.store.Bitmap;

public class DiskBitmapTest extends AbstractBitmapTest {
    
    private static File dir;
    
    @BeforeAll
    public static void createDir() throws IOException {
        dir = File.createTempFile("dbt", null);
        dir.mkdir();
    }
    
    @AfterAll
    public static void removeDir() throws IOException {
        FileUtils.deleteDirectory(dir);
    }

    @Override
    protected Supplier<Bitmap> getSupplier() {
        return () -> new DiskBitmap(dir.getAbsolutePath());
    }

}