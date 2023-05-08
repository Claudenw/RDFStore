package org.xenei.rdfstore.disk;

import java.io.IOException;

import org.xenei.rdfstore.disk.DiskLongList.IOExec;
import org.xenei.rdfstore.disk.DiskLongList.IOSupplier;

public class ExceptionHandler {

    public static <X> X exH(DiskLongList.IOSupplier<X> supplier) {
        try {
            return supplier.run();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void exH(DiskLongList.IOExec exec) {
        try {
            exec.run();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
