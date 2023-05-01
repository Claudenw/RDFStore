package org.xenei.rdfstore.idx;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.graph.impl.LiteralLabel;
import org.xenei.rdfstore.store.Bitmap;

/**
 * An index of numbers to uri indices.
 */
public class NumberIdx extends AbstractIndex<BigDecimal> {

    public NumberIdx(Supplier<Bitmap> bitmapSupplier, Mapper<BigDecimal> map) {
        super(() -> "NumberIdx", bitmapSupplier, map);
    }

    private static Function<Number, BigDecimal> longF = (n) -> BigDecimal.valueOf(n.longValue());
    private static Function<Number, BigDecimal> doubleF = (n) -> BigDecimal.valueOf(n.doubleValue());
    private static Function<Number, BigDecimal> floatF = (n) -> BigDecimal.valueOf(n.floatValue());

    enum Converter {
        BigDecimal(n -> (BigDecimal) n), BigInteger(n -> new BigDecimal((BigInteger) n)), Byte(longF), Double(doubleF),
        Float(floatF), Integer(longF), Long(longF), Short(longF);

        Function<Number, BigDecimal> func;

        Converter(Function<Number, BigDecimal> func) {
            this.func = func;
        }

    };

    public static BigDecimal parse(LiteralLabel label) {
        try {
            Object o = label.getValue();
            if (o instanceof Number) {

                // these first because they are instances of Number
                String[] segs = o.getClass().getName().split("\\.");

                Converter c = Converter.valueOf(segs[segs.length - 1]);

                return c.func.apply((Number) o);
            } else if (o instanceof String) {
                return new BigDecimal((String) label.getValue());
            }
        } catch (IllegalArgumentException | DatatypeFormatException e) {
            // fall through
        }
        return null;
    }

}
