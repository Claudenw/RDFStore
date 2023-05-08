package org.xenei.rdfstore.idx;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.util.iterator.WrappedIterator;
import org.junit.jupiter.api.Test;
import org.xenei.rdfstore.mem.MemBitmap;
import org.xenei.rdfstore.store.Bitmap;
import org.xenei.rdfstore.store.IdxData;
import org.xenei.rdfstore.store.Index;

public class NumberIdxTest extends AbstractIndexTest<BigDecimal> {
    // private Map<Number, IdxData<Bitmap>> map = new TreeMap<>();
    private Mapper<BigDecimal> mapper = new MapMapper<BigDecimal>(
            new TreeMap<BigDecimal, IdxData<Bitmap>>());

    private Supplier<Bitmap> supplier = () -> new MemBitmap();

    @Override
    protected Supplier<Index<BigDecimal>> supplier() {
        return () -> new NumberIdx(supplier, mapper);
    }

    int count = 0;

    @Override
    protected BigDecimal get() {
        return BigDecimal.valueOf(count++);
    }

    private void assertContains(Bitmap bitmap, long[] values) {
        for (long l : values) {
            assertTrue(bitmap.contains(l), "looking for " + l);
        }
    }

    Object[] values = { Integer.MAX_VALUE, Long.MAX_VALUE, Double.MAX_VALUE, Float.MAX_VALUE, BigDecimal.TEN,
            BigInteger.TEN, Byte.MAX_VALUE, Integer.valueOf(Byte.MAX_VALUE), Long.valueOf(Byte.MAX_VALUE),
            Long.valueOf(Integer.MAX_VALUE), Double.valueOf(Float.MAX_VALUE), Double.valueOf(Byte.MAX_VALUE),
            BigDecimal.valueOf(Integer.MAX_VALUE), BigDecimal.valueOf(Byte.MAX_VALUE),
            BigDecimal.valueOf(Long.MAX_VALUE), BigInteger.valueOf(Integer.MAX_VALUE),
            BigInteger.valueOf(Byte.MAX_VALUE), BigInteger.valueOf(Long.MAX_VALUE),
            BigDecimal.valueOf(Double.MAX_VALUE), BigDecimal.valueOf(Float.MAX_VALUE), 10, 3.14, 3.14f,
            Float.valueOf(3.14f), Double.valueOf(3.14), BigDecimal.valueOf(3.14), BigDecimal.valueOf(3.14f),
            Short.MAX_VALUE, Long.valueOf(Short.MAX_VALUE), "3.14", "123", 123 };

    private List<Literal> getLiterals() {
        return Arrays.stream(values).map(v -> ResourceFactory.createTypedLiteral(v)).collect(Collectors.toList());
    }

    @Test
    public void parseTest() {

        List<Literal> literals = getLiterals();

        List<BigDecimal> converted = WrappedIterator.create(literals.iterator())
                .mapWith((n) -> NumberIdx.parse(n.asNode().getLiteral())).toList();

        for (int i = 0; i < converted.size(); i++) {
            assertNotNull(converted.get(i), "failure with " + literals.get(i));
        }

        BigDecimal b = NumberIdx.parse(ResourceFactory.createPlainLiteral("123").asNode().getLiteral());
        assertNotNull(b);
        assertEquals(123, b.intValue());

        b = NumberIdx.parse(NodeFactory.createLiteral("123").getLiteral());
        assertNotNull(b);
        assertEquals(123, b.intValue());

        b = NumberIdx.parse(NodeFactory.createLiteral("wow").getLiteral());
        assertNull(b);

        b = NumberIdx.parse(
                ResourceFactory.createTypedLiteral("1682936023743", XSDDatatype.XSDunsignedLong).asNode().getLiteral());
        assertNotNull(b);
        assertEquals(1682936023743L, b.longValue());
    }

    @Test
    public void lookupTest() {

        List<Literal> literals = getLiterals();

        NumberIdx idx = new NumberIdx(supplier, mapper);

        List<BigDecimal> converted = WrappedIterator.create(literals.iterator())
                .mapWith((n) -> NumberIdx.parse(n.asNode().getLiteral())).toList();

        int i = 1;
        for (BigDecimal val : converted) {
            idx.register(val, i++);
        }

        long[] intMax = { 1, 10, 13, 16 };
        assertContains(idx.get(converted.get(0)), intMax);

        long[] longMax = { 2, 15, 18 };
        assertContains(idx.get(converted.get(1)), longMax);

        long[] dblMax = { 3, 19 };
        assertContains(idx.get(converted.get(2)), dblMax);

        long[] fltMax = { 4, 11, 20 };
        assertContains(idx.get(converted.get(3)), fltMax);

        long[] ten = { 5, 6, 21 };
        assertContains(idx.get(converted.get(4)), ten);

        long[] bytMax = { 7, 8, 9, 12, 14, 17 };
        assertContains(idx.get(converted.get(6)), bytMax);

        long[] longPi = { 22, 25, 26, 30 };
        assertContains(idx.get(converted.get(21)), longPi);

        long[] floatPi = { 23, 24, 27 };
        assertContains(idx.get(converted.get(22)), floatPi);

        long[] shortMax = { 28, 29 };
        assertContains(idx.get(converted.get(27)), shortMax);

        long[] num123 = { 31, 32 };
        assertContains(idx.get(converted.get(30)), num123);
    }

}
