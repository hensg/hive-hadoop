package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.StructObject;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyBoolean;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.*;

@Description(
        name = "CheapestPricesPerHotel",
        value = "_FUNC_(map<int,struct<advertisers:map<string,array<struct<eurocents:int,breakfast:boolean>>>>>) - "
                + "returns a list containing cheapest prices per hotel",
        extended = "Example:\n"
                + "  > SELECT _FUNC_(hotelresults) FROM search_results LIMIT 1;\n"
                + " [1, 2] "
)
public final class CheapestPricesPerHotelBreakfast extends GenericUDF {

    private MapObjectInspector mapInput;

    private List<IntWritable> ret = new LinkedList<>();

    @Override
    public ObjectInspector initialize(final ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("cheapest_prices_per_hotel_breakfast expects 1 argument");
        }
        if (arguments[0].getCategory() != ObjectInspector.Category.MAP) {
            throw new UDFArgumentException("cheapest_prices_per_hotel_breakfast expects a map as argument, got " +
                    arguments[0].getCategory());
        }
        this.mapInput = (MapObjectInspector)arguments[0];
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    }

    @Override
    public Object evaluate(final DeferredObject[] arguments) throws HiveException {
        ret.clear();

        Map<IntWritable, StructObject> hotelResults = (Map<IntWritable, StructObject>) this.mapInput.getMap(arguments[0].get());
        for (StructObject advertiserStructObject: hotelResults.values()) {

            StructObjectInspector advertiserStructObjectInspector = (StructObjectInspector) this.mapInput.getMapValueObjectInspector();
            List<? extends StructField> advertiserFields = advertiserStructObjectInspector.getAllStructFieldRefs();
            StructField advertisersStructField = advertiserFields.get(0);
            Object advertiserStructFieldData = advertiserStructObjectInspector.getStructFieldData(advertiserStructObject, advertisersStructField);
            MapObjectInspector advertiserMapObjectInspector = (MapObjectInspector) advertisersStructField.getFieldObjectInspector();

            Map<Text, LazyArray> advertiserMap = (Map<Text, LazyArray>) advertiserMapObjectInspector.getMap(advertiserStructFieldData);

            IntWritable cheapestPricePerHotelThatOffersBreakfast = null;

            for (LazyArray deals: advertiserMap.values()) {

                ListObjectInspector dealsObjectInspector = (ListObjectInspector) advertiserMapObjectInspector.getMapValueObjectInspector();
                List<StructObject> dealsStrucs = (List<StructObject>) dealsObjectInspector.getList(deals);

                for (StructObject deal : dealsStrucs) {
                    StructObjectInspector dealElementInspector = (StructObjectInspector) dealsObjectInspector.getListElementObjectInspector();

                    List<? extends StructField> dealFields = dealElementInspector.getAllStructFieldRefs();
                    StructField eurocentsField = dealFields.get(0);
                    StructField breakfastField = dealFields.get(1);

                    IntWritable eurocentsValue = ((LazyInteger) dealElementInspector.getStructFieldData(deal, eurocentsField)).getWritableObject();
                    BooleanWritable breakfastValue = ((LazyBoolean) dealElementInspector.getStructFieldData(deal, breakfastField)).getWritableObject();

                    if (breakfastValue.get()) {
                        if (cheapestPricePerHotelThatOffersBreakfast == null ||
                                (eurocentsValue.compareTo(cheapestPricePerHotelThatOffersBreakfast) < 0)) {
                            cheapestPricePerHotelThatOffersBreakfast = eurocentsValue;
                        }
                    }
                }
            }
            if (cheapestPricePerHotelThatOffersBreakfast != null)
                ret.add(cheapestPricePerHotelThatOffersBreakfast);
        }
        if (ret.isEmpty())
            return null;
        return ret;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "cheapest_prices_per_hotel_breakfast(map<int:struct<advertisers...)";
    }
}
