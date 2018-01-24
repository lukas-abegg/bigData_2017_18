import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


@SuppressWarnings("serial")
public class JoinQuery {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // orders
        DataSet<OrderData> orders = getOrderData(env, "/Users/lukas/git-projects/bigData_2017_18/tutorial_2/data/tpch/orders.tbl");
        DataSet<LineItem> lineItems = getLineItemData(env, "/Users/lukas/git-projects/bigData_2017_18/tutorial_2/data/tpch/lineitem.tbl");

        if (params.has("status") && params.has("olderDate") && params.has("newerDate")) {
            try {
                DataSet<OrderPrice> op = filterOrders(orders, params.get("status"))
                        .joinWithHuge(filterItems(lineItems, params.get("olderDate"), params.get("newerDate")))
                        .where(new OrderKeySelector())
                        .equalTo(new LineItemKeySelector())
                        .with(new JoinFunction<OrderData, LineItem, OrderPrice>() {
                            public OrderPrice join(OrderData orderData, LineItem lineItem) throws Exception {
                                return new OrderPrice(orderData.getOrderkey(), lineItem.getExtendedPrice());
                            }
                        }).groupBy(0).sum(1).setParallelism(4);

                if (params.has("output")) {
                    op.writeAsText(params.get("output"));
                    env.execute("Join Query");
                }
            } catch (Exception e) {
                System.err.println("Some parameters are missing");
                e.printStackTrace();
            }
        }
    }

    public static class OrderKeySelector implements KeySelector<OrderData, Long> {
        public Long getKey(OrderData order) {
            return order.getOrderkey();
        }
    }

    public static class LineItemKeySelector implements KeySelector<LineItem, Long> {
        public Long getKey(LineItem item) {
            return item.getOrderkey();
        }
    }

    public static DataSet<OrderData> filterOrders(DataSet<OrderData> orders, final String status) {
         return orders.filter(
                new FilterFunction<OrderData>() {
                    public boolean filter(OrderData orderData) throws Exception {
                            return orderData.getOrderStatus().equals(status);
                    }
                }
        );
    }

    public static DataSet<LineItem> filterItems(DataSet<LineItem> lineItems, final String newerDate, final String olderDate) {
        return lineItems.filter(
                new FilterFunction<LineItem>() {
                    public boolean filter(LineItem lineItem) throws Exception {
                        return isOlderEqual(lineItem.getShipDate(), newerDate) && isNewer(lineItem.getShipDate(), olderDate) ;
                    }
                }
        );
    }

    public static Boolean isOlderEqual(String shipDate, String compDate) throws ParseException {
        final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final Date sDate = format.parse(shipDate);
        final Date cDate = format.parse(compDate);

        return sDate.before(cDate) || sDate.equals(cDate);
    }

    public static Boolean isNewer(String shipDate, String compDate) throws ParseException {
        final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final Date sDate = format.parse(shipDate);
        final Date cDate = format.parse(compDate);

        return sDate.after(cDate);
    }

    public static class OrderPrice extends Tuple2<Long, Double>{

        public OrderPrice(){}

        public Long getOrderkey() {
            return this.f0;
        }

        public Double getExtendedPrice() {
            return this.f1;
        }

        public OrderPrice(Long orderkey, Double extendedPrice) {
            super(orderkey, extendedPrice);
        }
    }

    public static class OrderData extends Tuple2<Long, String>{

        public Long getOrderkey() {
            return this.f0;
        }

        public String getOrderStatus() {
            return this.f1;
        }
    }

    public static DataSet<OrderData> getOrderData(ExecutionEnvironment env, String path) {
        return env.readCsvFile(path)
                .fieldDelimiter("|")
                .includeFields("101000000")
                .tupleType(OrderData.class);
    }

    public static class LineItem extends Tuple3<Long, Double, String>{

        public Long getOrderkey() {
            return this.f0;
        }

        public Double getExtendedPrice() {
            return this.f1;
        }

        public String getShipDate() {
            return this.f2;
        }
    }

    public static DataSet<LineItem> getLineItemData(ExecutionEnvironment env, String path) {
        return env.readCsvFile(path)
                .fieldDelimiter("|")
                .includeFields("1000010000100000")
                .tupleType(LineItem.class);
    }
}
