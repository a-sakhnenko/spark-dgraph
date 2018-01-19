import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphClient.Transaction;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphGrpc.DgraphBlockingStub;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DGraphExample {
    public static final String SCHEMA = "firstName: string @index(exact, trigram) .\n" +
            "lastName: string @index(exact, trigram) .\n" +
            "age: int @index(int) .\n";
    public static final String FIND_BY_NAME = "query all($firstName: string){\n" + "all(func: eq(firstName, $firstName)) {\n" + "    firstName\n" + "  }\n" + "}";
    public static final String FIND_BY_AGE = "query all($age: int){all(func: ge(age, $age)) {    \n" +
            " firstName\n" +
            "\tlastName\n" +
            "\tage\n" +
            "}\n" +
            "}";
    private static final String TEST_HOSTNAME = "10.66.170.158";
    private static final int TEST_PORT = 9080;
    private DgraphClient dgraphClient;

    public DGraphExample() {
        ManagedChannel channel =
                ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build();
        DgraphBlockingStub blockingStub = DgraphGrpc.newBlockingStub(channel);
        dgraphClient = new DgraphClient(Collections.singletonList(blockingStub));
//        dgraphClient.alter(Operation.newBuilder().setSchema(SCHEMA).build());
//        dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());
//        Operation op = Operation.newBuilder().setSchema(SCHEMA).build();
//        dgraphClient.alter(op);
    }

    public static void main(final String[] args) {
        DGraphExample app = new DGraphExample();
        Long start = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {
            app.createPerson();
        }

        Long finish = System.currentTimeMillis();
        System.out.println((finish - start) / 100.0);

        Long start2 = System.currentTimeMillis();
        People ppl = app.getOlderThan(21);
        System.out.printf("people found: %d\n", ppl.all.size());
        ppl.all.forEach(System.out::println);
        Long finish2 = System.currentTimeMillis();
        System.out.println((finish2 - start2) / 1000.0);
    }

    public People getPeopleByName(String firstName) {
        Gson gson = new Gson();
        Map<String, String> vars = Collections.singletonMap("$firstName", firstName);
        Response res = dgraphClient.newTransaction().queryWithVars(FIND_BY_NAME, vars);
        return gson.fromJson(res.getJson().toStringUtf8(), People.class);
    }

    public People getOlderThan(int age) {
        Gson gson = new Gson();
        Map<String, String> vars = Collections.singletonMap("$age", String.valueOf(age));
        Response res = dgraphClient.newTransaction().queryWithVars(FIND_BY_AGE, vars);
        return gson.fromJson(res.getJson().toStringUtf8(), People.class);
    }

    public void createPerson() {
        Gson gson = new Gson();
        Transaction txn = dgraphClient.newTransaction();
        try {
            Person p = new Person();
            String json = gson.toJson(p);
            Mutation mu = Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(json)).build();
            txn.mutate(mu);
            txn.commit();
        } finally {
            txn.discard();
        }
    }

    static class People {
        List<Person> all;

        People() {
        }
    }
}