package neo4jTest;

import org.neo4j.driver.v1.*;

import static org.neo4j.driver.v1.Values.parameters;

public class HelloWorld implements AutoCloseable {
    private final Driver driver;

    public HelloWorld(String url, String user, String password) {
        this.driver = GraphDatabase.driver(url, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws Exception {

    }

    public void printGreeting(String start, String end) {
        try (Session session = driver.session()) {
            Record greeting = session.writeTransaction(new TransactionWork<Record>() {
                @Override
                public Record execute(Transaction tx) {
                    StatementResult result = tx.run(
                            "match p = shortestpath((n)-[*..30]->(m)) " +
                                    "where n.name=$start and m.name=$end " +
                                    "return p",
                            parameters("start", start,"end", end));
                    Record single = result.single();
                    System.out.println(single.asMap().values());
                    return single;
                }
            });
            System.out.println(greeting);
        }
    }

    public static void main(String... args) throws Exception {
        try (HelloWorld greeter = new HelloWorld("bolt://192.168.2.35:7687", "neo4j", "123456")) {
            greeter.printGreeting("南环线栗庙","凤莲大道大蔡湾");
        }
    }

}
