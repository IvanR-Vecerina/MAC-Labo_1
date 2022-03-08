package ch.heig.mac;

import com.couchbase.client.java.Cluster;

public class Main {

    // TODO: Configure credentials to allow connection to your local Couchbase instance
    public static Cluster openConnection() {
        var connectionString = "127.0.0.1";
        var username = "Administrator";
        var password = "mac2022";

        Cluster cluster = Cluster.connect(
                connectionString,
                username,
                password
        );
        return cluster;
    }

    public static void main(String[] args) {
        var cluster = openConnection();

        var requests = new Requests(cluster);
        var indices = new Indices(cluster);

        indices.createRequiredIndices();

        requests.getCollectionNames().forEach(System.out::println);
        requests.inconsistentRating().forEach(System.out::println);
        requests.hiddenGem().forEach(System.out::println);
        requests.topReviewers().forEach(System.out::println);
        requests.greatReviewers().forEach(System.out::println);
        requests.bestMoviesOfActor("Ralph Fiennes").forEach(System.out::println);
        requests.plentifulDirectors().forEach(System.out::println);
        requests.confusingMovies().forEach(System.out::println);
        requests.commentsOfDirector1("Woody Allen").forEach(System.out::println);
        requests.commentsOfDirector2("Woody Allen").forEach(System.out::println);
    }
}
