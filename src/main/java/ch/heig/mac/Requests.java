package ch.heig.mac;

import java.util.List;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryMetrics;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import netscape.javascript.JSObject;


public class Requests {
    private final Cluster cluster;

    public Requests(Cluster cluster) {
        this.cluster = cluster;
    }

    public List<String> getCollectionNames() {
        QueryResult result = cluster.query(
                "SELECT RAW r.name\n" +
                        "FROM system:keyspaces r\n" +
                        "WHERE r.`bucket` = \"mflix-sample\";"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> inconsistentRating() {
        QueryResult result = cluster.query("SELECT imdb.id AS imdb_id, tomatoes.viewer.rating AS tomatoes_rating, imdb.rating AS imdb_rating\n" +
                "FROM `mflix-sample`._default.movies\n" +
                "WHERE tomatoes.viewer.rating != 0 AND ABS(tomatoes.viewer.rating - imdb.rating) >= 7");
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> hiddenGem() {
        QueryResult result = cluster.query("SELECT title\n" +
                "FROM `mflix-sample`._default.movies\n" +
                "WHERE tomatoes.critic.rating == 10 AND tomatoes.viewer.rating IS MISSING");
        return  result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> topReviewers() {
        QueryResult result = cluster.query("SELECT comments.name, COUNT(comments.name) AS cnt\n" +
                "FROM `mflix-sample`._default.comments\n" +
                "GROUP BY comments.name\n" +
                "ORDER BY cnt DESC\n" +
                "LIMIT 10");
        return result.rowsAs(JsonObject.class);
    }

    public List<String> greatReviewers() {
        QueryResult result = cluster.query("SELECT RAW comments.name\n" +
                "FROM `mflix-sample`._default.comments\n" +
                "GROUP BY comments.name\n" +
                "HAVING COUNT(comments.name) > 300");
        return result.rowsAs(String.class);
    }

    public List<JsonObject> bestMoviesOfActor(String actor) {
        QueryResult result = cluster.query("SELECT imdb.id imdb_id, imdb.rating, (`cast`)\n" +
                "FROM `mflix-sample`._default.movies\n" +
                "WHERE ISNUMBER(imdb.rating) AND imdb.rating > 8 AND ARRAY_CONTAINS((`cast`), \""+ actor + "\")");
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> plentifulDirectors() {
        QueryResult result = cluster.query("SELECT director director_name, COUNT(director) count_film \n" +
                "FROM `mflix-sample`._default.movies\n" +
                "UNNEST directors as director\n" +
                "GROUP BY director\n" +
                "HAVING COUNT(director) > 30");
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> confusingMovies() {
        QueryResult result = cluster.query("SELECT _id AS movie_id, title\n" +
                "FROM `mflix-sample`._default.movies\n" +
                "WHERE ARRAY_COUNT(directors) > 20;");
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> commentsOfDirector1(String director) {
        QueryResult result = cluster.query("SELECT movies._id AS movie_id, comments.text\n" +
                "FROM `mflix-sample`._default.movies\n" +
                "INNER JOIN `mflix-sample`._default.comments ON comments.movie_id = movies._id\n" +
                "WHERE ARRAY_CONTAINS(movies.directors, '" + director + "');");
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        QueryResult result = cluster.query("SELECT movie_id, text\n" +
                "FROM `mflix-sample`._default.comments\n" +
                "WHERE movie_id IN (\n" +
                "    SELECT RAW movies._id\n" +
                "    FROM `mflix-sample`._default.movies\n" +
                "    WHERE ARRAY_CONTAINS(movies.directors, '" + director +"'));");
        return result.rowsAs(JsonObject.class);
    }

    // Returns the number of documents updated.
    public long removeEarlyProjection(String movieId) {
        QueryResult result = cluster.query("UPDATE `mflix-sample`._default.theaters AS t\n" +
                "SET t.schedule = ARRAY v FOR v IN t.schedule WHEN (v.movieId = \"" + movieId + "\" AND v.hourBegin >= \"18:00:00\") OR v.movieId != \"" + movieId + "\" END\n" +
                "WHERE ANY v IN t.schedule SATISFIES v.movieId = \"" + movieId + "\" AND v.hourBegin < \"18:00:00\" END;", QueryOptions.queryOptions().metrics(true));

        return result.metaData().metrics().get().mutationCount();
    }

    public List<JsonObject> nightMovies() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }


}
