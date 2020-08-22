package be.dekeyser.badminton.fetchnewranking;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Slf4j
public class DownloadRanking implements CommandLineRunner {

    private static final String GRAPHQL_URL = "https://badvlasim.westeurope.cloudapp.azure.com/api/v1/graphql";
    private static final String COMMA_DELIMITER = ";";
    private static final int QUERY_BATCH_SIZE = 50;

    private final DownloadRankingConfig downloadRankingConfig;

    @Autowired
    public DownloadRanking(final DownloadRankingConfig downloadRankingConfig) {
        this.downloadRankingConfig = downloadRankingConfig;
    }

    @Override
    public void run(final String... args) {

        final String inputFile = downloadRankingConfig.getInput();
        final Path outputFile = Path.of(downloadRankingConfig.getOutput());


        try (final BufferedReader br = new BufferedReader(new FileReader(inputFile))) {


            try (final BufferedWriter writer = Files.newBufferedWriter(outputFile)) {

                final int memberidIndex = processHeaderLineAndReturnMemberIdIndex(br.readLine(), writer);

                processDataLines(br, writer, memberidIndex);

            } catch (final IOException e) {
                throw new RuntimeException("Unable to write file " + outputFile, e);
            }

        } catch (final IOException e) {
            throw new RuntimeException("Unable to read file " + inputFile, e);
        }

    }

    private int processHeaderLineAndReturnMemberIdIndex(
            final String firstLine,
            final BufferedWriter writer) throws IOException {
        final int memberidIndex = Arrays.asList(firstLine.split(COMMA_DELIMITER))
                                        .indexOf("memberid");

        writer.write(firstLine + COMMA_DELIMITER + Joiner.on(COMMA_DELIMITER)
                                                         .join(
                                                                 Arrays.asList("PlayerLevelSingleR",
                                                                         "PlayerLevelDoubleR",
                                                                         "PlayerLevelMixedR")) + "\n");
        writer.flush();

        return memberidIndex;
    }

    private void processDataLines(
            final BufferedReader br,
            final BufferedWriter writer,
            final int memberidIndex) throws IOException {

        // There can be multiple lines for the same memberId number
        // Need to remain the same amount of lines
        final Multimap<String, String> memberIdMap = ArrayListMultimap.create();

        String line;
        int partitionCounter = 0;
        int memberIdCounter = 0;
        while ((line = br.readLine()) != null) {
            memberIdCounter++;
            final String memberId = line.split(COMMA_DELIMITER)[memberidIndex];
            memberIdMap.put(memberId, line);

            if (memberIdMap.keySet()
                           .size() == QUERY_BATCH_SIZE) {
                processPartition(memberIdMap, writer, ++partitionCounter);
            }
        }
        // Process last incomplete partition.
        processPartition(memberIdMap, writer, ++partitionCounter);
        log.info("Processed {} data lines", memberIdCounter);
    }


    private void processPartition(
            final Multimap<String, String> memberIdMap,
            final BufferedWriter writer,
            final Integer partitionId) throws IOException {

        log.info("Processing partition {} with size {} for {} memberIds", partitionId, memberIdMap.size(), memberIdMap.keySet()
                                                                                                                      .size());
        updatePartitionWithNewRankingInfo(memberIdMap);
        for (final String memberId : memberIdMap.keySet()) {
            for (final String csvLine : memberIdMap.get(memberId)) {
                writer.write(csvLine + "\n");
            }
        }
        writer.flush();
        memberIdMap.clear();
    }

    private void updatePartitionWithNewRankingInfo(final Multimap<String, String> memberIdMap) {


        final String qPart = memberIdMap.keySet()
                                        .stream()
                                        .map(m -> "{memberId:\"" + m + "\"}")
                                        .collect(Collectors.joining(","));

        final JSONObject graphQL = new JSONObject();
        graphQL.put("query", "{\n" +
                " players(where: {or : [\n" + qPart +
                "]}) {\n" +
                "  firstName, lastName, memberId, rankingPlaces (limit:1, order:\"rankingDate\", direction:\"desc\") {single,mix,double}\n" +
                "}\n" +
                "}");

        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);

        final HttpEntity<String> request =
                new HttpEntity<>(graphQL.toString(), httpHeaders);

        final RestTemplate restTemplate = new RestTemplate();

        final ResponseEntity<String> responseEntity = restTemplate.exchange(GRAPHQL_URL, HttpMethod.POST, request, String.class);
        if (responseEntity.getStatusCode() == HttpStatus.OK) {
            final JSONObject responseJSON = new JSONObject(responseEntity.getBody());

            final JSONArray playersArray = responseJSON.getJSONObject("data")
                                                       .getJSONArray("players");

            final Map<String, String> playersToNewRank = new HashMap<>();
            for (int i = 0; i < playersArray.length(); i++) {
                final JSONObject player = playersArray.getJSONObject(i);
                final int memberId = player.getInt("memberId");
                final JSONObject rankingPlaces = player.getJSONArray("rankingPlaces")
                                                       .getJSONObject(0);


                playersToNewRank.put(String.valueOf(memberId), Joiner.on(COMMA_DELIMITER)
                                                                     .join(
                                                                             Arrays.asList(
                                                                                     rankingPlaces.getInt("single"),
                                                                                     rankingPlaces.getInt("double"),
                                                                                     rankingPlaces.getInt("mix"))));
            }

            log.info("Received {} results for this partition.", playersToNewRank.size());
            memberIdMap.keySet()
                       .forEach(memberId -> memberIdMap.replaceValues(memberId, memberIdMap.get(memberId)
                                                                                           .stream()
                                                                                           .map(s -> s + COMMA_DELIMITER + playersToNewRank.getOrDefault(memberId,
                                                                                                   COMMA_DELIMITER + COMMA_DELIMITER))
                                                                                           .collect(Collectors.toList())));

        } else {
            throw new RuntimeException("GraphQL request failed: " + responseEntity.toString());
        }
    }


}
