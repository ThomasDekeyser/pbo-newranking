package be.dekeyser.badminton.fetchnewranking;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "donwload-ranking")
@Data
public class DownloadRankingConfig {
    private String input;
    private String output;
}
