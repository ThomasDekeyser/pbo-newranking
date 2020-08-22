package be.dekeyser.badminton.fetchnewranking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CmdLineApplication {

    public static void main(final String[] args) {
        SpringApplication.run(CmdLineApplication.class, args)
                         .close();
    }

}
