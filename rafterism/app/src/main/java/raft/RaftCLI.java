package raft;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Scanner;

import com.google.gson.Gson;

public class RaftCLI {

    private static String[] clusterClientEndpoints;
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private static final Gson gson = new Gson();
    private static String currentTargetEndpoint;

    private static class CommandPayload {
        String command; String key; String value;
        CommandPayload(String command, String key, String value) {
            this.command = command; this.key = key; this.value = value;
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java RaftCLI <cluster-client-endpoints>");
            System.out.println("Example: java RaftCLI localhost:9001,localhost:9002,localhost:9003");
            return;
        }
        clusterClientEndpoints = args[0].split(",");
        currentTargetEndpoint = clusterClientEndpoints[0]; // mulai dari node pertama
        System.out.println("Raft KVStore CLI");
        System.out.println("Initial target node: " + currentTargetEndpoint);
        System.out.println("Enter 'exit' to quit.");

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine();
            if (line.equalsIgnoreCase("exit")) break;
            if (line.trim().isEmpty()) continue;

            try {
                processCommand(line);
            } catch (Exception e) {
                System.err.println("Error: Could not connect to target node " + currentTargetEndpoint + ". Trying another node...");
                // kalau koneksi gagal coba node lain
                switchToNextEndpoint();
            }
        }
        scanner.close();
        System.out.println("Exiting CLI.");
    }
    
    private static void processCommand(String line) throws IOException, InterruptedException {
        String[] parts = line.trim().split("\\s+", 3);
        String command = parts[0].toLowerCase();
        String key = parts.length > 1 ? parts[1] : null;
        String value = parts.length > 2 ? parts[2] : null;

        if (key == null && !command.equals("ping")) {
            System.out.println("ERROR: Command '" + command + "' requires a key.");
            return;
        }
        if (value == null && (command.equals("set") || command.equals("append"))) {
            System.out.println("ERROR: Command '" + command + "' requires a value.");
            return;
        }

        String responseBody = sendRequest(command, key, value);

        if (responseBody == null) {
            System.out.println("ERROR: Command failed after retries.");
            return;
        }
        
        // Tampilkan hasil sesuai format soal
        if (command.equals("get")) {
            System.out.println("\"" + responseBody + "\"");
        } else if (command.equals("strln")) {
            System.out.println(responseBody.length());
        } else if (command.equals("del")) {
            System.out.println("\"" + responseBody + "\"");
        } else {
            System.out.println(responseBody);
        }
    }

    private static String sendRequest(String command, String key, String value) throws IOException, InterruptedException {
        HttpRequest request;
        
        for (int i = 0; i < clusterClientEndpoints.length + 1; i++) {
            if (command.equals("get") || command.equals("strln") || command.equals("ping")) {
                String path = command.equals("ping") ? "/ping" : "/" + key;
                request = HttpRequest.newBuilder().uri(URI.create("http://" + currentTargetEndpoint + path)).GET().build();
            } else {
                CommandPayload payload = new CommandPayload(command, key, value);
                String requestBody = gson.toJson(payload);
                request = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + currentTargetEndpoint + "/command"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .build();
            }
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            String body = response.body();

            if (body.startsWith("REDIRECT:")) {
                String newLeaderAddr = body.substring(9);
                System.out.println("--> Redirected to leader at " + newLeaderAddr);
                currentTargetEndpoint = newLeaderAddr;
                continue;
            }
            return body;
        }
        return null;
    }

    private static void switchToNextEndpoint() {
        int currentIndex = -1;
        for (int i = 0; i < clusterClientEndpoints.length; i++) {
            if (clusterClientEndpoints[i].equals(currentTargetEndpoint)) {
                currentIndex = i;
                break;
            }
        }
        int nextIndex = (currentIndex + 1) % clusterClientEndpoints.length;
        currentTargetEndpoint = clusterClientEndpoints[nextIndex];
        System.out.println("--> Switched target to "+ currentTargetEndpoint);
    }
}