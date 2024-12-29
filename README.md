import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class FileRenameScriptGenerator {

    public static void createRenameScript(List<String> filenames, String outputFilePath) {
        // Generate a dynamic batch ID
        String batchId = generateBatchId();
        // Generate a dynamic prefix using the batch ID and today's date
        String prefix = generateDynamicPrefix(batchId);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (String filename : filenames) {
                String newFilename = prefix + "_" + filename;
                writer.write("mv " + filename + " " + newFilename);
                writer.newLine();
            }
            System.out.println("Script written successfully to " + outputFilePath);
        } catch (IOException e) {
            System.err.println("Error writing to the file: " + e.getMessage());
        }
    }

    private static String generateBatchId() {
        // Generate a unique batch ID (UUID or custom logic)
        return "batch_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static String generateDynamicPrefix(String batchId) {
        // Get current date in mmddyyyy format
        String currentDate = new SimpleDateFormat("MMddyyyy").format(new Date());
        // Combine batchId and current date
        return batchId + "_" + currentDate;
    }

    public static void main(String[] args) {
        // Example usage
        List<String> filenames = List.of("file1.txt", "file2.txt", "file3.txt");
        String outputFilePath = "rename_files.sh";

        createRenameScript(filenames, outputFilePath);
    }
}
