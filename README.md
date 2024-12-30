public static List<String> createRenameScript(List<String> filenames, String batchId, String prefix) {
    // List to store renamed filenames
    List<String> renamedFilenames = new ArrayList<>();

    for (String filename : filenames) {
        String newFilename = prefix + "_" + filename;
        renamedFilenames.add(newFilename);
    }

    System.out.println("Renamed filenames generated successfully.");
    return renamedFilenames;
}

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class MissingFilesInserter {
    private static final String DB_URL = "jdbc:mysql://<HOST>:<PORT>/<DB_NAME>";
    private static final String USER = "<USERNAME>";
    private static final String PASSWORD = "<PASSWORD>";

    public static void insertMissingFilenames(List<String> missingFilenames) {
        String insertQuery = "INSERT INTO RAW_DATA_ING_METADATA " +
                "(PROJ_ID, DEPT_ID, JOB_ID, TRACKING_ID, FILE_NAME, MODULE, STATUS, UPDATE_TSMP) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        String projectId = "test_hadoop_pharmacy_test_12272024";
        String deptId = "test12272024";
        String jobId = "raw_ingestion_12272024";
        String module = "RDI_AUTOMATION";
        String status = "new";

        // Get today's date
        String currentTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {

            for (String filename : missingFilenames) {
                pstmt.setString(1, projectId);
                pstmt.setString(2, deptId);
                pstmt.setString(3, jobId);
                pstmt.setString(4, "tracking_" + filename);
                pstmt.setString(5, filename);
                pstmt.setString(6, module);
                pstmt.setString(7, status);
                pstmt.setString(8, currentTimestamp);

                pstmt.addBatch(); // Add the statement to the batch
            }

            int[] rowsInserted = pstmt.executeBatch(); // Execute the batch
            System.out.println(rowsInserted.length + " rows inserted successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // Example list of missing filenames
        List<String> missingFilenames = List.of("missing_file1.txt", "missing_file2.txt");
        insertMissingFilenames(missingFilenames);
    }
}
