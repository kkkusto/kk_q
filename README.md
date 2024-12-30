import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class FileNameHandler {

    private static final String DB_URL = "jdbc:mysql://<HOST>:<PORT>/<DB_NAME>";
    private static final String USER = "<USERNAME>";
    private static final String PASSWORD = "<PASSWORD>";

    public static void getFileNamesAndWriteToFile(String projectId, String deptId, String jobId, String module, String outputFilePath) {
        String selectQuery = "SELECT FILE_NAME FROM RAW_DATA_ING_METADATA " +
                             "WHERE PROJ_ID = ? AND DEPT_ID = ? AND JOB_ID = ? AND MODULE = ?";

        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(selectQuery);
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {

            pstmt.setString(1, projectId);
            pstmt.setString(2, deptId);
            pstmt.setString(3, jobId);
            pstmt.setString(4, module);

            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                String fileName = rs.getString("FILE_NAME");
                writer.write(fileName);
                writer.newLine();
            }

            System.out.println("Filenames have been written to " + outputFilePath);

        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("File writing error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String projectId = "test_hadoop_pharmacy_test_12272024";
        String deptId = "test12272024";
        String jobId = "raw_ingestion_12272024";
        String module = "RDI_AUTOMATION";
        String outputFilePath = "output_filenames.txt"; // Specify the output file path

        getFileNamesAndWriteToFile(projectId, deptId, jobId, module, outputFilePath);
    }
}



public static void deleteRows(String projectId, String deptId, String jobId, String module) {
    String deleteQuery = "DELETE FROM RAW_DATA_ING_METADATA " +
                         "WHERE PROJ_ID = ? AND DEPT_ID = ? AND JOB_ID = ? AND MODULE = ?";

    try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
         PreparedStatement pstmt = conn.prepareStatement(deleteQuery)) {

        pstmt.setString(1, projectId);
        pstmt.setString(2, deptId);
        pstmt.setString(3, jobId);
        pstmt.setString(4, module);

        int rowsDeleted = pstmt.executeUpdate();
        System.out.println(rowsDeleted + " rows deleted successfully.");
    } catch (SQLException e) {
        e.printStackTrace();
    }
}
