import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FileNameComparison {
    public static void main(String[] args) {
        // JDBC connection parameters
        String url = "jdbc:your_database_url"; // Replace with your DB URL
        String user = "your_username";        // Replace with your DB username
        String password = "your_password";    // Replace with your DB password

        // Input and output file paths
        String inputFile = "filenames.txt";   // Path to the file containing filenames (one per line)
        String outputFile = "missing_filenames.txt"; // Path to the output file for missing filenames

        // SQL query
        String sqlQuery = "SELECT FILE_NAME FROM RAW_DATA_ING_METADATA " +
                          "WHERE PROJ_ID = 'hadoop_pharmacy_prod' " +
                          "AND DEPT_ID = 'ICPlus' " +
                          "AND JOB_ID = 'raw_ingestion' " +
                          "AND MODULE = 'PO' " +
                          "AND STATUS = 'C' " +
                          "AND UPDATE_TSMP >= TO_DATE('26-DEC-2024 00.05.03', 'DD-MON-YYYY HH24.MI.SS') " +
                          "ORDER BY UPDATE_TSMP DESC";

        try {
            // Read filenames from the input file
            Set<String> allFilenames = readFile(inputFile);

            // Retrieve filenames from the SQL query
            Set<String> sqlFilenames = fetchSQLFilenames(url, user, password, sqlQuery);

            // Compare and get missing filenames
            List<String> missingFilenames = new ArrayList<>();
            for (String filename : allFilenames) {
                if (!sqlFilenames.contains(filename)) {
                    missingFilenames.add(filename);
                }
            }

            // Write missing filenames to the output file
            writeFile(outputFile, missingFilenames);

            System.out.println("Missing filenames have been written to: " + outputFile);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Reads filenames from a file and stores them in a Set.
     */
    private static Set<String> readFile(String filePath) throws IOException {
        Set<String> filenames = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                filenames.add(line.trim());
            }
        }
        return filenames;
    }

    /**
     * Fetches filenames from the database using JDBC.
     */
    private static Set<String> fetchSQLFilenames(String url, String user, String password, String sqlQuery) throws SQLException {
        Set<String> filenames = new HashSet<>();
        try (Connection connection = DriverManager.getConnection(url, user, password);
             PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                filenames.add(resultSet.getString("FILE_NAME"));
            }
        }
        return filenames;
    }

    /**
     * Writes missing filenames to an output file.
     */
    private static void writeFile(String filePath, List<String> filenames) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (String filename : filenames) {
                writer.write(filename);
                writer.newLine();
            }
        }
    }
}
