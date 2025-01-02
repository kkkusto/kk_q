import java.util.*;
import java.util.concurrent.TimeUnit;

public class FilenameChecker {

    public static void main(String[] args) throws Exception {
        // Example usage
        String url = "jdbc:oracle:thin:@your-db-url"; // Replace with your DB URL
        String username = "your-username";
        String password = "your-password";
        String sqlQuery = "SELECT FILE_NAME FROM YOUR_TABLE WHERE PROJECT_ID = ? AND DEPT_ID = ? AND JOB_ID = ?"; // Replace with your query

        List<String> filenamesToCheck = Arrays.asList("file1.txt", "file2.txt", "file3.txt");

        checkFilenamesInDatabase(url, username, password, sqlQuery, filenamesToCheck);

        System.out.println("All filenames are found in the database!");
    }

    public static void checkFilenamesInDatabase(
            String url,
            String username,
            String password,
            String sqlQuery,
            List<String> filenamesToCheck) throws Exception {

        Set<String> filenamesFound = new HashSet<>();

        while (true) {
            // Fetch filenames from the database using the provided function
            Set<String> filenamesInDb = fetchSQLFilenames(url, username, password, sqlQuery);

            // Add found filenames to the set
            filenamesFound.addAll(filenamesInDb);

            // Check if all filenames in the list are found
            if (filenamesFound.containsAll(filenamesToCheck)) {
                break;
            }

            // Wait for 5 minutes before the next check
            System.out.println("Not all filenames found. Checking again in 5 minutes...");
            TimeUnit.MINUTES.sleep(5);
        }
    }

    private static Set<String> fetchSQLFilenames(
            String url,
            String username,
            String password,
            String sqlQuery) throws Exception {

        Set<String> filenames = new HashSet<>();
        
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");

            try (Connection connection = DriverManager.getConnection(url, username, password);
                 PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {

                // Set parameters for the query (example placeholders, update as necessary)
                preparedStatement.setString(1, "projectId");
                preparedStatement.setString(2, "deptId");
                preparedStatement.setString(3, "jobId");
                preparedStatement.setString(4, "POM");
                preparedStatement.setString(5, "CN");
                preparedStatement.setString(6, "todayDate"); // Example date

                // Execute the query
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    filenames.add(resultSet.getString("FILE_NAME"));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error fetching filenames from the database.");
        }

        return filenames;
    }
}
