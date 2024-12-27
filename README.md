import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class OracleJDBCExample {
    public static void main(String[] args) {
        // Database connection details
        String url = "jdbc:oracle:thin:@<hostname>:<port>:<service_name>";
        String username = "<your_username>";
        String password = "<your_password>";
        
        // SQL query
        String query = "SELECT COUNT(*) "
                     + "FROM RAW_DATA_ING_METADATA "
                     + "WHERE PROJ_ID = ? "
                     + "AND DEPT_ID = ? "
                     + "AND JOB_ID = ? "
                     + "AND MODULE = ? "
                     + "AND STATUS = ? "
                     + "AND UPDATE_TSMP >= TO_DATE(?, 'DD-MON-YYYY HH24.MI.SS') "
                     + "ORDER BY UPDATE_TSMP DESC";

        try (Connection connection = DriverManager.getConnection(url, username, password);
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            
            // Set parameters for the query
            preparedStatement.setString(1, "hadoop_pharmacy_prod");
            preparedStatement.setString(2, "ICPplus");
            preparedStatement.setString(3, "raw_ingestion");
            preparedStatement.setString(4, "PO");
            preparedStatement.setString(5, "C");
            preparedStatement.setString(6, "20-OCT-2021 00.05.03");
            
            // Execute the query
            ResultSet resultSet = preparedStatement.executeQuery();
            
            // Process the result
            if (resultSet.next()) {
                int count = resultSet.getInt(1);
                System.out.println("Count: " + count);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
