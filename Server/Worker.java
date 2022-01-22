import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.sql.*;

public class Worker {
    ServerSocket work_soc = null;
    Socket soc = null;
    DataInputStream dis = null;
    DataOutputStream dos = null;
    private static final String dbClassName = "com.mysql.cj.jdbc.Driver";
    private static String CONNECTION = "jdbc:mysql://localhost:3306/project";
    Connection c = null;
    Statement stmt = null;

    Worker(InetAddress addr, int port) throws IOException, ClassNotFoundException, SQLException {
        work_soc = new ServerSocket(port);
        System.out.println("Waiting for Leader to make a connection request ...");
        soc = work_soc.accept();
        System.out.println("Leader connected!!");

        dis = new DataInputStream(soc.getInputStream());
        dos = new DataOutputStream(soc.getOutputStream());
        int digit = port % 10;
        // project_0 or project_1 or project_1 will be the database number
        CONNECTION = CONNECTION + "_" + digit + "?autoReconnect=true&useSSL=false";
        Class.forName(dbClassName);
        c = DriverManager.getConnection(CONNECTION, "username", "password");
        stmt = c.createStatement();
    }

    // Execute a query and send response in specific format
    String process_query(String query) {
        ResultSet res;
        String to_client = "";
        try {
            res = stmt.executeQuery(query);
            // Metadata used to get the column names and count
            ResultSetMetaData rsmd = res.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            if (columnsNumber >= 1) {
                for (int i = 1; i <= columnsNumber; i++) {
                    // The first line of response is the column names
                    to_client += rsmd.getColumnName(i) + " ";
                }
            }
            to_client += "\n";
            // Add the data values of all tuple to the response
            while (res.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnValue = res.getString(i);
                    to_client += columnValue + " ";
                }
                to_client += '\n';
            }
        } catch (SQLException e) {
            System.out.println(e);
            to_client = "error";
        }
        return to_client;
    }

    // This is where communication between Leader and worker takes place
    void start() {
        String query = null;
        while (true) {
            try {
                query = dis.readUTF();
                System.out.println(query);
                // This over must be send by the leader not by client.
                if (query.toLowerCase().equals("over")) {
                    break;
                }
                // Otherwise execute the query
                else {
                    String response = "";
                    if (query.toLowerCase().contains("select")) {
                        response = process_query(query);
                        dos.writeUTF(response);
                    } else {
                        stmt.executeUpdate(query);
                        response = process_query("select * from persons;");
                        dos.writeUTF(response);
                    }
                }

            } catch (SQLException s) {
                System.out.println(s);
                try {
                    dos.writeUTF("error");
                } catch (IOException i) {
                    System.out.println(i);
                    break;
                }
            } catch (IOException i) {
                System.out.println(i);
                break;
            }
        }
    }

    private void stop() {
        try {
            dis.close();
            dos.close();
            soc.close();
            work_soc.close();
        } catch (IOException i) {
            System.out.println(i);
        }
    }

    public static void main(String[] args) throws ClassNotFoundException {
        int port = 0;
        // can be changed as per requirement
        String address = "127.0.0.1";
        try {
            if (args.length > 1) {
                System.out.println("More number of arguments given while Only argument(Port_Number) is required!");
                return;
            } else {
                port = Integer.parseInt(args[0]);
                InetAddress addr = InetAddress.getByName(address);
                Worker worker = new Worker(addr, port);
                worker.start();
                worker.stop();
            }
        } catch (NumberFormatException nfe) {
            System.out.println("First argument must be an integer!" + nfe);
            return;
        } catch (UnknownHostException h) {
            System.out.println("Specified address doesn't have a known host!" + h);
            return;
        } catch (IOException i) {
            System.out.println("Exception in worker constructor!!" + i);
            return;
        } catch (SQLException s) {
            System.out.println("Problem in executing SQL query" + s);

            return;
        } catch (ClassNotFoundException c) {
            System.out.println("Problem in class forname!" + c);
            return;
        }
    }
}
