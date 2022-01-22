import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.net.*;
import java.io.*;
import java.util.HashSet;

class run_worker extends Thread {
    BlockingQueue<String> query_q;
    BlockingQueue<String> response_q;
    Socket soc = null;
    DataInputStream in = null;
    DataOutputStream out = null;

    run_worker(Socket socket, BlockingQueue<String> queryQ, BlockingQueue<String> responseQ) throws IOException {
        soc = socket;
        this.query_q = queryQ;
        this.response_q = responseQ;
        in = new DataInputStream(soc.getInputStream());
        out = new DataOutputStream(soc.getOutputStream());
    }

    @Override
    public void run() {
        while (true) {
            try {
                String client_query = query_q.take();
                String resp = "";
                System.out.println(client_query);
                System.out.println("Sending the query to Worker: " + Thread.currentThread().getName());
                out.writeUTF(client_query);
                resp = in.readUTF();
                if (resp.isEmpty())
                    resp = "Yo";
                response_q.put(resp);
            } catch (InterruptedException | IOException e) {
                System.out.println(e);
                e.printStackTrace();
            }
        }
    }
}

public class Leader {
    private static final int[] port_array = { 5000, 5001, 5002 };
    private static final String[] address = { "127.0.0.1", "127.0.0.1", "127.0.0.1" };
    private HashSet<String> query_responses = new HashSet<String>();
    // streams for the sockets with the workers
    DataInputStream[] worker_dis = new DataInputStream[3];
    DataOutputStream[] worker_dos = new DataOutputStream[3];

    BlockingQueue<String> query_w[] = new LinkedBlockingQueue[3];
    BlockingQueue<String> response_w[] = new LinkedBlockingQueue[3];
    run_worker obj[] = new run_worker[3];

    // for the client and leader connection on port 8000
    DataInputStream dis = null;
    DataOutputStream dos = null;
    Socket[] socket_workers = new Socket[3];
    ServerSocket leader_soc = null;
    Socket soc = null;

    Leader(int port) throws IOException {
        leader_soc = new ServerSocket(port);
    }

    void connect_with_Workers() throws IOException {
        System.out.println("START to Connect with all the workers!!");
        for (int i = 0; i < port_array.length; i++) {
            InetAddress addr = InetAddress.getByName(address[i]);
            socket_workers[i] = new Socket(addr, port_array[i]);
            worker_dis[i] = new DataInputStream(socket_workers[i].getInputStream());
            worker_dos[i] = new DataOutputStream(socket_workers[i].getOutputStream());
        }
        System.out.println("DONE Connecting with all the workers!!");
    }

    void connect_with_client() throws IOException, InterruptedException {
        while (true) {
            soc = leader_soc.accept();
            System.out.println("Client connected!");
            dis = new DataInputStream(soc.getInputStream());
            dos = new DataOutputStream(soc.getOutputStream());

            // Listen to client's database query requests and give response to them
            communicate_with_client();
        }
    }

    void communicate_with_client() throws IOException, InterruptedException {
        // Send greetings
        for (int i = 0; i < 3; i++) {
            query_w[i] = new LinkedBlockingQueue<String>();
            response_w[i] = new LinkedBlockingQueue<String>();
        }
        for (int i = 0; i < 3; i++) {
            obj[i] = new run_worker(socket_workers[i], query_w[i], response_w[i]);
        }
        Thread w_Thread[] = new Thread[3];
        for (int i = 0; i < 3; i++) {
            w_Thread[i] = new Thread(obj[i]);
            w_Thread[i].start();
        }

        String leader_msg = "Please enter your MYSQL query to get results!";
        String client_msg = null;
        dos.writeUTF(leader_msg);
        while (true) {
            leader_msg = "";
            client_msg = dis.readUTF();
            if (client_msg.toLowerCase().equals("disconnect")) {
                System.out.println("Disconnected with the client as per their request!!");
                break;
            }
            leader_msg = execute_query(client_msg);
            dos.writeUTF(leader_msg);
        }
        soc.close();
    }

    boolean merge(String newline) {
        newline.trim();
        if (query_responses.contains(newline))
            return false;
        else {
            query_responses.add(newline);
            return true;
        }
    }

    String execute_query(String client_msg) throws InterruptedException {
        String response = "";

        if (client_msg.toLowerCase().contains("insert")) {
            int hash_id = get_hashed_id(client_msg);
            query_w[hash_id].put(client_msg);
            response = response_w[hash_id].take();
            if (response.toLowerCase().equals("error"))
                response = "Incorrect SQL query.Please try again.";
            else
                response = "Inserted";

        } else {
            String column_name = "";
            for (int i = 0; i < 3; i++) {
                query_w[i].put(client_msg);
            }
            boolean flag = false;
            for (int i = 0; i < 3; i++) {
                // .get() waits for the callable to returns some value
                String worker_response = response_w[i].take();
                if (flag || worker_response.toLowerCase().equals("error")) {
                    response = "Error occured on executing query. Please check the query.";
                    flag = true;
                } else {
                    String[] temp = worker_response.split("\n");
                    column_name = temp[0];
                    for (int j = 1; j < temp.length; j++) {
                        if (merge(temp[j])) {
                            response += temp[j] + "\n";
                        }
                    }
                    // response += temp[j]+'\n';
                }
            }
            if (!flag) {
                System.out.println(column_name);
                System.out.println(response);
                response = generate_table(column_name, response);
            }
        }
        query_responses.clear();
        return response;
    }

    String generate_table(String column_name, String response) {
        String[] columns = column_name.split("\\s+");
        int col = columns.length;
        response.trim();
        String[] responses = response.split("\\r?\\n");
        int[] size = new int[col];
        for (int i = 0; i < col; i++) {
            size[i] = columns[i].length() + 4;
        }
        for (int i = 0; i < responses.length; i++) {
            responses[i] = responses[i].trim();
            String[] values = responses[i].split("\\s+");
            for (int j = 0; j < values.length; j++) {
                size[j] = Math.max(size[j], values[j].length());
            }
        }
        String table = "";
        String header = "+";
        for (int i = 0; i < col; i++) {
            for (int j = 0; j < size[i]; j++)
                header += "-";
            if (i < col - 1)
                header += "-";
            else
                header += "+";
        }
        table = header + "\n" + "|";
        for (int i = 0; i < col; i++) {
            table = table + columns[i];
            for (int j = 0; j < size[i] - columns[i].length(); j++) {
                table += " ";
            }
            table = table + "|";
        }
        table += "\n" + header + "\n";

        for (int i = 0; i < responses.length; i++) {
            table += "|";
            String[] values = responses[i].split("\\s+");
            for (int j = 0; j < values.length; j++) {
                table = table + values[j];
                for (int k = 0; k < size[j] - values[j].length(); k++)
                    table += " ";
                table += "|";
            }
            table += "\n";
        }
        table += header + "\n";
        return table;
    }

    int get_hashed_id(String query) {
        int index;
        int valueskeyword;
        String idnumber;
        String[] aftersplit = query.split("\\s+");
        index = 0;
        valueskeyword = 3;
        if (aftersplit[3].contains("(")) {
            for (int i = 3; i < aftersplit.length; i++) {
                if (aftersplit[i].toLowerCase().contains("id")) {
                    index = i - 3;
                }
                if (aftersplit[i].toLowerCase().contains("values")) {
                    valueskeyword = i;
                    break;
                }
            }
        }
        String values = "";
        for (int i = valueskeyword + 1; i < aftersplit.length; i++) {
            values += aftersplit[i];
        }
        String[] valuessplit = values.split(",");
        idnumber = valuessplit[index];
        idnumber = idnumber.replace("(", "");
        idnumber = idnumber.replace(")", "");
        // hash idnumber
        int id_int = Integer.parseInt(idnumber);
        System.out.println("ID number is " + idnumber);
        return id_int % 3;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 8000;
        Leader leader = new Leader(port);
        leader.connect_with_Workers();
        leader.connect_with_client();
    }
}
