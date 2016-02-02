/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package teradataquery;

/**
 *
 * @author Panji Tri Atmojo
 */
import com.google.gson.Gson;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TeradataQuery {

    public String connurl = "jdbc:teradata://teradata.query.com";
    public String username = "username";
    public String password = "password";
    public String query = "";
    public String queryType = "";
    public int timeLimit = 5 * 60;
    public Connection conn = null;
    public PreparedStatement stmt = null;
    public ResultSet rs = null;
    public HashMap<String, Object> resultArray = new HashMap<>();
    public Boolean queryStatus = false;
    public Integer affectedRows = 0;

    public static void main(String[] args) throws Exception {

        TeradataQuery teradataQuery = new TeradataQuery();

        try {

            /*  get connection url from supplied args   */
            teradataQuery.getConnectionUrl(args);

            /*  get credentials from supplied args  */
            teradataQuery.getCredential(args);

            /*  get query from supplied args  */
            teradataQuery.getQuery(args);

            teradataQuery.analyze();

            /*  create connection  */
            teradataQuery.createConnection();

            /*  prepare statement  */
            teradataQuery.prepareStatement();

            /*  get results  */
            teradataQuery.getResult();

            /*  close connection    */
            teradataQuery.resultArray.put("status", "OK");

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);

            teradataQuery.resultArray.put("exception", sw.toString());
            teradataQuery.resultArray.put("status", "NOK");
        } finally {
            teradataQuery.closeConn();
        }

        teradataQuery.showResult();
    }

    public void createConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.teradata.jdbc.TeraDriver");
        this.conn = DriverManager.getConnection(this.connurl, this.username, this.password);
    }

    public void prepareStatement() throws SQLException {
        this.stmt = this.conn.prepareStatement(this.query);
    }

    public void getResult() throws SQLException {

        switch (this.queryType) {
            case "select":
                this.rs = this.stmt.executeQuery();
                break;
            case "insert":
                this.affectedRows = this.stmt.executeUpdate();
                break;
            case "update":
                this.affectedRows = this.stmt.executeUpdate();
                break;
            case "delete":
                this.affectedRows = this.stmt.executeUpdate();
                break;
            default:
                this.rs = this.stmt.executeQuery();
                break;
        }

        HashMap<Integer, Object> resultMap = new HashMap<>();

        ResultSetMetaData rsmd = rs.getMetaData();

        int columnsNumber = rsmd.getColumnCount();

        Integer rowNum = 0;
        while (this.rs.next()) {
            Integer columnCount = 0;
            HashMap<String, Object> rowResult = new HashMap<>();

            for (columnCount = 0; columnCount < columnsNumber; columnCount++) {
                rowResult.put(rsmd.getColumnName(columnCount + 1).toLowerCase(), rs.getString(columnCount + 1));
            }
            resultMap.put(rowNum, rowResult);
            rowNum++;
        }

        /* put the result into resultArray */
        this.resultArray.put("data", resultMap);
    }

    public void getCredential(String[] args) {
        this.username = args[1];
        this.password = args[2];
        this.resultArray.put("username", this.username);
    }

    public void getConnectionUrl(String[] args) {
        this.connurl = args[0];
        this.resultArray.put("connectionUrl", this.connurl);
    }

    public void getQuery(String[] args) {
        this.query = args[3];
        this.resultArray.put("query", this.query);
    }

    public void closeConn() throws SQLException {
        if (this.conn != null) {
            this.conn.close();
        }
    }

    public void showResult() {

        try {
            Gson gson = new Gson();
            String json = gson.toJson(this.resultArray);
            System.out.println(json);
        } catch (Exception e) {

        }
    }

    public void analyze() {
        String[] queryArray = this.query.split(";");

        if (queryArray.length == 0) {
            queryArray[0] = this.query;
        }

        int queryLength = queryArray.length;

        String lastQuery = queryArray[0].toLowerCase();
        String type = "others";

        Pattern select = Pattern.compile("^.*?select.*$");
        Pattern insert = Pattern.compile("^.*?insert.*$");
        Pattern update = Pattern.compile("^.*?update.*$");
        Pattern delete = Pattern.compile("^.*?delete.*$");

        Matcher m = select.matcher(lastQuery);
        if (m.matches()) {
            type = "select";
        }
        m = insert.matcher(lastQuery);
        if (m.matches()) {
            type = "insert";
        }
        m = update.matcher(lastQuery);
        if (m.matches()) {
            type = "update";
        }
        m = delete.matcher(lastQuery);
        if (m.matches()) {
            type = "delete";
        }

        this.queryType = type;

        this.resultArray.put("queryType", this.queryType);

    }

}
