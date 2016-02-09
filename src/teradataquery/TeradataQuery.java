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
import java.awt.List;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    public HashMap<Integer, Object> resultSet = new HashMap<>();
    public Boolean queryStatus = false;
    public Integer affectedRows = 0;
    public String[] queryArray = null;

    public static void main(String[] args) throws Exception {

        TeradataQuery teradataQuery = new TeradataQuery();

        try {

            /*  get connection url from supplied args   */
            teradataQuery.getConnectionUrl(args);

            /*  get credentials from supplied args  */
            teradataQuery.getCredential(args);

            /*  get query from supplied args  */
            teradataQuery.getQuery(args);

            /*  create connection  */
            teradataQuery.createConnection();

            /*  break the query into query array    */
            teradataQuery.breakQuery();

            for (int i = 0; i < teradataQuery.queryArray.length; i++) {

                teradataQuery.query = teradataQuery.queryArray[i];

                /*  analyze the type of the query   */
                teradataQuery.analyze();

                /*  prepare statement  */
                teradataQuery.prepareStatement();

                /*  get results  */
                teradataQuery.getResult();

                /*  close connection    */
                teradataQuery.resultArray.put("status", "OK");

                teradataQuery.resultSet.put(i, teradataQuery.resultArray);
            }
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

            case "insert":
                this.affectedRows = this.stmt.executeUpdate();
                break;
            case "update":
                this.affectedRows = this.stmt.executeUpdate();
                break;
            case "delete":
                this.affectedRows = this.stmt.executeUpdate();
                break;
            case "select":
                this.rs = this.stmt.executeQuery();
                break;
            default:
                this.queryStatus = this.stmt.execute();
                break;
        }

        HashMap<Integer, Object> resultMap = new HashMap<>();

        if (this.queryType.equals("select")) {
            /*  if query type is select then provide result set */

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
        } else if (this.queryType.equals("multiple")) {
            HashMap<String, Object> rowResult = new HashMap<>();
            rowResult.put("queryStatus", this.queryStatus);
            resultMap.put(0, rowResult);
        } else {
            HashMap<String, Object> rowResult = new HashMap<>();
            rowResult.put("affectedRows", this.affectedRows);
            resultMap.put(0, rowResult);
        }
        /* put the result into resultArray */
        this.resultArray.put("data", resultMap);
        
        this.stmt.close();
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

    public void getQuery(String[] args) throws IOException {

        Pattern file = Pattern.compile("^file:(.*)$", Pattern.CASE_INSENSITIVE);

        Matcher m = file.matcher(args[3]);

        if (m.matches()) {

            String fileSource = m.group(1);

            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(fileSource));

                StringBuilder sb = new StringBuilder();
                String line = br.readLine();

                while (line != null) {
                    sb.append(line);
                    sb.append(System.lineSeparator());
                    line = br.readLine();
                }

                this.query = sb.toString();

            } catch (FileNotFoundException ex) {
                Logger.getLogger(TeradataQuery.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(TeradataQuery.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        } else {
            this.query = args[3];
        }
        //this.resultArray.put("query", this.query);
    }

    public void closeConn() throws SQLException {
        if (this.conn != null) {
            this.conn.close();
        }
    }

    public void showResult() {

        try {
            Gson gson = new Gson();
            String json = gson.toJson(this.resultSet);
            System.out.println(json);
        } catch (Exception e) {

        }
    }

    public void breakQuery() {
        /*  split the query based limit by ";"  */
        this.queryArray = this.query.split(";");

    }

    public void analyze() {

        String type = "others";

        Pattern select = Pattern.compile("^.*?select.*$", Pattern.CASE_INSENSITIVE);
        Pattern insert = Pattern.compile("^.*?insert.*$", Pattern.CASE_INSENSITIVE);
        Pattern update = Pattern.compile("^.*?update.*$", Pattern.CASE_INSENSITIVE);
        Pattern delete = Pattern.compile("^.*?delete.*$", Pattern.CASE_INSENSITIVE);

        Matcher m = select.matcher(this.query);
        if (m.matches()) {
            type = "select";
        }
        m = insert.matcher(this.query);
        if (m.matches()) {
            type = "insert";
        }
        m = update.matcher(this.query);
        if (m.matches()) {
            type = "update";
        }
        m = delete.matcher(this.query);
        if (m.matches()) {
            type = "delete";
        }

        this.queryType = type;

        this.resultArray.put("queryType", this.queryType);

    }

}
