package upgrad;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.sun.org.apache.xalan.internal.xsltc.runtime.Operators;
import org.bson.Document;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Driver {

    /**
     * Driver class main method
     * @param args
     * @throws SQLException
     */

    //Import all Data in MOngo DB
    public static MongoCollection<Document> importDataToMongoDB(Connection sqlConnection, MongoCollection<Document> collection) throws SQLException {
        List<Document> docs = new ArrayList<Document>();
        Statement statement = sqlConnection.createStatement();
        String selectMobiles = "select * from mobiles";
        ResultSet rs = statement.executeQuery(selectMobiles);
        ResultSetMetaData rsMetaData = rs.getMetaData();

        //Adding docs for Mobiles
        docs = addDocument(docs, rs, rsMetaData, "Mobiles");
        rs.close();

        statement = sqlConnection.createStatement();
        String selectHeadphones = "select * from headphones";
        rs = statement.executeQuery(selectHeadphones);
        rsMetaData = rs.getMetaData();
        //Adding docs for Headphones
        docs = addDocument(docs,rs,rsMetaData,"Headphones");
        rs.close();

        statement = sqlConnection.createStatement();
        String selectCameras = "select * from cameras";
        rs = statement.executeQuery(selectCameras);
        rsMetaData = rs.getMetaData();
        //Adding docs for Cameras
        docs = addDocument(docs,rs,rsMetaData,"Cameras");
        //insert all documents to DB
        collection.insertMany(docs);

        rs.close();
        statement.close();
        return collection;
    }

    //Add all mySql data to documents
    public static List<Document> addDocument(List<Document> docs,ResultSet rs, ResultSetMetaData rsMetaData, String category) throws SQLException {
        while(rs.next()) {
            Document document  = new Document();
            document.append("Category", category);
            for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                document.append(rsMetaData.getColumnName(i), rs.getString(i));
            }
            docs.add(document);
        }
        return docs;
    }

    public static void main(String[] args) throws SQLException {
        // MySql credentials
        String userName = "student";//
        String password = "STUDENT123";
        //mySql Connection String
        String url = "jdbc:mysql://ADD CONNECTION STRING HERE";
        // MongoDB Configurations
        String mongoUrl = "mongodb://ADD MONGO ENDPOINT HERE";
        List<Document> docs = new ArrayList<Document>();

        // Connection Default Value Initialization
        Connection sqlConnection = null;
        MongoClient mongoClient = null;

        try {
            // Creating database connections
            sqlConnection = DriverManager.getConnection(url,userName,password);
            mongoClient = MongoClients.create(mongoUrl);

            //Create MongoDB
            MongoDatabase database = mongoClient.getDatabase("<ADD DB NAME>");

            if (sqlConnection != null) {
                //It will create collection products
                // Import data into MongoDb
                MongoCollection<Document> collection = database.getCollection("products");
                collection = importDataToMongoDB(sqlConnection,collection);
                // List all products in the inventory
                CRUDHelper.displayAllProducts(collection);

                // Display top 5 Mobiles
                CRUDHelper.displayTop5Mobiles(collection);

                // Display products ordered by their categories in Descending Order Without autogenerated Id
                CRUDHelper.displayCategoryOrderedProductsDescending(collection);

                // Display product count in each category
                CRUDHelper.displayProductCountByCategory(collection);

                // Display wired headphones
                CRUDHelper.displayWiredHeadphones(collection);
            }
        }
        catch(Exception ex) {
            System.out.println("Got Exception.");
            ex.printStackTrace();
        }
        finally {
            mongoClient.close();
            sqlConnection.close();
            // Close Connections
        }
    }
}