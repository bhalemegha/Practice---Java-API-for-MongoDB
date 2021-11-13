package upgrad;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;

import java.sql.*;
import java.util.Arrays;

import static com.mongodb.client.model.Aggregates.count;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

public class CRUDHelper {

    /**
     * Display ALl products
     * @param collection
     */
    public static void displayAllProducts(MongoCollection<Document> collection) {
        System.out.println("------ Displaying All Products ------");
        for(Document doc : collection.find()) {
            PrintHelper.printSingleCommonAttributes(doc);
        }
        //Call printSingleCommonAttributes to display the attributes on the Screen

    }

    /**
     * Display top 5 Mobiles
     * @param collection
     */
    public static void displayTop5Mobiles(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Top 5 Mobiles ------");
        for(Document doc : collection.find(eq("Category", "Mobiles")).limit(5)) {
            PrintHelper.printAllAttributes(doc);
        }
        // Call printAllAttributes to display the attributes on the Screen
    }

    /**
     * Display products ordered by their categories in Descending order without auto generated Id
     * @param collection
     */
    public static void displayCategoryOrderedProductsDescending(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Products ordered by categories ------");
        for(Document doc : collection.find().sort((descending("category")))) {
            doc.remove("_id");
            PrintHelper.printAllAttributes(doc);
        }
        // Call printAllAttributes to display the attributes on the Screen
    }


    /**
     * Display number of products in each group
     * @param collection
     */
    public static void displayProductCountByCategory(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Product Count by categories ------");
        for(Document document : collection.aggregate(
                Arrays.asList(
                        Aggregates.group("$Category", Accumulators.sum("Count", 1))
                ))){
            PrintHelper.printProductCountInCategory(document);
        }
        // Call printProductCountInCategory to display the attributes on the Screen
    }

    /**
     * Display Wired Headphones
     * @param collection
     */
    public static void displayWiredHeadphones(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Wired headphones ------");
        for(Document doc : collection.find(and(eq("Category","Headphones"),ne("ConnectorType","Wireless")))){
            PrintHelper.printAllAttributes(doc);
        }
        // Call printAllAttributes to display the attributes on the Screen
    }
}