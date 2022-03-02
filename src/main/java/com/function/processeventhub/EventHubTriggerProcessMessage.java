package com.function.processeventhub;

import com.microsoft.azure.functions.annotation.*;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.microsoft.azure.functions.*;
import java.util.*;

/**
 * Azure Functions with Event Hub trigger.
 */
public class EventHubTriggerProcessMessage {
    /**
     * This function will be invoked when an event is received from Event Hub.
     */
    @FunctionName("EventHubTriggerProcessMessage")
    public void run(
        @EventHubTrigger(name = "message", eventHubName = "blockchain-usd-btc-price", connection = "msblog_javaapp_EVENTHUB", consumerGroup = "$Default", cardinality = Cardinality.MANY) List<String> message,
        @CosmosDBOutput(
            name = "msblogcosmos",
            databaseName = "blockchain-price",
            collectionName = "blockchainPrices",
            connectionStringSetting = "CosmosDBConnectionString")
            OutputBinding<BlockchainPrice> document,
        final ExecutionContext context
    ) {
        context.getLogger().info("Java Event Hub trigger function executed.");
        context.getLogger().info("Length:" + message.size());
        message.forEach(singleMessage -> {
            //context.getLogger().info(singleMessage);
            // convert the message to a JSON Object
            JSONObject json = new JSONObject(singleMessage);
            // Parse out the price array
            JSONArray priceArr = json.getJSONArray("price");
            // put each item in the array into its own property
            json.put("timestamp",priceArr.get(0));
            json.put("openPrice",priceArr.get(1));
            json.put("highPrice",priceArr.get(2));
            json.put("lowPrice",priceArr.get(3));
            json.put("closePrice",priceArr.get(4));
            json.put("volume",priceArr.get(5));
            // remove the price array
            json.remove("price");
            // convert the json object into a Java Object and store in CosmosDB
            Gson g = new Gson();
            BlockchainPrice price = g.fromJson(json.toString(), BlockchainPrice.class);
            context.getLogger().info(price.toString());

            document.setValue(price);
        });
    }
}
