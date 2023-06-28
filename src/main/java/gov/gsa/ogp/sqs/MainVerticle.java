package gov.gsa.ogp.sqs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class MainVerticle extends AbstractVerticle 
{
	static final String queueName = "sqs-kantares-test";
	static final int port = 8838;
	
	Map<String, Map<String, JsonObject>> cacheLookup = new HashMap<>();
	
	SqsClient sqsClient;
	String sqsUrl;

  @Override
  public void start(Promise<Void> startPromise) throws Exception 
  {
	  init();
	  
	  Router router = Router.router(vertx);
	  
	  router.get("/api/v1/refresh").handler(ctx -> {
		  this.refresh();
		  ctx.request().response()
		  	.putHeader("Content-Type", "application/text")
		  	.end("Data refreshed!");
	  });
	  
	  router.get("/api/v1/data").handler(ctx -> {
		  JsonObject allData = this.getAllData();
		  ctx.request().response()
		  	.putHeader("Content-Type", "application/json")
		  	.end(allData.encodePrettily());
	  });
	  
	  router.get("/api/v1/data/:type").handler(ctx -> {
		  String type = ctx.pathParam("type");
		  JsonArray dataArray = this.getDataByType(type);
		  ctx.request().response()
		  	.putHeader("Content-Type", "application/json")
		  	.end(dataArray.encodePrettily());
	  });
	  
	  router.get("/api/v1/data/:type/:id").handler(ctx -> {
		  String type = ctx.pathParam("type");
		  String id = ctx.pathParam("id");
		  JsonObject data = this.getData(type, id);
		  ctx.request().response()
		  	.putHeader("Content-Type", "application/json")
		  	.end(data.encodePrettily());
	  });
	  
	  router.post("/api/v1/message").handler(ctx -> {
		  ctx.request().bodyHandler(body -> {
			  String msg = body.toString();
			  sendMessage(msg);			  
		  });
		  
		  ctx.request().response()
		  	.putHeader("Content-Type", "application/text")
		  	.end("Message sent!");
	  });
	  
    vertx.createHttpServer().requestHandler(router).listen(port, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        System.out.println("HTTP server started on port: " +  port);
      } else {
        startPromise.fail(http.cause());
      }
    });
  }
  
  public void refresh()
  {
	  System.out.println("refresh: ...");
      ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder().queueUrl(sqsUrl).build();
      List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

      for (Message m : messages) 
      {
    	  System.out.println("\n" +m.body());
    	  
    	  JsonObject data = new JsonObject(m.body());
    	  String type = data.getString("changeType");
    	  JsonObject cdc = data.getJsonObject("cdc");
    	  String id = cdc.getString("id");
    	  Map<String, JsonObject> cache = ensureCache(type);
    	  cache.put(id, cdc);
    	  
    	  DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
    			  .queueUrl(sqsUrl)
    			  .receiptHandle(m.receiptHandle())
    			  .build();
    	  sqsClient.deleteMessage(deleteMessageRequest);
      }
  }
  
  public JsonObject getAllData()
  {
	  JsonObject result = new JsonObject();
	  for (String type : cacheLookup.keySet())
	  {
		  JsonArray dataArray = getDataByType(type);
		  result.put(type, dataArray);
	  }
	  
	  return result;
  }
  
  public JsonArray getDataByType(String type)
  {
	  Map<String, JsonObject> cache = cacheLookup.get(type);
	  if (cache == null)
		  return new JsonArray().add(new JsonObject().put("error", "Data not found: type = " + type));
	  
	  JsonArray result = new JsonArray();
	  for (JsonObject data: cache.values())
		  result.add(data);
	  
	  return result;
  }
  
  public JsonObject getData(String type, String id)
  {
	  Map<String, JsonObject> cache = cacheLookup.get(type);
	  if (cache == null)
		  return new JsonObject().put("error", "Data not found: type = " + type);
	  
	  JsonObject data = cache.get(id);
	  if (data == null)
		  return new JsonObject().put("error", "Data not found: type = " + type + ", id = " + id);
	  else
		  return data;
	  
  }
  
  public void sendMessage(String msg)
  {
	  System.out.println("sendMessage: msg = " + msg);
      SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
              .queueUrl(sqsUrl)
              .messageBody(msg)
              .build();
      sqsClient.sendMessage(sendMsgRequest);
  }
  
  private Map<String, JsonObject> ensureCache(String type)
  {
	  Map<String, JsonObject> cache = cacheLookup.get(type);
	  if (cache == null)
	  {
		  cache = new HashMap<>();
		  cacheLookup.put(type, cache);
	  }
	  
	  return cache;
  }
  
  private void init()
  {
	  sqsClient = SqsClient.builder().build();
	  GetQueueUrlRequest queuUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
	  GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(queuUrlRequest);
	  sqsUrl = getQueueUrlResponse.queueUrl();
	  System.out.println("sqsUrl: " + sqsUrl);
  }
}
