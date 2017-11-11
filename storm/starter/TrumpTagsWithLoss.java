package storm.starter;

import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.HashMap;
import java.util.Iterator;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.HashtagEntity;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

public class TrumpTagsWithLoss{
	public static long startTime;
	public static long totalNum;
	public static final long interval = 600000;
	
	public static class tagCount extends BaseRichBolt{
		Map<String, Integer> counts = new HashMap<String, Integer>();
		OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector){
            _collector = collector;
        }

        @Override
		public void execute(Tuple tuple){
			String word = tuple.getString(0);
            if(System.currentTimeMillis() % 10 != 0){
			    TrumpTagsWithLoss.totalNum++;
			    Integer count = counts.get(word);
			    if(count == null)
				    count = 0;
			    count++;
			    counts.put(word, count);
            }
            else{
                _collector.fail(tuple);
            }
            _collector.ack(tuple);

			if(System.currentTimeMillis() - startTime > TrumpTagsWithLoss.interval){
				Iterator<Map.Entry<String, Integer>> it = counts.entrySet().iterator();
				PrintWriter writer = null;
                try{
                    writer = new PrintWriter(new FileOutputStream(new File("Results.txt")), true);
                }
                catch(IOException e){
                
                }
				while(it.hasNext()){
					Map.Entry<String, Integer> entry = it.next();
					float frequency = (float) entry.getValue() / TrumpTagsWithLoss.totalNum;
					if(frequency > 0.01 && frequency < 1){
					    System.out.println("Results: " + entry.getKey() + " " + frequency);
                    }
				}
				writer.println("-------------------------------");
                writer.close();
				startTime = System.currentTimeMillis();
				totalNum = 0;
				counts.clear();
			}
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer){
			declarer.declare(new Fields("word", "count"));
		}
	}
	
	public static void main(String[] args) throws Exception{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TwitterSpout(), Integer.parseInt(args[1]));
		TrumpTagsWithLoss.startTime = System.currentTimeMillis();
		TrumpTagsWithLoss.totalNum = 0;
		builder.setBolt("count", new tagCount(), 12).shuffleGrouping("spout");
		
		Config conf = new Config();
		conf.setDebug(true);
		if(args  != null && args.length > 1){
            conf.setNumWorkers(20);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else{
		    conf.setMaxTaskParallelism(12);
		    LocalCluster cluster = new LocalCluster();
		    cluster.submitTopology("TrumpTagsWithLoss", conf, builder.createTopology());
		
		    Thread.sleep(10000);
		    //cluster.shutdown();
        }
	}
}

class TwitterSpout extends BaseRichSpout{
	SpoutOutputCollector _collector;
	LinkedBlockingQueue<String> queue = null;
    Map<String, String> message = new HashMap<String, String>();
	TwitterStream _twitterStream;
	String consumerKey = "GhRoCN3Lxwrk7EcAfWLMaejtj";
	String consumerSecret = "9caL3vGsRnLCK5189mYhYV9xj02j097yy657BuntPGcSuZUCQA";
	String accessToken = "834729843723956224-OYLn7R7WUe3zcGt2vylojc3ZHIB1LP0";
	String accessTokenSecret = "kCT0UnauUSnIWnDkuCVAEGuLumpwYWdyfAWUNaB5m9dEo";
	String[] keyWords = {"trump"};

	public TwitterSpout(){}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
		queue = new LinkedBlockingQueue<String>(100000);
		_collector = collector;
		
		StatusListener listener = new StatusListener(){
			@Override
			public void onStatus(Status status){
				HashtagEntity[] hashtagEntities = status.getHashtagEntities();
				for(HashtagEntity hashtag: hashtagEntities){
					String str = hashtag.getText().toLowerCase();
					queue.offer(str);
				}
			}
			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn){}
			@Override
			public void onTrackLimitationNotice(int i){}
			@Override
			public void onScrubGeo(long l, long ll){}
			@Override
			public void onException(Exception ex){}
			@Override
			public void onStallWarning(StallWarning arg0){}
		};
		
		TwitterStream twitterStream = new TwitterStreamFactory(new ConfigurationBuilder().setJSONStoreEnabled(true).build()).getInstance();
		twitterStream.addListener(listener);
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);
		
		FilterQuery query = new FilterQuery().track(keyWords);
		twitterStream.filter(query);
	}
	
	@Override
	public void nextTuple(){
		String ret = queue.poll();
		if(ret == null){
			Utils.sleep(50);
		}
        else{
            String msgId = ret.hashCode() + "_" + System.currentTimeMillis();
			message.put(ret, msgId);
            _collector.emit(new Values(ret), msgId);
		}
	}
	
	@Override
	public void close(){
		_twitterStream.shutdown();
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration(){
		Config ret = new Config();
		ret.setMaxTaskParallelism(20);
		return ret;
	}
	
	@Override
	public void ack(Object id){
        String msgId = (String) id;
        message.remove(msgId);
    }

	@Override
	public void fail(Object id){
        String msgId = (String) id;
        String ret = message.get(msgId);
        _collector.emit(new Values(ret), msgId);
    }
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields("tweet"));
	}
}
