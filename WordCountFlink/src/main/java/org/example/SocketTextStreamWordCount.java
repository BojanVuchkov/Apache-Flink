package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * To start an example socket text stream on your local machine run netcat from
 * a command line: <code>nc -lk 9999</code>, where the parameter specifies the
 * port number.
 */
public class SocketTextStreamWordCount {
	public static void main(String[] args) throws Exception {

		if (args.length != 2){
			System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
			return;
		}

		String hostName = args[0];
		int port = Integer.parseInt(args[1]);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		DataStream<String> text = env.socketTextStream(hostName, port);

		DataStream<Tuple2<String, Integer>> counts =
		text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
						String[] tokens = value.toLowerCase().split("\\W+");
						// emit the pairs
						for (String token : tokens) {
								out.collect(new Tuple2<>(token, 1));
						}
					}
				})
				.keyBy(0)
				.sum(1);

		counts.print();
		env.execute("Java WordCount from SocketTextStream Example");
	}
}
