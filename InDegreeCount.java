package example;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.graph.BasicComputation;

public class InDegreeCount extends BasicComputation<IntWritable, IntWritable,
NullWritable, IntWritable> {

  @Override
public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex,
		Iterable<IntWritable> messages) {
	if (getSuperstep() == 0) {
		Iterable<Edge<IntWritable, NullWritable>> edges = vertex.getEdges();
		for (Edge<IntWritable, NullWritable> edge : edges) {
			sendMessage(edge.getTargetVertexId(), new IntWritable(1));
		}
	}
	else if(  getSuperstep() == 1){
		Iterable<Edge<IntWritable,NullWritable>> edges=vertex.getEdges();
		for(Edge<IntWritable,NullWritable> edge:edges){
			int sum=0;
			for(IntWritable msg:messages){
			sum+=msg.get();
			}
			sum=sum+1;
			sendMessage(edge.getTargetVertexId(),new IntWritable(sum));
		}
	}
	else {
		int sum = 0;
		for (IntWritable message : messages) {
			sum+=message.get();
		}
		IntWritable vertexValue = vertex.getValue();
		vertexValue.set((int) sum);
		vertex.setValue(vertexValue);
		vertex.voteToHalt();
	}
}

}
