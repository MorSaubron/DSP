import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Vector;

import org.apache.hadoop.io.WritableComparable;

public class HookCorpus implements WritableComparable<HookCorpus> {
	private String hook;
	protected HashSet<Clusters> clusters;
	protected ArrayList<Clusters> sortedClusters;
	protected boolean visited = false;


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hook == null) ? 0 : hook.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HookCorpus other = (HookCorpus) obj;
		if (hook == null) {
			if (other.hook != null)
				return false;
		} else if (!hook.equals(other.hook))
			return false;
		return true;
	}
	
	public HookCorpus(String hook) {
		super();
		this.hook = hook;		
		clusters=new HashSet<>();
	}
	
	public void AddCluster(Clusters c){
		clusters.add(c);
	}

	public String getHook() {
		return hook;
	}

	public HashSet<Clusters> getClusters() {
		return clusters;
	}
	
	public boolean CehckIfClustersEx(){
		boolean b=false;
		for (Clusters cluster : clusters) {
			if(cluster.unconfirmedPatternsOnly()){
				return true;
			}
		}
		return b;
	}

	public Clusters minimalClus(){
		if (null == sortedClusters){
			sortedClusters = new ArrayList<>(clusters);
			Collections.sort(sortedClusters);
		}
		
		Clusters retCluster = new Clusters(" ");
		do{
			retCluster = sortedClusters.get(0);
			sortedClusters.remove(0);
		}while(sortedClusters.size()> 0 && !retCluster.unconfirmedPatternsOnly());
		
		if (!retCluster.unconfirmedPatternsOnly()){
			retCluster = new Clusters(" ");
		}
		return retCluster;
		
		
//		int size=0;
//		Clusters c= new Clusters(" ");
//		for (Clusters cluster : clusters) {
//			if(cluster.unconfirmedPatternsOnly() && cluster.getNumPatterns()>size){
//				size=cluster.getNumPatterns();
//				c=cluster;
//			}
//		}
//		return c;
	}
	
	public Clusters getClus(String target){
		Clusters c= new Clusters(" ");
		for (Clusters cluster : clusters) {
			if(cluster.getTarget().equals(target)){
				c=cluster;
				return c;
			}
		}
		return c;
	}

	public boolean canWeMerged(Clusters c2, Clusters c1){
		if((c1.CheckIfEquals(c2.getCorePatterns()))&&(c1.CheckIfHalfEquals(c2.getUnconfirmedPatterns()))){
			merge(c2,c1);
			return true;
		}
		return false;
	}


	public void merge(Clusters c2,Clusters c1){
		HashSet<Pattern> inter = new HashSet<>(c2.getUnconfirmedPatterns());
		inter.addAll(c2.getCorePatterns());
		for(Pattern pi:inter){
			for(Pattern p1:c1.getUnconfirmedPatterns() ){
				if(p1.compareTo(pi)==0){
					p1.setType(true);
					//p1.addHookTarget(pi.getFirstHook(), pi.getFirstTarget());
					p1.getHooks().addAll(pi.getHooks());
					p1.getTargets().addAll(pi.getTargets());
					c1.AddCorePattern(p1);
				}
			}		
		}
		c1.getUnconfirmedPatterns().addAll(c2.getCorePatterns());
		c1.getUnconfirmedPatterns().addAll(c2.getUnconfirmedPatterns());
		ArrayList<Pattern> del=new ArrayList<>();
		for(Pattern pU:c1.getUnconfirmedPatterns()){
			for(Pattern pC:c1.getCorePatterns() ){
				if(pU.compareTo(pC)==0){
					del.add(pC);
				}
			}
		}
		c1.deletePattern(del);
		for (Pattern pattern : c1.getUnconfirmedPatterns()) {
			pattern.setType(false);
		}

	}

	public void DeleteClusters(ArrayList<Clusters> clus) {
		for(Clusters c:clus){
			clusters.remove(c);	
		}
	}
	
	public void DeleteCluster(Clusters c) {
		clusters.remove(c);	
	}
	
	public boolean equalsTo(HookCorpus h1) {
		if(h1.getHook().equals(hook))
			return true;

		return false;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}
	
	@Override
	public int compareTo(HookCorpus o) {
		return o.getHook().compareTo(hook);	
	}
	
	public void setVisited(){
		visited = true;
	}
	
	public boolean getVisited(){
		return visited;
	}

}
