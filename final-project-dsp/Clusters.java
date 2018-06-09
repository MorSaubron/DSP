import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Vector;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.WritableComparable;

public class Clusters implements WritableComparable<Clusters>, Comparable<Clusters> {
	String target;
	protected HashSet<Pattern> corePatterns;
	protected HashSet<Pattern> unconfirmedPatterns;
	

	public Clusters(String target) {
		super();
		this.target = target;
		corePatterns=new HashSet<>();
		unconfirmedPatterns=new HashSet<>();
	}

	public boolean  unconfirmedPatternsOnly(){
		return (corePatterns.size()==0 && unconfirmedPatterns.size()!=0);
			
		
	}
	public String getTarget() {
		return target;
	}

	public void AddCorePattern(Pattern p){
		corePatterns.add(p);
	}

	public void AddUnconfimedPattern(Pattern p){
	
		unconfirmedPatterns.add(p);
		
	}

	public HashSet<Pattern> getCorePatterns() {
		return corePatterns;
	}

	public HashSet<Pattern> getUnconfirmedPatterns() {
		return unconfirmedPatterns;
	}

	public int getNumPatterns(){
		return corePatterns.size()+unconfirmedPatterns.size();
	}

	public boolean CheckIfEquals( HashSet<Pattern> corePatterns1){
		boolean b=false;
		if(corePatterns1.equals(corePatterns)){
			b=true;
		}
		return b;
	}
	public void deletePattern(ArrayList<Pattern> patt){
		for(Pattern p: patt){
			unconfirmedPatterns.remove(p);
		}
	}
	
	public void deletePattern (Pattern p){
		unconfirmedPatterns.remove(p);
	}
	public boolean CheckIfHalfEquals( HashSet<Pattern> unConfirmed){
		boolean b=false;
		int count=0;
		int size= unConfirmed.size();
		
		if (size == 0){
			return b;
		}

		for (Pattern pattern : unConfirmed) {
			for(Pattern p:unconfirmedPatterns){
				if(pattern.compareTo(p)==0){
					count++;
				}
			}
		} 
		if(((double)count)/(double)size >=  2.0 / 3.0){
			b=true;
		}
		return b;
	}

	@Override
	public String toString() {
		String s="";
		for (Pattern pattern : corePatterns) {
			s=s+pattern.toString()+" ";
		}
		for (Pattern pattern : unconfirmedPatterns) {
			s=s+pattern.toString()+" ";
		}

		return  target +s;
	}

	public Double calcTheHits(String w1,String w2){
		int appearsInPcore = 0;
		int appearsInPunconf = 0;
		
		for(Pattern p:corePatterns){	
			if (p.checkHookTarget(w1, w2)){
				appearsInPcore++;
			}
			else{
				if(p.checkHookTarget(w2, w1)){
					appearsInPcore++;
				}
			}
		}
		for (Pattern p : unconfirmedPatterns){
			if (p.checkHookTarget(w1, w2)){
				appearsInPunconf++;
			}
			else{
				if(p.checkHookTarget(w2, w1)){
					appearsInPunconf++;
				}
			}
		}

		double hit = 0.0;
		double core = 0.0;
		if(!(corePatterns.size()==0)){
			core = ((double)appearsInPcore) /corePatterns.size();
			
		}
		double unco = 0.0;
		if(!(unconfirmedPatterns.size()==0)){
			unco = (((double)appearsInPunconf) / unconfirmedPatterns.size());
			
		}
		
		
		hit = core + 0.1* unco;
		if(hit!=0){
			System.out.println("yes");
		}
		return hit;

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
	public int compareTo(Clusters o) {
		return this.unconfirmedPatterns.size() - o.unconfirmedPatterns.size();
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((target == null) ? 0 : target.hashCode());
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
		Clusters other = (Clusters) obj;
		if (target == null) {
			if (other.target != null)
				return false;
		} else if (!target.equals(other.target))
			return false;
		return true;
	}
}