import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.WritableComparable;

public class Pattern implements WritableComparable<Pattern>{
	private String patt;
	private boolean type;
	private Vector<String> hooks;
	private Vector<String> targets;
	public Pattern(String patt, boolean type) {
		super();
		this.patt = patt;
		this.type = type;
		this.hooks = new Vector<>();
		this.targets = new Vector<>();
	}



	public void addHookTarget(String hook,String targrt){
		hooks.add(hook);
		targets.add(targrt);
	}

	public Vector<String> getHooks() {
		return hooks;
	}

	public Vector<String> getTargets() {
		return targets;
	}

	public boolean checkHookTarget(String w1,String w2){
		boolean b= false;
		if(hooks.contains(w1)&&targets.contains(w2)){
			return true;
		}
		return b;
	}


	@Override
	public String toString() {
		return patt;
	}

	public String getPatt() {
		return patt;
	}

	public boolean isType() {
		return type;
	}

	public void setType(boolean type) {
		this.type = type;
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
	public int compareTo(Pattern p) {
		if(p!=null){
			if(patt.equals(p.getPatt())==true)
				return 0;
		}
		return 1;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((patt == null) ? 0 : patt.hashCode());
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
		Pattern other = (Pattern) obj;
		if (patt == null) {
			if (other.patt != null)
				return false;
		} else if (!patt.equals(other.patt))
			return false;
		return true;
	}

}
