import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

public class AllCorpuses {
	HashMap<String,HookCorpus> h=new HashMap<>();
	
	
	public void AddCorpuses(String hook,HookCorpus h1) {
		h.put(hook,h1);

	}
	
	public HashMap<String, HookCorpus> getH() {
		return h;
	}

	public void setH(HashMap<String, HookCorpus> h) {
		this.h = h;
	}


}




