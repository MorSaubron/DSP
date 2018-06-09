import java.io.File;

public class Delete {
	public static boolean deleteDirectory(String pathname) {
		File directory=new File(pathname);
		if(directory.exists()){
	        File[] files = directory.listFiles();
	        if(null!=files){
	            for(int i=0; i<files.length; i++) {
	                    files[i].delete();
	            }
	        }
	    }
	    return(directory.delete());
	}
	
	public static void main(String[] args) throws Exception {
		deleteDirectory("/output1/");
		deleteDirectory("/output2/");
		deleteDirectory("/output3/");
		deleteDirectory("/output4/");
		deleteDirectory("/output5/");
	}
}
