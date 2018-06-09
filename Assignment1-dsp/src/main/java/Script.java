import org.apache.commons.codec.binary.Base64;



public class Script {

    private String managerScript;
    private String workersScript;


    Script(){

    }

    public void setManagerScript() {
        StringBuilder managerBuild = new StringBuilder();
        managerBuild.append("#!/bin/sh\n");
        managerBuild.append("aws s3 cp s3://akiaisatt3u2kglwoipq-c--users-mor-ideaprojects-assignment1/ManagerDep.zip ManagerDep.zip\n");
        managerBuild.append("unzip -P dbhfbb389832488 ManagerDep.zip\n");
        managerBuild.append("aws s3 cp s3://akiaisatt3u2kglwoipq-c--users-mor-ideaprojects-assignment1/manager.zip manager.zip\n");
        managerBuild.append("unzip -P dbhfbb389832488 manager.zip\n");
        managerBuild.append("java -jar manager.jar\n");

        this.managerScript = new String(Base64.encodeBase64(managerBuild.toString().getBytes()));
    }



    public void setWorkersScript() {
        StringBuilder workersBuild = new StringBuilder();
        workersBuild.append("#!/bin/sh\n");
        workersBuild.append("aws s3 cp s3://akiaisatt3u2kglwoipq-c--users-mor-ideaprojects-assignment1/WorkersDep.zip WorkersDep.zip\n");
        workersBuild.append("unzip -P dgf7465gdgf5gfy6s WorkersDep.zip\n");
        workersBuild.append("aws s3 cp s3://akiaisatt3u2kglwoipq-c--users-mor-ideaprojects-assignment1/workers.zip workers.zip\n");
        workersBuild.append("unzip -P dgf7465gdgf5gfy6s workers.zip\n");
        workersBuild.append("java -jar workers.jar\n");
        this.workersScript = new String(Base64.encodeBase64(workersBuild.toString().getBytes()));
    }

    public String getManagerScript() {

        return managerScript;
    }

    public String getWorkersScript() {

        return workersScript;
    }
}