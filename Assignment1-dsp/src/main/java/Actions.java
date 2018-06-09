public enum Actions {
    TERMINATE,
    URL,
    REVIEW,
    OUTPUT,
    WORKERNUM;

     public static  int fromInt ( String a){
        switch (a) {
            case "TERMINATE":
                return  0;
            case "URL":
                return 1;
            case "REVIEW":
                return 2;
            case "OUTPUT":
                return 3;
            case "WORKERNUM":
                return 4;
        }
        return -1;
    }
}


