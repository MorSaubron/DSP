import java.util.ArrayList;

public class ReviewMsg  extends Msg {
    ArrayList<Review> reviews= new ArrayList<Review>();
    String title;
    private String fileName;

    public ReviewMsg(ArrayList<Review> reviews, String title) {
        super.action=Actions.REVIEW.ordinal();
        this.reviews = reviews;
        this.title = title;
    }


    public ArrayList<Review> getReviews() {
        return reviews;
    }

    public void setReviews(ArrayList<Review> reviews) {
        this.reviews = reviews;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
