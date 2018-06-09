import java.util.List;

public class ReviewResponse {
    Review review;
    List<String> m;
    int sentiment;
    boolean sarcasm;

    public ReviewResponse(Review review, List<String> m, int sentiment, boolean sarcasm) {
        this.review = review;
        this.m = m;
        this.sentiment = sentiment;
        this.sarcasm=sarcasm;
    }

    public Review getReview() {
        return review;
    }

    public void setReview(Review review) {
        this.review = review;
    }

    public List<String> getM() {
        return m;
    }

    public void setM(List<String> m) {
        this.m = m;
    }

    public int getSentiment() {
        return sentiment;
    }

    public void setSentiment(int sentiment) {
        this.sentiment = sentiment;
    }

    public boolean isSarcasm() {
        return sarcasm;
    }

    public void setSarcasm(boolean sarcasm) {
        this.sarcasm = sarcasm;
    }

    @Override
    public String toString() {
        return "ReviewResponse{" +
                "review=" + review +
                ", m=" + m +
                ", sentiment=" + sentiment +
                ", sarcasm=" + sarcasm +
                '}';
    }
}
