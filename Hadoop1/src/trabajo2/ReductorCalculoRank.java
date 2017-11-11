package trabajo2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
public class ReductorCalculoRank extends Reducer<Text, Text, Text, Text> {
    private static final float amortiguamiento = 0.85F;
    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isExistingWikiPage = false;
        String[] split;
        float sumShareOtherPageRanks = 0;
        String links = "";
        String pageWithRank;
        
        // For each otherPage: 
        // - check control characters
        // - calculate pageRank share <rank> / count(<links>)
        // - add the share to sumShareOtherPageRanks
        for (Text value : values){
            pageWithRank = value.toString();
            System.out.println("red22pageWithRank:"+pageWithRank);
            
            if(pageWithRank.equals("!")) {
                isExistingWikiPage = true;
                continue;
            }
            
            if(pageWithRank.startsWith("|")){
                links = "\t"+pageWithRank.substring(1);
                continue;
            }

            split = pageWithRank.split("\\t");
            
            float pageRank = Float.valueOf(split[1]);
            int countOutLinks = Integer.valueOf(split[2]);
            
            sumShareOtherPageRanks += (pageRank/countOutLinks);
        }

        if(!isExistingWikiPage) return;
        System.out.println("red22sumShareOtherPageRanks:"+sumShareOtherPageRanks);
        float newRank = amortiguamiento * sumShareOtherPageRanks + (1-amortiguamiento);

        context.write(page, new Text(newRank + links));
        System.out.println("red22page:"+page+"...newRank:"+newRank+"...links:"+links);
    }
}
