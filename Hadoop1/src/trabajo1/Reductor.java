package trabajo1;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
public class Reductor extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
        String nota_salidas = "1.0\t" ;//inician con 1
        boolean primero = true;
        System.out.println("red11Llave: "+key.toString());
        for (Text valor : values) {
            if(!primero) {
                nota_salidas += ",";//se le agrega coma y la 
            }
            nota_salidas += valor.toString();
            primero = false;
        }
        System.out.println("red11nota_salidas:"+nota_salidas);
        context.write(key, new Text(nota_salidas));
    }
}
