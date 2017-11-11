package tendencias;     
import org.apache.hadoop.io.Text;
import trabajo1.Reductor;
import trabajo1.Mapeador;
import trabajo1.FormatoEntradaXml;
import trabajo2.MapeadorCalculoRank;
import trabajo2.ReductorCalculoRank;
import trabajo3.MapeadorRanking;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class PrincipalPageRank extends Configured implements Tool {
    
    private static NumberFormat numero = new DecimalFormat("000");
    static final String pagina="page";
    
    public static void main(String[] args) throws Exception {
        System.out.println("Inicio de ejecución (Tendencias)...");
        //System.exit(ToolRunner.run(new Configuration(), new PrincipalPageRank(), args));
        int resultado=(ToolRunner.run(new Configuration(), new PrincipalPageRank(), args));        
        System.out.println("Num:"+resultado);
        System.out.println("Fin de ejecución. ");
    }
    @Override
    public int run(String[] args) throws Exception { 
        int iteraciones=6;
        String rutaEntrada=args[0];
        String carpeta_salida=args[1];         
        String out="out";
        
        String rutaSalida=carpeta_salida+out+"000";      
        System.out.println("antes de runXmlParsing.");
        boolean terminado = runXmlParsing(rutaEntrada,rutaSalida );
        if (!terminado){
            System.out.println("Se va a retornarr 1");
            return 1;
        }
        String lastResultPath = null;
        System.out.println("antes de ciclo de n iteraciones");        
        for (int runs = 0; runs < iteraciones; runs++) {
            System.out.println("dentro del ciclo en la iteración "+runs);
            
            String inPath =carpeta_salida+out+ numero.format(runs);//iter00,iter01,... antes era "wiki/ranking/iter" + nf.format(runs)
            //String inPath ="c://navi/hadoop/nlwiki-latest-pages-articles/out/"+"iter"+ nf.format(runs);//iter00,iter01,... antes era "wiki/ranking/iter" + nf.format(runs)
            //String inPath = "wiki/ranking/iter" + nf.format(runs);//ruta de entrada           
            //esta ruta anterior no existe...err
            
            System.out.println("------------------------inPath:"+inPath);
            
            //lastResultPath = "wiki/ranking/iter" + nf.format(runs + 1);//ruta de último resultado
            lastResultPath = carpeta_salida+out + numero.format(runs + 1);//iter01, iter02,... antes era "wiki/ranking/iter" + nf.format(runs + 1)
            //lastResultPath = "C:\\navi\\hadoop\\nlwiki-latest-pages-articles\\out\\"+"iter" + nf.format(runs + 1);//iter01, iter02,... antes era "wiki/ranking/iter" + nf.format(runs + 1)
            //ruta de último resultado              
            
            System.out.println("----------------------lastResultPath__:"+lastResultPath);
            
            terminado = runRankCalculation(inPath, lastResultPath);//err

            if (!terminado) {
                System.out.println("se va a reetornar 1...");
                return 1;
            }
        }
        
        String rutaAdicionalResult= carpeta_salida+"resultado";//result antes era "wiki/result"
        //String rutaAdicionalResult="C:\\navi\\hadoop\\nlwiki-latest-pages-articles\\out\\"+"result";//result antes era "wiki/result"
        //isCompleted = runRankOrdering(lastResultPath, "wiki/result");
        
        terminado = runRankOrdering(lastResultPath, rutaAdicionalResult);

        if (!terminado) {
            System.out.println("se va a retornar 1");
            return 1;
        }
        //System.out.println("se va a retornar 0");
        //return 0;
        System.out.println("El proceso ha terminado.");
        return 0;
    }


    public boolean runXmlParsing(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(FormatoEntradaXml.START_TAG_KEY, "<"+pagina+">");
        conf.set(FormatoEntradaXml.END_TAG_KEY, "</" + pagina+">");

        Job xmlHakker = Job.getInstance(conf, "xmlHakker");
        xmlHakker.setJarByClass(PrincipalPageRank.class);

        // Input / Mapper
        FileInputFormat.addInputPath(xmlHakker, new Path(inputPath));
        xmlHakker.setInputFormatClass(FormatoEntradaXml.class);
        xmlHakker.setMapperClass(Mapeador.class);
        xmlHakker.setMapOutputKeyClass(Text.class);

        // Output / Reducer
        FileOutputFormat.setOutputPath(xmlHakker, new Path(outputPath));
        xmlHakker.setOutputFormatClass(TextOutputFormat.class);

        xmlHakker.setOutputKeyClass(Text.class);
        xmlHakker.setOutputValueClass(Text.class);
        xmlHakker.setReducerClass(Reductor.class);//se dice cual es el reductor
        System.out.println("antes de waitForCompletion..");
        //System.out.println("estado:"+xmlHakker.getJobState().getValue());
        return xmlHakker.waitForCompletion(true);
    }

    private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        
        //conf.set

        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
        rankCalculator.setJarByClass(PrincipalPageRank.class);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

        rankCalculator.setMapperClass(MapeadorCalculoRank.class);
        rankCalculator.setReducerClass(ReductorCalculoRank.class);
        System.out.println("antes de otro waitforcompletion");
        return rankCalculator.waitForCompletion(true);
    }

    private boolean runRankOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankOrdering = Job.getInstance(conf, "rankOrdering");
        rankOrdering.setJarByClass(PrincipalPageRank.class);

        rankOrdering.setOutputKeyClass(FloatWritable.class);
        rankOrdering.setOutputValueClass(Text.class);

        rankOrdering.setMapperClass(MapeadorRanking.class);

        FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

        rankOrdering.setInputFormatClass(TextInputFormat.class);
        rankOrdering.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("antes del último waitforcompletion");
        return rankOrdering.waitForCompletion(true);
    }

}

