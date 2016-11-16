package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable UNO = new IntWritable(1);
    private final Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (value.toString().contains("PostTypeId=\"1\"")) { //If it is a question
            String[] stopWords = new String[]{"'ll","a","a's","able","about","above","abst","accordance","according","accordingly","across","act","actually","added","adj","affected","affecting","affects","after","afterwards","again","against","ah","ain't","all","allow","allows","almost","alone","along","already","also","although","always","am","among","amongst","an","and","announce","another","any","anybody","anyhow","anymore","anyone","anything","anyway","anyways","anywhere","apart","apparently","appear","appreciate","appropriate","approximately","are","aren","aren't","arent","arise","around","as","aside","ask","asking","associated","at","auth","available","away","awfully","b","back","be","became","because","become","becomes","becoming","been","before","beforehand","begin","beginning","beginnings","begins","behind","being","believe","below","beside","besides","best","better","between","beyond","biol","both","brief","briefly","but","by","c","c'mon","c's","ca","came","can","can't","cannot","cant","cause","causes","certain","certainly","changes","clearly","co","com","come","comes","concerning","consequently","consider","considering","contain","containing","contains","corresponding","could","couldn't","couldnt","course","currently","d","date","definitely","described","despite","did","didn't","different","do","does","doesn't","doing","don't","done","down","downwards","due","during","e","each","ed","edu","effect","eg","eight","eighty","either","else","elsewhere","end","ending","enough","entirely","especially","et","et-al","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","exactly","example","except","f","far","few","ff","fifth","first","five","fix","followed","following","follows","for","former","formerly","forth","found","four","from","further","furthermore","g","gave","get","gets","getting","give","given","gives","giving","go","goes","going","gone","got","gotten","greetings","h","had","hadn't","happens","hardly","has","hasn't","have","haven't","having","he","he'd","he'll","he's","hed","hello","help","hence","her","here","here's","hereafter","hereby","herein","heres","hereupon","hers","herself","hes","hi","hid","him","himself","his","hither","home","hopefully","how","how's","howbeit","however","hundred","i","i'd","i'll","i'm","i've","id","ie","if","ignored","im","immediate","immediately","importance","important","in","inasmuch","inc","indeed","index","indicate","indicated","indicates","information","inner","insofar","instead","into","invention","inward","is","isn't","it","it'd","it'll","it's","itd","its","itself","j","just","k","keep","keep","keeps","kept","kg","km","know","known","knows","l","largely","last","lately","later","latter","latterly","least","less","lest","let","let's","lets","like","liked","likely","line","little","look","looking","looks","ltd","m","made","mainly","make","makes","many","may","maybe","me","mean","means","meantime","meanwhile","merely","mg","might","million","miss","ml","more","moreover","most","mostly","mr","mrs","much","mug","must","mustn't","my","myself","n","na","name","namely","nay","nd","near","nearly","necessarily","necessary","need","needs","neither","never","nevertheless","new","next","nine","ninety","no","nobody","non","none","nonetheless","noone","nor","normally","nos","not","noted","nothing","novel","now","nowhere","o","obtain","obtained","obviously","of","off","often","oh","ok","okay","old","omitted","on","once","one","ones","only","onto","or","ord","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","owing","own","p","page","pages","part","particular","particularly","past","per","perhaps","placed","please","plus","poorly","possible","possibly","potentially","pp","predominantly","present","presumably","previously","primarily","probably","promptly","proud","provides","put","q","que","quickly","quite","qv","r","ran","rather","rd","re","readily","really","reasonably","recent","recently","ref","refs","regarding","regardless","regards","related","relatively","research","respectively","resulted","resulting","results","right","run","s","said","same","saw","say","saying","says","sec","second","secondly","section","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sensible","sent","serious","seriously","seven","several","shall","shan't","she","she'd","she'll","she's","shed","shes","should","shouldn't","show","showed","shown","showns","shows","significant","significantly","similar","similarly","since","six","slightly","so","some","somebody","somehow","someone","somethan","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specifically","specified","specify","specifying","still","stop","strongly","sub","substantially","successfully","such","sufficiently","suggest","sup","sure","sure","t's","take","taken","tell","tends","th","than","thank","thanks","thanx","that","that's","thats","the","their","theirs","them","themselves","then","thence","there","there's","thereafter","thereby","therefore","therein","theres","thereupon","these","they","they'd","they'll","they're","they've","think","third","this","thorough","thoroughly","those","though","three","through","throughout","thru","thus","to","together","too","took","toward","towards","tried","tries","truly","try","trying","twice","two","un","under","unfortunately","unless","unlikely","until","unto","up","upon","us","use","used","useful","uses","using","usually","value","various","very","via","viz","vs","want","wants","was","wasn't","way","we","we'd","we'll","we're","we've","welcome","well","went","were","weren't","what","what's","whatever","when","when's","whence","whenever","where","where's","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","who's","whoever","whole","whom","whose","why","why's","will","willing","wish","with","within","without","won't","wonder","would","wouldn't","yes","yet","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves","zero"};
            String output = value.toString(); //.replaceAll("ody=\"", ""); //Removes <></>he leading tag
            Pattern pattern = Pattern.compile("itle=\"(.*?)\"");
    //            Pattern pattern = Pattern.compile("^(.*?)\"");

            Matcher matcher = pattern.matcher(output);
            if (matcher.find()) {
                output = matcher.group(1);
            }
            //System.out.println(output); For debug purposes
            output = output.replaceAll("\"", ""); //Removes the trailing tag
            output = StringEscapeUtils.unescapeHtml(output); //Unescapes the html tags
            output = output.replaceAll("\\<.*?>", ""); //Removes html tags
            output = output.replaceAll("\\W", " "); //Replaces non alphanumeric characters with space
            output = output.replaceAll("\\d", " "); //Replaces digits with space
            output = output.replaceAll("_", " "); //Replaces underscore with space
            output = output.trim().replaceAll(" +", " "); //Removes any double spaces
            output = output.toLowerCase(); //Sets all characters to lower case
            //System.out.println(stopWords.length);
            //Iterates through the ouputstring, splits it in words
            final String[] words = output.split(" ");
            for (String word : words) {
                if(!Arrays.asList(stopWords).contains(word)) {
                    context.write(new Text(word), UNO);
                }

            }
        }

    }
}