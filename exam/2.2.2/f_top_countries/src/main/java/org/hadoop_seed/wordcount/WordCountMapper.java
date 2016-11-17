package org.hadoop_seed.wordcount;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable UNO = new IntWritable(1);
    private final Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String output = value.toString();
        String[] countries = new String[]{"Afghanistan","Albania","Algeria","Andorra","Angola","Antigua and Barbuda","Argentina","Armenia","Aruba","Australia","Austria","Azerbaijan","Bahamas","Bahrain","Bangladesh","Barbados","Belarus","Belgium","Belize","Benin","Bhutan","Bolivia","Bosnia and Herzegovina","Botswana","Brazil","Brunei ","Bulgaria","Burkina Faso","Burma","Burundi","Cambodia","Cameroon","Canada","Cabo Verde","Central African Republic","Chad","Chile","China","Colombia","Comoros","Congo, Democratic Republic of the","Congo, Republic of the","Costa Rica","Cote d'Ivoire","Croatia","Cuba","Curacao","Cyprus","Czechia","Denmark","Djibouti","Dominica","Dominican Republic","East Timor","Ecuador","Egypt","El Salvador","Equatorial Guinea","Eritrea","Estonia","Ethiopia","Fiji","Finland","France","Gabon","Gambia, The","Georgia","Germany","Ghana","Greece","Grenada","Guatemala","Guinea","Guinea-Bissau","Guyana","Haiti","Holy See","Honduras","Hong Kong","Hungary","Iceland","India","Indonesia","Iran","Iraq","Ireland","Israel","Italy","Jamaica","Japan","Jordan","Kazakhstan","Kenya","Kiribati","Korea, North","Korea, South","Kosovo","Kuwait","Kyrgyzstan","Laos","Latvia","Lebanon","Lesotho","Liberia","Libya","Liechtenstein","Lithuania","Luxembourg","Macau","Macedonia","Madagascar","Malawi","Malaysia","Maldives","Mali","Malta","Marshall Islands","Mauritania","Mauritius","Mexico","Micronesia","Moldova","Monaco","Mongolia","Montenegro","Morocco","Mozambique","Namibia","Nauru","Nepal","Netherlands","New Zealand","Nicaragua","Niger","Nigeria","North Korea","Norway","Oman","Pakistan","Palau","Palestinian Territories","Panama","Papua New Guinea","Paraguay","Peru","Philippines","Poland","Portugal","Qatar","Romania","Russia","Rwanda","Saint Kitts and Nevis","Saint Lucia","Saint Vincent and the Grenadines","Samoa ","San Marino","Sao Tome and Principe","Saudi Arabia","Senegal","Serbia","Seychelles","Sierra Leone","Singapore","Sint Maarten","Slovakia","Slovenia","Solomon Islands","Somalia","South Africa","South Korea","South Sudan","Spain ","Sri Lanka","Sudan","Suriname","Swaziland ","Sweden","Switzerland","Syria","Taiwan","Tajikistan","Tanzania","Thailand ","Timor-Leste","Togo","Tonga","Trinidad and Tobago","Tunisia","Turkey","Turkmenistan","Tuvalu","Uganda","Ukraine","United Arab Emirates","United Kingdom","Uruguay","Uzbekistan","Vanuatu","Venezuela","Vietnam","Yemen","Zambia","Zimbabwe","USA","United States of America"};
        String[] countriesLowerCase = new String[]{"afghanistan","albania","algeria","andorra","angola","antigua and barbuda","argentina","armenia","aruba","australia","austria","azerbaijan","bahamas","bahrain","bangladesh","barbados","belarus","belgium","belize","benin","bhutan","bolivia","bosnia and herzegovina","botswana","brazil","brunei ","bulgaria","burkina faso","burma","burundi","cambodia","cameroon","canada","cabo verde","central african republic","chad","chile","china","colombia","comoros","congo, democratic republic of the","congo, republic of the","costa rica","cote d'ivoire","croatia","cuba","curacao","cyprus","czechia","denmark","djibouti","dominica","dominican republic","east timor","ecuador","egypt","el salvador","equatorial guinea","eritrea","estonia","ethiopia","fiji","finland","france","gabon","gambia, the","georgia","germany","ghana","greece","grenada","guatemala","guinea","guinea-bissau","guyana","haiti","holy see","honduras","hong kong","hungary","iceland","india","indonesia","iran","iraq","ireland","israel","italy","jamaica","japan","jordan","kazakhstan","kenya","kiribati","korea, north","korea, south","kosovo","kuwait","kyrgyzstan","laos","latvia","lebanon","lesotho","liberia","libya","liechtenstein","lithuania","luxembourg","macau","macedonia","madagascar","malawi","malaysia","maldives","mali","malta","marshall islands","mauritania","mauritius","mexico","micronesia","moldova","monaco","mongolia","montenegro","morocco","mozambique","namibia","nauru","nepal","netherlands","new zealand","nicaragua","niger","nigeria","north korea","norway","oman","pakistan","palau","palestinian territories","panama","papua new guinea","paraguay","peru","philippines","poland","portugal","qatar","romania","russia","rwanda","saint kitts and nevis","saint lucia","saint vincent and the grenadines","samoa ","san marino","sao tome and principe","saudi arabia","senegal","serbia","seychelles","sierra leone","singapore","sint maarten","slovakia","slovenia","solomon islands","somalia","south africa","south korea","south sudan","spain ","sri lanka","sudan","suriname","swaziland ","sweden","switzerland","syria","taiwan","tajikistan","tanzania","thailand ","timor-leste","togo","tonga","trinidad and tobago","tunisia","turkey","turkmenistan","tuvalu","uganda","ukraine","united arab emirates","united kingdom","uruguay","uzbekistan","vanuatu","venezuela","vietnam","yemen","zambia","zimbabwe","usa","united states of america"};
        String[] stateList = new String[]{"AK","AL","AR","AZ","CA","CO","CT","DC","DE","FL","GA","GU","HI","IA","ID", "IL","IN","KS","KY","LA","MA","MD","ME","MH","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY", "OH","OK","OR","PA","PR","PW","RI","SC","SD","TN","TX","UT","VA","VI","VT","WA","WI","WV","WY"};
        if (output.contains("Location=")) { //If the row contains a location
            Pattern pattern = Pattern.compile("Location=\"(.*?)\"");
            Matcher matcher = pattern.matcher(output);
            if (matcher.find()) { //Only write to context if we find a location
                output = matcher.group(1);
            }

            output = output.replaceAll("\\W", " "); //Replaces non alphanumeric characters with space
            output = output.trim().replaceAll(" +", " "); //Removes any double spaces
            final String[] words = output.split(" "); //Splits by space
            int indexOfCountry = 0;
            for (String word : words) {
                indexOfCountry = ArrayUtils.indexOf(countriesLowerCase, word.toLowerCase());
                if (indexOfCountry == -1) {
                    if (ArrayUtils.indexOf(stateList, word) != -1) {
                        indexOfCountry = 204; //If it does not find country, but does find a state abbreviation
                    }
                }
            }
            if (indexOfCountry > -1) {
                context.write(new Text(Arrays.asList(countries).get(indexOfCountry)), UNO);
            }
            else {
                context.write(new Text("ZZ Not recognized"), UNO);
            }
        }

    }
}
