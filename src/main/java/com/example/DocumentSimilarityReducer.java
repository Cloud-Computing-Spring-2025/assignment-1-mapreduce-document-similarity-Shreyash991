package com.example.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class DocumentSimilarityReducer extends Reducer<Text, Text, Text, Text> {
    
    private Map<String, Set<String>> docWords = new HashMap<>();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        for (Text doc : values) {
            String docName = doc.toString();
            docWords.computeIfAbsent(docName, k -> new HashSet<>()).add(key.toString());
        }
    }
    
    @Override
    protected void cleanup(Context context) 
            throws IOException, InterruptedException {
        
        // Compare the document pair and calculate their Jaccard Similarity
        List<String> docs = new ArrayList<>(docWords.keySet());
        
        for (int i = 0; i < docs.size(); i++) {
            for (int j = i + 1; j < docs.size(); j++) {
                String doc1 = docs.get(i);
                String doc2 = docs.get(j);
                
                Set<String> words1 = docWords.get(doc1);
                Set<String> words2 = docWords.get(doc2);
                
                // Calculating intersection size
                Set<String> common = new HashSet<>(words1);
                common.retainAll(words2);
                int commonWords = common.size();
                
                // Calculating union size
                Set<String> all = new HashSet<>(words1);
                all.addAll(words2);
                int totalWords = all.size();
                
                // For Jaccard similarity
                double similarity = (double) commonWords / totalWords;
                
                // if similarity is greater than 50%
                if (similarity > 0.5) {
                
                    String result = String.format("%.2f%%", similarity * 100);
                    context.write(new Text("(" + doc1 + ", " + doc2 + ")"), new Text("-> " + result));
                }
            }
        }
    }
}
