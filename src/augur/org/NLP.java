//http://rahular.com/twitter-sentiment-analysis/
package augur.org;

import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;

public class NLP
{
	StanfordCoreNLP pipeline;
	
	public NLP()
	{
		 pipeline = new StanfordCoreNLP("nlp_file.properties");
	}
	
	public int analyse(String text)
	{
		//initial sentiment is 0.
		int final_sentiment = 0;
		//check to see if the supplied text is non-null. 
		if (text != null && text.length()>0)
		{
			int length = 0;
			Annotation annotation = pipeline.process(text);
			for (CoreMap sub_text : annotation.get(CoreAnnotations.SentencesAnnotation.class))
			{
				Tree tree = sub_text.get(SentimentCoreAnnotations.AnnotatedTree.class);
				int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
				String partText = sub_text.toString();
				if (partText.length()>length)
				{
					final_sentiment = sentiment;
					length = partText.length();
				}
			}
			
		}
		return final_sentiment;
		
		
	}
}