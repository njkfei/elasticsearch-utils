package es1;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.njp.learn.lucene.es1.IndexUtil;

public class testIndex {
	private static final Logger logger = Logger.getLogger(testIndex.class);

	@Test
	public void testIndex() {
		try {
			IndexUtil.Indexer();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
