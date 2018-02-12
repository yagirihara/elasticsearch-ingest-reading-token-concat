/*
 * Copyright [2017] [yagirihara]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.elasticsearch.plugin.ingest.reading_token_concat;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ja.JapaneseReadingFormFilterFactory;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizerFactory;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public class Reading_token_concatProcessor extends AbstractProcessor {

    public static final String TYPE = "reading_token_concat";

    private final String field;
    private final String targetField;
    private final Analyzer kuromojiAnalyzer;

    public Reading_token_concatProcessor(String tag, String field, String targetField) throws IOException {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.kuromojiAnalyzer = loadAnalyzer();
    }

    private Analyzer loadAnalyzer() {
        Map<String, String> tokenizerOptions = new HashMap<>();
        tokenizerOptions.put("mode", JapaneseTokenizer.Mode.NORMAL.toString());
        // TODO set dictionary
        TokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory(tokenizerOptions);
        TokenFilterFactory[] tokenFilterFactories = new TokenFilterFactory[1];
        tokenFilterFactories[0] = new JapaneseReadingFormFilterFactory(new HashMap<>());

        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String s) {
                Tokenizer tokenizer = tokenizerFactory.create();
                TokenStream tokenStream = tokenizer;
                for (TokenFilterFactory tokenFilterFactory : tokenFilterFactories) {
                    tokenStream = tokenFilterFactory.create(tokenStream);
                }
                return new TokenStreamComponents(tokenizer, tokenStream);
            }
        };

        return analyzer;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        String content = ingestDocument.getFieldValue(field, String.class);
        // contentをanalyzeして得られた読みを繋げたものを格納する
        StringBuilder concatTokens = new StringBuilder();
        if (Strings.isNullOrEmpty(content) == false) {
            try (TokenStream tokens = this.kuromojiAnalyzer.tokenStream("field", content)) {
                tokens.reset();
                CharTermAttribute termAttr = tokens.getAttribute(CharTermAttribute.class);

                while (tokens.incrementToken()) {
                    concatTokens.append(termAttr.toString());
                }
                tokens.end();

                ingestDocument.setFieldValue(targetField, concatTokens.toString());
                return;
                // tokens.close();
            }
        }
        ingestDocument.setFieldValue(targetField, content);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public Reading_token_concatProcessor create(Map<String, Processor.Factory> processorFactories, String tag,
                                                    Map<String, Object> config)
                throws Exception {
            String field = readStringProperty(TYPE, tag, config, "field");
            String targetField = readStringProperty(TYPE, tag, config, "target_field", "default_field_name");
            return new Reading_token_concatProcessor(tag, field, targetField);
        }
    }
}
