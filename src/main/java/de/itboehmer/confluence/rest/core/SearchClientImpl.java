/**
 * Copyright 2016 Micromata GmbH
 * Modifications Copyright 2017-2018 Martin Böhmer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.itboehmer.confluence.rest.core;

import de.itboehmer.confluence.rest.ConfluenceRestClient;
import de.itboehmer.confluence.rest.client.SearchClient;
import de.itboehmer.confluence.rest.core.cql.CqlSearchBean;
import de.itboehmer.confluence.rest.core.domain.BaseBean;
import de.itboehmer.confluence.rest.core.domain.cql.CqlSearchResult;
import de.itboehmer.confluence.rest.core.util.HttpMethodFactory;
import de.itboehmer.confluence.rest.core.util.URIHelper;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link SearchClient} to query content by CQL.
 *
 * @author Christian Schulze (c.schulze@micromata.de)
 * @author Martin Böhmer
 */
public class SearchClientImpl extends BaseClient implements SearchClient {

    private final Logger log = LoggerFactory.getLogger(SearchClientImpl.class);

    public SearchClientImpl(ConfluenceRestClient confluenceRestClient, ExecutorService executorService) {
        super(confluenceRestClient, executorService);
    }

    @Override
    public Future<CqlSearchResult> searchContent(CqlSearchBean searchBean) {
        log.debug("Starting content search:\n  CQL: {}\n  Context: {}, \n  Expand: {}\n  Start: {}\n  Limit: {}",
                searchBean.getCql(), searchBean.getCqlcontext(), searchBean.getExpand(), searchBean.getStart(), searchBean.getLimit());
        Validate.notNull(searchBean);
        Validate.notNull(StringUtils.trimToNull(searchBean.getCql()));
        // Basic request
        String cql = searchBean.getCql();
        List<NameValuePair> basicNameValuePairs = new ArrayList<>();
        basicNameValuePairs.add(new BasicNameValuePair(CQL, cql));
        if (StringUtils.trimToNull(searchBean.getCqlcontext()) != null) {
            basicNameValuePairs.add(new BasicNameValuePair(CQL_CONTEXT, searchBean.getCqlcontext()));
        }
        if (searchBean.getExcerpt() != null) {
            basicNameValuePairs.add(new BasicNameValuePair(EXCERPT, searchBean.getExcerpt().getName()));
        }
        if (CollectionUtils.isNotEmpty(searchBean.getExpand()) == true) {
            String join = StringUtils.join(searchBean.getExpand(), ",");
            basicNameValuePairs.add(new BasicNameValuePair(EXPAND, join));
        }
        if (searchBean.getLimit() > 0) {
            basicNameValuePairs.add(new BasicNameValuePair(LIMIT, String.valueOf(searchBean.getLimit())));
        }
        // Initital request with start parameter
        URIBuilder uriBuilder = URIHelper.buildPath(baseUri, SEARCH).addParameters(basicNameValuePairs);
        if (searchBean.getStart() > 0) {
            uriBuilder.addParameter(START, String.valueOf(searchBean.getStart()));
        }
        return executorService.submit(() -> {
            HttpGet method = HttpMethodFactory.createGetMethod(uriBuilder.build());
            CqlSearchResult searchResult = executeRequest(method, CqlSearchResult.class);
            if (searchBean.isRetrieveAllResults()) {
                // Retrieve all (remaining) results
                log.debug("Retrieving all (remaining) results");
                int count = 1;
                List<BaseBean> completeResults = searchResult.getResults();
                URI nextURI = getNextResultsUri(searchResult, basicNameValuePairs);
                while (nextURI != null) {
                    log.debug("Retrieving part {} of the result set", count++);
                    method = HttpMethodFactory.createGetMethod(nextURI);
                    CqlSearchResult nextResults = executeRequest(method, CqlSearchResult.class);
                    completeResults.addAll(nextResults.getResults());
                    nextURI = getNextResultsUri(nextResults, basicNameValuePairs);
                }
                searchResult.setSize(completeResults.size());
                searchResult.setLimit(searchBean.getLimit());
            }
            log.info("Content search results - total: {}, size: {}, start: {}, limit: {}", searchResult.getTotalSize(),
                    searchResult.getSize(), searchResult.getStart(), searchResult.getLimit());
            return searchResult;
        }
        );

    }

    private URI getNextResultsUri(CqlSearchResult lastResult, List<NameValuePair> basicNameValuePairs) throws URISyntaxException {
        boolean hasNextUri = lastResult.get_links() != null && lastResult.get_links().getNext() != null;
        if (hasNextUri) {
            // URI provided, so take it
            String nextUri = lastResult.get_links().getNext();
            return new URI(nextUri);
        } else if (lastResult.hasMoreResults()) {
            // No URI provided, so compose it
            URIBuilder uriBuilder = URIHelper.buildPath(baseUri, SEARCH).addParameters(basicNameValuePairs);
            int start = lastResult.getStart() + lastResult.getSize();
            uriBuilder.addParameter(START, String.valueOf(start));
            return uriBuilder.build();
        } else {
            // No more results to query
            return null;
        }
    }

}
