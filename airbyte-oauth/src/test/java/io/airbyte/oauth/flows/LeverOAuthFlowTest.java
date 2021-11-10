/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LeverOAuthFlowTest {

  private UUID workspaceId;
  private UUID definitionId;
  private ConfigRepository configRepository;
  private LeverOAuthFlow flow;
  private HttpClient httpClient;

  private static final String REDIRECT_URL = "https://airbyte.io";

  private static String getConstantState() {
    return "state";
  }

  @BeforeEach
  public void setup() throws IOException, JsonValidationException {
    workspaceId = UUID.randomUUID();
    definitionId = UUID.randomUUID();
    configRepository = mock(ConfigRepository.class);
    httpClient = mock(HttpClient.class);
    when(configRepository.listSourceOAuthParam()).thenReturn(List.of(new SourceOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withSourceDefinitionId(definitionId)
        .withWorkspaceId(workspaceId)
        .withConfiguration(Jsons.jsonNode(
            ImmutableMap.builder()
                .put("client_id", "test_client_id")
                .put("client_secret", "test_client_secret")
                .build()))));
    flow = new LeverOAuthFlow(configRepository, httpClient, LeverOAuthFlowTest::getConstantState);

  }

  @Test
  public void testGetSourceConsentUrl() throws IOException, InterruptedException, ConfigNotFoundException {
    final String consentUrl =
        flow.getSourceConsentUrl(workspaceId, definitionId, REDIRECT_URL);
    assertEquals(
        "https://sandbox-lever.auth0.com/authorize?client_id=test_client_id&redirect_uri=https://airbyte.io&response_type=code&state=state&scope=applications:read:admin+contact:read:admin+interviews:read:admin+offers:read:admin+opportunities:read:admin+postings:read:admin+referrals:read:admin+requisitions:read:admin+resumes:read:admin+sources:read:admin+stages:read:admin+offline_access&prompt=consent&audience=https://api.sandbox.lever.co/v1/",
        consentUrl);
  }

  @Test
  public void testCompleteSourceOAuth() throws IOException, JsonValidationException, InterruptedException, ConfigNotFoundException {

    Map<String, String> returnedCredentials = Map.of("refresh_token", "refresh_token_response");
    final HttpResponse response = mock(HttpResponse.class);
    when(response.body()).thenReturn(Jsons.serialize(returnedCredentials));
    when(httpClient.send(any(), any())).thenReturn(response);
    final Map<String, Object> queryParams = Map.of("code", "test_code");
    final Map<String, Object> actualQueryParams =
        flow.completeSourceOAuth(workspaceId, definitionId, queryParams, REDIRECT_URL);
    assertEquals(Jsons.serialize(returnedCredentials), Jsons.serialize(actualQueryParams));
  }

}